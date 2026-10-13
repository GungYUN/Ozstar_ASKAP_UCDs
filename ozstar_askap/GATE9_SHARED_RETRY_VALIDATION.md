# Gate9：共享模式失败隔离、显式重试与 controller 恢复

入口为 `gate9_retry_validation.py`。本测试不重查全表、不修改 SIF、不复用或
改动 Gate8/生产的 MS、产物、处理数据库。只读取已发布的 central 索引及其
不可变 ECSV，复制索引到全新的 Gate9 工作目录。

## 测试内容及预期

| 样本 | 观测与目标 | 受控故障 | 预期最终结果 |
| --- | --- | --- | --- |
| shared-recovery | SB74372_beam28：UCS689/690/693 | UCS690 首次 insert 失败；处理重试时在 create 前失败 | 三颗源最终成功；伙伴产物字节不变 |
| exhaustion | SB67772_beam18：UCS225 | 初次及两次允许的重试均在 create 前失败 | exhausted，保留最后失败 MS，不再提交 |

具体检查：

1. 首次共享作业提交回执落盘后，驱动只向自己启动的登录 controller 发 SIGINT。
   不取消 Slurm、不杀计算 worker、不删除锁。随后以相同参数恢复，要求仍是原作业 ID。
2. 一颗目标失败后，伙伴仍完成；失败目标的 MS 与公共模型保留。
3. 普通运行不重试失败项；仅测试驱动的显式 `--retry-existing` 阶段进入重试。
4. 处理重试回到 reset/preprocess；再次失败后删除该目标的失败 MS/checksum，
   重新下载一次；恢复样本最终成功，且伙伴产物 SHA256 保持不变。
5. 耗尽样本最后仍失败时保留 MS、详细错误/命令日志；再正常/显式运行均不新增作业。
6. 成功样本再正常/显式运行也不新增作业、下载或修改 SB 记录。

两个样本分别应有 **3 个真实作业、2 次成功归档下载**，合计通常6个作业、
4次下载。恢复样本只需两次真实建模；其处理重试在 create 执行前故意失败。
耗尽样本三次都在 create 执行前故意失败，不消耗三次长时间建模。

资源为一次一个作业：**8 核、40G RAM、10G tmp**。沿用估时逻辑，首次三目标
共享作业申请5.5小时，其余单目标申请4.5小时，不是交互式节点。
`--timeout-hours` 默认48，限制单个 controller 的登录端等待时长（含下载/排队），
不是 Slurm 的请求时间。

有意注入的错误、首次 controller 的 `KeyboardInterrupt`，以及耗尽样本的
`show_failures.py` 输出都是预期的；以 `state/gate9-result.json` 中 `status=PASS`
和各阶段断言判断，不要求日志完全没有 ERROR。

本轮不强制制造48小时壁钟耗尽/计算节点崩溃，也不验证 Ada 拉取；不能将这些
未执行项目列作已验收。本测试不向科学产物加入格式/内容校验。

## 1. Mac：部署

确认共享程序目录没有仍在运行的作业后执行。没有密码文件时，会提示隐藏输入密码：

```bash
cd /Users/qhua0119/Documents/ChatGPT/Ozstar/Ozstar_ASKAP_UCDs/ozstar_askap
OZSTAR_WORK=/fred/oz299/qhuang/ASKAP-UCDs-shared bash deploy_to_ozstar.sh
```

需要更新 driver、process_job.py、shared_worker.py、ozstar_main.py 和 tests。
完整部署也包含根目录的单源下载程序，避免回归测试缺文件。
新增生产代码仅为显式 Gate9 配置启用的故障钩子；驱动自己设置该变量，
不要在普通生产 shell 中手动 export `ASKAP_GATE9_FAULT_CONFIG`。

## 2. Ozstar 登录节点：新 tmux 与预检查

```bash
tmux new -s askap-gate9
```

进入后执行，检查失败只返回函数，不关闭 shell：

```bash
gate9_preflight() {
  cd /fred/oz299/qhuang/ASKAP-UCDs-shared/ozstar_askap || return 1
  module load gcc/13.3.0 || return 1
  module load python/3.12.3 || return 1
  module load apptainer || return 1
  export ASKAP_PYTHON_BIN=/fred/oz299/qhuang/ASKAP-UCDs/.venv/askap-python/bin/python
  export ASKAP_PYTHON_PARENT_MODULE=gcc/13.3.0
  export ASKAP_PYTHON_MODULE=python/3.12.3
  unset ASKAP_GATE6_FAULT_CONFIG ASKAP_GATE9_FAULT_CONFIG

  "$ASKAP_PYTHON_BIN" -m py_compile gate9_retry_validation.py \
    process_job.py shared_worker.py ozstar_main.py || return 1
  "$ASKAP_PYTHON_BIN" gate9_retry_validation.py --help || return 1
  "$ASKAP_PYTHON_BIN" -m unittest discover -s tests -v || return 1
  sha256sum -c /fred/oz299/qhuang/dstools/dstools-260925.sif.sha256 || return 1
  df -h /fred/oz299/qhuang/dstools || return 1
  df -i /fred/oz299/qhuang/dstools || return 1
}
if gate9_preflight; then
  echo "Gate9 preflight: PASS"
else
  echo "Gate9 preflight: FAIL，请先停止后续操作并检查输出。"
fi
```

除共享/目标模型副本外，耗尽样本会按约定保留失败输入。请确认用户/项目配额
而非仅查看 `df`；本测试不会自动清理这些诊断输入或之前的 Gate8 数据。

## 3. 启动

固定路径便于新终端定位。首次运行必须是空/不存在目录；若之前已启动，
不要以“首次运行”覆盖，用下一节的 `--resume`。

```bash
GATE9_WORK=/fred/oz299/qhuang/dstools/gate9-validation/gate9-20261012
hostname
echo "Gate9 work: $GATE9_WORK"

"$ASKAP_PYTHON_BIN" gate9_retry_validation.py \
  --work "$GATE9_WORK" \
  --index-work /fred/oz299/qhuang/dstools/shared-validation/central-frequency-20261008 \
  --submit
```

驱动自动按阶段执行，不需要手动运行 `select_case`、`prepare_case`、
`run_case` 或重复提交 sbatch。日志自己保存在 `logs/gate9-driver.log`，
无需额外 tee；请前台运行，不用 Ctrl-Z 或手动中断它。
断开 tmux 用 Ctrl-b、d，再用 `tmux attach -t askap-gate9` 回到同一节点。
驱动记录主机和 PID，避免“当前节点没有 tmux”被误认为全部工作停止。

## 4. 新终端：只读检查与恢复

检查不需要重设 ASKAP_WORK，不会开启新任务：

```bash
module load gcc/13.3.0
module load python/3.12.3
export ASKAP_PYTHON_BIN=/fred/oz299/qhuang/ASKAP-UCDs/.venv/askap-python/bin/python
GATE9_WORK=/fred/oz299/qhuang/dstools/gate9-validation/gate9-20261012

cat "$GATE9_WORK/state/gate9-runtime.json"
cat "$GATE9_WORK/state/gate9-child.json"
tail -n 60 "$GATE9_WORK/logs/gate9-driver.log"
squeue -u qhuang

if test -f "$GATE9_WORK/state/gate9-result.json"; then
  cat "$GATE9_WORK/state/gate9-result.json"
else
  echo "尚未生成最终结果，请查看 driver 日志和 checkpoint。"
fi
```

仅在确认原驱动已经停止时，重载模块并恢复**同一目录**：

```bash
cd /fred/oz299/qhuang/ASKAP-UCDs-shared/ozstar_askap
module load gcc/13.3.0
module load python/3.12.3
module load apptainer
export ASKAP_PYTHON_BIN=/fred/oz299/qhuang/ASKAP-UCDs/.venv/askap-python/bin/python
export ASKAP_PYTHON_PARENT_MODULE=gcc/13.3.0
export ASKAP_PYTHON_MODULE=python/3.12.3
GATE9_WORK=/fred/oz299/qhuang/dstools/gate9-validation/gate9-20261012

"$ASKAP_PYTHON_BIN" gate9_retry_validation.py \
  --work "$GATE9_WORK" --resume --submit
```

完成的阶段不会重跑。锁仍被占用时不要删锁或另起同目录驱动，应先查看记录的
主机/进程。若结果为 FAIL，先返回日志及结果排查；不要把恢复命令当作无限重试。

## 5. 验收

最终应显示 `Gate9 shared retry validation: PASS`，结果 JSON 的 `status=PASS`。
恢复样本三颗源全部 complete，UCS690 的两个重试计数均为1；耗尽样本 UCS225
为 exhausted，两个计数也均为1。后者的失败记录是测试目的，不代表 Gate9 失败。
结果包含6个提交ID、各次故障事件及检查点；失败日志保留完整命令并明确标为
synthetic，不把未执行的 create/insert 当作真实 DStools 程序失败。

请返回 driver 最后60行与结果 JSON，确认通过后再安排 Gate10。
