# 2. 使用与可调整参数

[索引](../../README.md) · [English](../en/USAGE.md) · [安装](INSTALLATION.md) · [运行机制](REFERENCE.md)

指令使用 Bash。第1–5节在 **Ozstar 登录节点**执行，第6节在 **Ada 登录节点**执行。
前置检查失败时停止，不要继续粘贴剩余指令。CLI 参数追加到命令末尾，环境变量
必须在启动 Python 前 export。显式 CLI 值优先于对应环境变量。
下表是代码默认值，不保证已 export 的值或已冻结的作业也使用该默认值。

## 1. 会话环境与公共设置

每次连接后恢复环境，建议先建立 `tmux new -s askap-production` 会话。
重连时在原登录主机 attach；离开会话用 Ctrl-b、d，不要用 Ctrl-Z。

```bash
askap_session() {
  cd /fred/oz299/qhuang/ASKAP-UCDs-shared/ozstar_askap || return 1
  module load gcc/13.3.0 || return 1
  module load python/3.12.3 || return 1
  module load apptainer || return 1
  export ASKAP_WORK=/fred/oz299/qhuang/ASKAP-UCDs-production
  export ASKAP_CATALOGUE=/fred/oz299/qhuang/ASKAP-UCDs/catalogue/UltracoolSheet_Main_index_unbinaryUCD.csv
  export ASKAP_TIME_CSV=/fred/oz299/qhuang/ASKAP-UCDs/catalogue/All_combine_data_unrepetition.csv
  export ASKAP_PYTHON_BIN=/fred/oz299/qhuang/ASKAP-UCDs/.venv/askap-python/bin/python
  export ASKAP_PYTHON_PARENT_MODULE=gcc/13.3.0
  export ASKAP_PYTHON_MODULE=python/3.12.3
  export DSTOOLS_CONTAINER=/fred/oz299/qhuang/dstools/dstools-260925.sif
  export ASKAP_PARALLEL_SLOTS=4 ASKAP_CPU_PER_SLOT=8 ASKAP_CREATE_MODEL_THREADS=8
  export ASKAP_SBATCH_MEM=160G ASKAP_SBATCH_TMP=10G
  export ASKAP_MAX_JOB_WALLTIME_HOURS=48
  unset ASKAP_CASDA_RADIUS_FREQUENCY_MODE ASKAP_GATE6_FAULT_CONFIG ASKAP_GATE9_FAULT_CONFIG
  test -s "$ASKAP_CATALOGUE" && test -s "$ASKAP_TIME_CSV" || return 1
  "$ASKAP_PYTHON_BIN" -c 'import numpy,pandas,astropy,astroquery,keyring' || return 1
  sha256sum -c /fred/oz299/qhuang/dstools/dstools-260925.sif.sha256 || return 1
  bash prepare_layout.sh || return 1
  set -o pipefail
}
if askap_session; then
  echo "Session: READY"
else
  echo "Session: FAIL; stop and inspect the output."
fi
```

必要时按[安装第5节](INSTALLATION.md#5-ozstar-的-casda-凭据)输入 CASDA 密码。
此函数显式选择生产目录与基准资源，请按部署需要修改这些 export。
不要让 `ASKAP_WORK` 为空；下载前检查个人/项目配额。

| 环境变量 | 代码默认值 | 作用 |
| --- | --- | --- |
| `ASKAP_WORK` | `/fred/oz299/qhuang/ASKAP-UCDs` | 持久化数据/状态根目录；本指南显式使用新生产目录 |
| `ASKAP_CATALOGUE` | `$ASKAP_WORK/catalogue/UltracoolSheet_Main_index_unbinaryUCD.csv` | 源 catalogue |
| `ASKAP_TIME_CSV` | `$ASKAP_WORK/catalogue/All_combine_data_unrepetition.csv` | 旧观测历元后备表 |
| `ASKAP_PYTHON_BIN` | `python3` | controller/worker 主机解释器，应使用 venv 绝对路径 |
| `ASKAP_PYTHON_PARENT_MODULE`、`ASKAP_PYTHON_MODULE` | `gcc/13.3.0`、`python/3.12.3` | 生成的 worker 加载的模块 |
| `ASKAP_PYTHON_ENV` | `$ASKAP_WORK/.venv/askap-python` | 仅安装脚本使用：要创建的环境 |
| `DSTOOLS_CONTAINER` | `/fred/oz299/qhuang/dstools/dstools-260925.sif` | 已验收处理镜像 |
| `APPTAINER_BIN`、`ASKAP_APPTAINER_BIND` | `apptainer`、`/fred/oz299/qhuang:/fred/oz299/qhuang` | 容器执行程序与 bind |
| `CASDA_USERNAME` | `qhua0119@uni.sydney.edu.au` | CASDA 登录身份 |
| `CASDA_PASSWORD`、`CASDA_PASSWORD_FILE` | 无环境密码；`$HOME/.config/askap/casda-password` | 运行时凭据，环境密码优先 |

部署 wrapper 选项（不属于运行处理设置）：

| 环境变量 | 默认值 | 作用 |
| --- | --- | --- |
| `OZSTAR_WORK` | `/fred/oz299/qhuang/ASKAP-UCDs` | 远端代码上传父目录 |
| `OZSTAR_SSH_USER`、`OZSTAR_SSH_HOST` | `qhuang`、`ozstar.swin.edu.au` | 部署 SSH 目标 |
| `TRANSFER_PASSWORD` | 未设置 | 当前进程的部署密码；否则使用文件/提示输入 |
| `OZSTAR_PASSWORD_FILE` | `TRANSFER_PASSWORD_FILE`，再后备到 `$HOME/.config/askap/transfer-password` | 600权限部署密码文件 |

## 2. 建立或更新观测索引

```bash
"$ASKAP_PYTHON_BIN" ozstar_main.py --crosscatalogue-only
```

需要时建立全表索引，否则复用兼容的已发布索引。不下载 MS、不提交计算作业。
随后检查：

```bash
cat "$ASKAP_WORK/state/crosscatalogue-summary.json"
```

`unknown=0` 表示没有尚未尝试的源；还需要 `query_failed=0` 才算整轮查询全部成功。
`no_observations` 可以是正常结果。CSV 便于人类阅读并支持 TOPCAT；观测行数是
源—观测关联数量，不一定等于独立模型数量或剩余工作量。

| CLI 参数 | 默认值 | 作用/限制 |
| --- | --- | --- |
| `--crosscatalogue-only` | 关闭 | 仅索引操作，不下载 MS/提交 |
| `--update-crosscatalogue` | 关闭 | 显式重新查询每颗源并发布更新索引 |
| `--retry-crosscatalogue-failures` | 关闭 | 仅重试失败/未完成查询；要求 `--crosscatalogue-only`，不能与全量更新、refresh 或旧模式组合 |
| `--radius-frequency-mode central\|top` | 无环境覆盖时为 `central` | 半功率半径所用频率；策略比较应使用另一工作目录 |
| `--refresh-cache` | 关闭 | 共享模式下发起全表更新，不是只查询一颗种子源 |

| 环境变量 | 默认值 | 作用 |
| --- | --- | --- |
| `ASKAP_CASDA_RADIUS_FREQUENCY_MODE` | `central` | CLI 可覆盖为 `top` 或 `central` |
| `ASKAP_CASDA_INITIAL_QUERY_RADIUS_DEG` | `1.0` | 初查半径，单位度 |
| `ASKAP_CASDA_REFERENCE_FWHM_DEG`、`ASKAP_CASDA_REFERENCE_FREQUENCY_GHZ` | `1.2`、`1.4` | 波束缩放参考，分别为全宽和 GHz |
| `ASKAP_CASDA_QUERY_LIMIT` | `5000` | TAP TOP 上限，不将截断结果当成完整查询 |
| `ASKAP_CROSSCATALOGUE_QUERY_DELAY_SECONDS` | `0.2` | 顺序查询源之间的间隔 |
| `ASKAP_SPTNUM_THRESHOLD` | `20` | 处理资格阈值，较低光谱型仍出现在索引中 |
| `ASKAP_CASDA_MAX_FILE_SIZE_KB` | `52428800` | 单数据产品筛选大小上限，CASDA KB |

仅在没有正在处理的计划时使用这些示例：

```bash
"$ASKAP_PYTHON_BIN" ozstar_main.py --update-crosscatalogue --crosscatalogue-only
"$ASKAP_PYTHON_BIN" ozstar_main.py --retry-crosscatalogue-failures --crosscatalogue-only
```

也可将兼容 central 索引的已发布 `crosscatalogue.json` 单独复制至新工作区复用。
它引用的不可变 ECSV 必须持续可读，catalogue/筛选签名必须匹配。不要复制旧处理
数据库或 controller 计划，也不要删除原索引树。本地观测表下载和检查统一放在
`/Users/qhua0119/Documents/ChatGPT/Ozstar/观测记录表格/` 及其子目录。

## 3. 处理包含端点的源范围

```bash
"$ASKAP_PYTHON_BIN" ozstar_main.py --begin 1501 --end 1550 --submit
```

登录 controller 准备归档、提交 sbatch、等待并推进批次。保持它在 tmux 中运行，
才能自动提交后续批次。种子范围包含1501–1550；范围外符合条件的伙伴也会处理
这些相同观测，但不展开伙伴的其他观测。50编号的输出文件夹只是命名约定，
不代表固定下载50颗源的一批。

| CLI 参数 | 默认值 | 作用/限制 |
| --- | --- | --- |
| `--begin`、`--end` | `ASKAP_UCS_START=1`、`ASKAP_UCS_END=1000` | 包含端点的种子范围，建议明确指定 |
| `--submit` | 关闭 | 按需下载、提交、等待并推进 |
| `--no-submit` | 关闭 | 不提交，优先于 `--submit` |
| `--prepare-download` | 关闭 | 配合 `--no-submit` 准备当前计划批次，但不提交 |
| `--parallel-slots N` | `4` | 并行独立观测数，N × 每槽CPU ≤32 |
| `--shared-observation-key KEY` | 未设置 | 仅处理一个索引产品，保留其全部合格伙伴 |
| `--max-resubmits N` | `20` | 部分完成作业的有界续提，不是科学失败重试 |
| `--controller-poll-interval SECONDS` | `60` | manifest/worker 标记轮询间隔 |

资源与下载环境变量：

| 环境变量 | 默认值 | 作用 |
| --- | --- | --- |
| `ASKAP_PARALLEL_SLOTS`、`ASKAP_CPU_PER_SLOT` | `4`、`8` | 四槽请求32 CPU，观测少时降低实际槽数 |
| `ASKAP_CREATE_MODEL_THREADS` | 每槽CPU（`8`） | DStools create-model 的 `-j`，必须在1…每槽CPU之间 |
| `ASKAP_SBATCH_MEM`、`ASKAP_SBATCH_TMP` | `160G`、`10G` | 整作业内存/scratch，不是每槽申请；内存不会自动随槽数缩小 |
| `ASKAP_SBATCH_PARTITION` | 空 | 可选分区，空值使用 Slurm 默认 |
| `ASKAP_MAX_MS_PER_JOB` | `40` | 每计划批次独立观测组数，同时受时间容量限制 |
| `ASKAP_MAX_JOB_WALLTIME_HOURS` | `48` | 估时申请与分批上限，允许>0且≤48，并非固定请求时间 |
| `ASKAP_HOURS_PER_OBS` | `2.0` | 每个已准备 MS 的建模工作估时，小时 |
| `ASKAP_SHARED_TARGET_HOURS` | `0.5` | 每个未完成目标额外处理估时，小时 |
| `ASKAP_SOURCE_OVERHEAD_HOURS` | `2.0` | 加到最忙槽负载上的整作业时间余量 |
| `ASKAP_WALLTIME_RESERVE_MINUTES` | `20` | 接近共享作业截止时间时不启动新工作 |
| `ASKAP_MAX_AUTO_RESUBMITS` | `20` | CLI `--max-resubmits` 默认值 |
| `ASKAP_CONTROLLER_POLL_INTERVAL_SECONDS` | `60` | CLI 轮询间隔默认值 |
| `ASKAP_STAGING_BUDGET_BYTES` | `2199023255552`（2 TiB） | 程序 staging 预算，不能覆盖文件系统/用户配额 |
| `ASKAP_CASDA_SOURCE_DOWNLOAD_RETRIES`、`ASKAP_CASDA_SOURCE_RETRY_DELAY_SECONDS` | `3`、`30` | 下载尝试次数和间隔，与处理重试分开 |

DStools/输出环境变量（规划新运行前设置）：

| 环境变量 | 默认值 | 作用 |
| --- | --- | --- |
| `ASKAP_DSTOOLS_BAND` | `AK_low` | 模型成像预设 |
| `ASKAP_CREATE_MODEL_ITERATIONS` | `100000` | create-model 迭代次数 |
| `ASKAP_MIN_UV_METRES` | `200` | 模型成像最小基线 |
| `ASKAP_CREATE_MODEL_MULTISCALE` | `1` | 启用多尺度；`0/false/no/off` 关闭 |
| `ASKAP_EXTRACT_MIN_UV_METRES` | `500` | dynamic spectrum 提取最小基线 |
| `ASKAP_CROP_ARCMIN` | `10` | 正方形 I/V cutout 的边长，角分 |
| `ASKAP_DEFAULT_T_MIN`、`ASKAP_MJD_J2000` | `61041.5`、`51545` | 最后历元后备值与自行修正参考 MJD |

示例：两槽，每槽40G，先准备再提交：

```bash
export ASKAP_SBATCH_MEM=80G
"$ASKAP_PYTHON_BIN" ozstar_main.py --begin 1501 --end 1550 --parallel-slots 2 --prepare-download --no-submit
"$ASKAP_PYTHON_BIN" ozstar_main.py --begin 1501 --end 1550 --parallel-slots 2 --submit
```

不要修改已冻结作业，或在计划中途更换槽数、SIF、成像配置。新设置影响后续工作/
模型key；改变成像设置不会自动重建或覆盖已经完整且身份匹配的产物。
`ASKAP_JOB_WALLTIME_HOURS`、`--controller-initial-delay`、`--allow-multiple` 等
旧模式控制项不是共享模式的调参入口。`--legacy-source-mode` 仅作兼容入口，
不属于正常流程；不能在同一活跃工作区混用两种模式。共享模式不支持计算节点下载。

## 4. 检查进度与失败

```bash
"$ASKAP_PYTHON_BIN" show_failures.py
```

只读输出 `UCSxxxx_SBxxxxx —— 失败步骤`。在尚未开始任何处理时，数据库不存在
并不表示科学处理失败。

| 报告 CLI 参数 | 默认值 | 作用 |
| --- | --- | --- |
| `--details` | 关闭 | beam、重试阶段/次数、错误与保留的失败日志 |
| `--retryable-only` | 关闭 | 隐藏耗尽失败，不会启动重试 |
| `--source UCSxxxx` | 全部源 | 可重复指定多颗源 |
| `--database PATH` | `$ASKAP_WORK/state/sb_processing.sqlite` | 读取另一处理数据库 |
| `--json` | 关闭 | 机器可读报告 |

其他只读检查：

```bash
hostname
squeue -u qhuang
"$ASKAP_PYTHON_BIN" - "$ASKAP_WORK" <<'PY'
import json
import sys
from pathlib import Path
path = Path(sys.argv[1]) / 'state/shared-controller.json'
if path.is_file():
    state = json.loads(path.read_text())
    print({key: state.get(key) for key in ('status', 'begin', 'end', 'group_index', 'active_manifest')})
else:
    print('No shared plan has been created yet')
PY
ls -lt "$ASKAP_WORK/logs/"slurm-*.err
"$ASKAP_PYTHON_BIN" show_failures.py --details
```

读取对应的 `slurm-JOBID.err` 与 `logs/failures/` 保留日志。队列为空、Slurm
COMPLETED、controller 退出0，单独都不能证明每个观测成功。种子计划完成后，
伙伴的其他观测仍可能 pending。见[状态与清理](REFERENCE.md#5-状态与清理)。

## 5. 恢复或显式重试失败

恢复中断的 controller 时，使用**相同工作目录和原始范围/重试模式/key/并行槽数**。
普通运行示例：

```bash
"$ASKAP_PYTHON_BIN" ozstar_main.py --begin 1501 --end 1550 --submit
```

如果原进程仍在另一登录主机运行，不要启动第二个 controller。不要删除被占用的
锁或状态文件来强行推进。若提交回执缺失，先检查已排队/运行作业；恢复逻辑会拒绝
猜测并再次提交。

此前计划结束后，显式重试已有处理失败项：

```bash
"$ASKAP_PYTHON_BIN" ozstar_main.py --begin 1501 --end 1550 --retry-existing --submit
```

| CLI 参数 | 默认值 | 作用 |
| --- | --- | --- |
| `--retry-existing` | 关闭 | 仅选择已有失败观测，不包含新目标、成功目标和耗尽目标 |

先 reset/preprocess 并独立重建模型；再次失败后，仅清除该目标的 MS/checksum，
再重新下载并处理一次。仍失败则记为 `exhausted`，保留最后失败输入，不再重试。
普通运行没有这一标志时会跳过失败记录。下载/网络尝试上限与这一处理策略分开。

## 6. 在 Ada 拉取全部完整产物

完成安装后，在 Ada 执行：

```bash
cd /import/ada1/qhua0119/Combine_ImagePlot/Ozstar/bin
python3 pull_askap_products.py --all \
  --source-root /fred/oz299/qhuang/ASKAP-UCDs-production/products \
  --receipt-remote-root /fred/oz299/qhuang/ASKAP-UCDs-production/state/transfer_receipts
```

程序扫描全部完整 DS/I/V 三件套，增量拉取缺失/hash不符文件，校验 SHA256 并上传
验证回执，不删除 Ozstar 产物。**新生产工作区必须显式指定这两个根目录**；
脚本默认值仍指向旧 `/fred/oz299/qhuang/ASKAP-UCDs`。

运行前复用受保护密码文件，或隐藏输入密码：

```bash
ADA_ROOT=/import/ada1/qhua0119/Combine_ImagePlot/Ozstar
if test -z "${TRANSFER_PASSWORD:-}" && ! test -s "$ADA_ROOT/state/ozstar-transfer-password"; then
  read -r -s -p "Ozstar password: " TRANSFER_PASSWORD
  printf '\n'
  export TRANSFER_PASSWORD
fi
```

没有密码来源时也支持 SSH key/SSH 交互提示。若通过环境输入密码，最后一次拉取
后 unset `TRANSFER_PASSWORD`。

| 拉取 CLI 参数 | 默认值 | 作用 |
| --- | --- | --- |
| `--all` 或重复 `--batch UCS1501-1550` | 必须选择一项 | 全部完整观测，或指定50源命名文件夹 |
| `--dry-run` | 关闭 | 读取/hash索引并报告待传文件，不传输或写台账/回执 |
| `--source-root` | `/fred/oz299/qhuang/ASKAP-UCDs/products` | Ozstar 产物根目录，必须匹配实际处理工作区 |
| `--ada-root` | `/import/ada1/qhua0119/Combine_ImagePlot/Ozstar` | 全部 Ada 输出必须位于这一允许目录内 |
| `--database` | `<ada-root>/state/transfer_ledger.sqlite` | Ada 传输台账 |
| `--receipt-remote-root` | `/fred/oz299/qhuang/ASKAP-UCDs/state/transfer_receipts` | Ozstar 回执目录，选择匹配的生产工作区 |
| `--method rsync\|scp` | `rsync` | 传输方式 |
| `--remote-user`、`--remote-host` | `qhuang`、`ozstar.swin.edu.au` | 源 SSH 身份 |
| `--remote-python` | `/usr/bin/python3` | 远端标准库解释器，不使用依赖module的项目venv |
| `--password-file` | `<固定Ada根目录>/state/ozstar-transfer-password` | 受保护 Ozstar 密码文件；`TRANSFER_PASSWORD` 环境值优先 |
| `--connect-timeout` | `30` | SSH 建连超时秒数，不是传输总时限 |

`--no-receipt-upload` 是诊断入口，不用于生产流程。不要以为 `config.py` 的 SSH
变量会改变这个独立拉取工具的默认值，应使用它的 CLI 参数。健康的重复拉取不传
产物文件，但仍核对 hash 并写审计尝试/回执，见[传输机制](REFERENCE.md#6-传输与空间管理)。

## 7. 可选的单坐标独立下载

```bash
"$ASKAP_PYTHON_BIN" ../Download_ASKAP_Visibility_Remote.py
```

先手动修改脚本的 `RA`、`Dec`、`OPAL_USER`、`casda_filepath`（使用 FRED 绝对
输出路径），可选修改 `RADIUS_FREQUENCY_MODE`（`central`/`top`）或 `QUERY_LIMIT`。
它不是生产 controller：曝光阈值与查询上限不同，不提交作业，并使用 Astroquery
自身的交互登录/keyring 行为，不要原样执行来替代索引流程。
