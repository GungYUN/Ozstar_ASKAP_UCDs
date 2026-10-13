# 全表交叉索引与共享模型处理

## 当前状态与边界

此版本在本地实现并测试；本地测试模拟 CASDA、Slurm 和 DStools，真实运行 SQLite、
CSV/ECSV、tar 解压和 FITS 裁剪。**这不替代 Ozstar 上真实 MS 的科学验收。**
不修改旧项目副本、远端程序、已有 SIF，也不自动上传 GitHub。

新版代码位置为本地 Git 仓库 `Ozstar_ASKAP_UCDs/ozstar_askap/`。
之前的 `ASKAP-UCDs/ozstar_askap/` 是旧版副本，不会自动同步。
不要在旧作业尚未结束时将新程序覆盖到旧作业使用的代码目录。

## 处理规则

1. 登录节点查询 catalogue 内的**每一颗源**，不局限于 `--begin/--end`。
   每次先在 1° 内查询，默认用中心频率对应的半功率半径过滤，并保留原有的
   quality、公开状态、SB 排除、大小和每 SB 最近 beam 的筛选规则。
   `em_min/em_max` 按波长单位转换为米；中心频率为 `(c/em_min + c/em_max)/2`，
   半径为 `0.6° × 1.4 GHz / νcentral`。`--radius-frequency-mode top` 可切回
   最高频率 `c/em_min`，`--radius-frequency-mode central` 可显式指定中心频率。
   CLI 优先于 `ASKAP_CASDA_RADIUS_FREQUENCY_MODE` 环境变量。
   保留原来的光谱型处理阈值；CSV 中的
   `eligible` 表示是否达到该阈值。低于阈值的源仍查询、统计，但不进入计算。
2. 共享必须有相同 SB、beam、文件名、publisher DID 和频段，不能只靠天空距离。
   两颗源可以相距超过 1°，只要各自独立筛选后选择同一数据产品即可共享。
3. 范围只决定种子与顺序。种子选中的观测会包括全表中选中同一产品的伙伴，
   即使伙伴在范围外；不会进一步展开伙伴的其他观测。
4. 登录节点下载每个观测的全部 URL（tar + checksum）一次交给 `download_files()`。
   不新增 checksum 内容校验。计算节点用命令行 tar 解压，保留 checksum，仅删除 tar。
5. 公共数据区 `staging/_shared/<model_key>/extracted/` 保留未处理原始 MS，复制到
   `master/` 后先 preprocess、再 create-model。**各目标复制的是已 preprocess 的
   master MS 及其完整模型，不是 extracted 内的原始 MS。**公共模型的 key 还包含 SIF SHA256 与成像参数。
   **公共模型不能做 insert、减源或裁剪。**
6. 各目标按顺序获得独立 MS 和完整模型文件副本（不用硬链接），从 insert 开始
   做 subtract/extract，再按各自自行修正坐标裁剪 Stokes I/V。
   同一个公共模型只创建一次；目标的掩膜、CORRECTED_DATA 与裁剪互不影响。
7. 每个源/SB/beam 的 SQLite 行分别记录步骤、错误与重试次数，新增
   `observation_key`、`model_origin`、`model_reused`。公共阶段耗时在 manifest 中保存，
   各目标复用公共阶段记 Y、耗时 0，不重复计算成像时间。
8. 每个目标只有 DS、I、V 三个文件复制到 products 后才清理自己的 MS。
   所有已索引伙伴均完成后才清理公共原始 MS、处理 MS、模型与临时数据。
   有失败/未处理伙伴时公共数据保留。产品只按文件名存在判断，不增加 DS/FITS
   内容验证；FITS 裁剪本身当然需要读取图像。
9. 输出目录仍为 `products/UCSxxxx-yyyy/UCSxxxx/LongObs/SBxxxxx_beamyy/`，
   DS 和 I/V 文件名不变。新添 `observation.json` 记录处理来源；ada 拉取仍识别原三件套。
   旧产物无 sidecar 时，只有 SQLite 中的原 CASDA 文件名一致才会被采纳。
   若已有文件/记录对应不同数据产品，程序停止并要求人工归档旧记录，不覆盖。

## 索引与查询状态

| 文件（位于 `ASKAP_WORK/state/`） | 用途 |
| --- | --- |
| `crosscatalogue_sources.csv` | 全表每颗源一行，包括无观测/查询失败/尚未查询的源 |
| `crosscatalogue_observations.csv` | 每颗源的每个最终观测一行；同一观测可对应多颗源 |
| `crosscatalogue-summary.json` | 查询分类计数、新增/移除观测、共享组和参与源数量 |
| `crosscatalogue.json` | 发布后的机器可读索引；不要手动修改 |
| `crosscatalogue-update.json` | 尚未完成或最近一次更新的检查点 |
| `crosscatalogue_rows/<revision>/*.ecsv` | 冻结的完整 CASDA 行，供后续 stage_data 使用 |
| `crosscatalogue_rows/<revision>/*.query.json` | 每个源的查询检查点，避免逐源重写整张大索引 |
| `shared-controller.json`、`jobs/shared-*.json` | 当前计划和各批工作 manifest |

并行批 manifest 的 `tasks` 指向各观测 manifest，`lane` 表示处理槽。
每个观测独立保存结果；兼容恢复旧版单观测 manifest，不重复提交已结束的观测。

CSV 为 UTF-8，首行是字段标题，TOPCAT 可按 CSV 打开。注释用 `query_note` 列，
不在 CSV 前塞入会影响解析的自由文本。字段说明如下：

- `query_status` 互斥：`available` 有最终观测；`no_observations` 查询成功但最终为零；
  `query_failed` 查询或元数据处理失败；`unknown` 本轮尚未查询（含中断时未处理的源）。
- `candidate_count` 是 1° 初查数量；`selected_count` 是全部筛选后的数量。
  查询失败时留空，不伪装成 0。`query_note` 区分初查零行和筛选后零行。
- `query_time_utc` 是尝试时间；`last_success_time_utc` 是最近成功时间。
- `using_previous_result=True` 表示保留了上次结果但本轮未成功；这些旧行仍可查看，
  **本轮调度不使用失败/未知源的旧结果**，也不把它们计入当前共享人数。
- `key` 唯一标识实际观测产品；`shared_source_count/shared_sources` 给出共享数量和源名。
- 文件名解析兼容 RACS 的 `SBxxxxx...beamyy` 与 HIPASS 的 `SBxxxxx_Fxx_Byy`。
  HIPASS 的 `Byy` 映射为 `beamyy`（保留前导零）；`Fxx` 仍保存在完整文件名/共享 key 中，
  不改名、不修改 MS 内容，不将不同 field 产品仅凭 SB/beam 合并。
- `frequency_min_ghz/frequency_max_ghz`、`separation_deg/half_power_radius_deg`
  给出频段、目标至 beam 中心角距和可用半径。
- `frequency_central_ghz` 是频率上下限的算术平均；`radius_frequency_mode`
  和 `radius_frequency_ghz` 标明实际半径计算使用 top 还是 central。
  默认是 central；[频率策略对比测试](FREQUENCY_COMPARISON.md)在独立目录进行。
- `added_count/removed_count` 是成功重查相对旧索引的变化；更新不是只追加新行，
  原观测消失或最近 beam 改变也会反映出来。不会因更新删除已生成科学产物。

第一次正常运行自动建立全表索引。以后直接读取；catalogue 内容或筛选配置改变
时会重新建索引。`--update-crosscatalogue` 显式重新查询所有源，绕过旧 TAP cache。
缺少频率模式标记的历史索引仍按 top 解释，不会因新默认值被误标为 central。
已完成且配置一致的 central 索引可直接复用，不必再次 `--update-crosscatalogue`。
中断后相同命令从检查点继续；完成后再次加该参数才开启新一轮查询。
运行中的计划冻结索引版本；有未完成计划时拒绝更新索引或更换种子范围。
CSV 在查询开始、结束或正常处理中断时导出；执行中的最新进度看 query 日志。

### 定向恢复失败索引，不重查成功源

若全表查询留下失败记录，或所有逐源检查点已经生成但最终发布因配额等原因中断，
在原 `ASKAP_WORK` 中执行：

```bash
"$ASKAP_PYTHON_BIN" ozstar_main.py \
  --retry-crosscatalogue-failures --crosscatalogue-only
```

该入口恢复逐源检查点，仅重查 `query_failed` 和尚未完成的源；成功的查询和 ECSV
保留原样，新查询使用新 revision，最后重新发布 JSON、两个 CSV 和 summary。
中断后相同命令继续当前定向恢复；完成后再显式调用则重试仍失败的源。
它不是科学处理的 `--retry-existing`，不会下载 MS、提交 Slurm 或修改处理数据库。
只允许索引模式，不能同时指定 `--update-crosscatalogue`/`--refresh-cache`。
catalogue/筛选配置必须和原索引一致，运行中的共享处理计划必须先结束。
如果磁盘配额仍无法写入，修复命名不能绕过该限制，不要删除成功检查点。

最终人工检查文件为 `state/crosscatalogue_sources.csv`（全部源含失败原因）和
`state/crosscatalogue_observations.csv`（观测、频率、距离、共享信息）。应先确认
`crosscatalogue.json` 已有 `finished_at`、summary 的 `unknown=0`；若
`query_failed>0`，发布已结束但仍有源未通过查询，不应视作全表验证成功。

## 资源、批次与重试

- 同时只运行一个共享 sbatch。默认 `ASKAP_PARALLEL_SLOTS=4`，每个槽处理一个
  独立 SB/beam 观测组，组内的目标串行；槽内可以依次处理多个观测，形成多轮。
  `--parallel-slots N` 覆盖环境设置，便于测试；每槽默认 8 核，4 槽请求 32 核，
  CPU 总数不能超过 32。观测不足 4 个时减少实际请求槽数，不空占 CPU。
  同一个观测不会同时安排到两个槽，即使其目标需要分批。
  当前批最多纳入 `ASKAP_MAX_MS_PER_JOB` 个独立观测（默认 40），也受时间容量限制。
  每槽独立 Python 子进程，避免配置/工作目录互相影响；SQLite 使用短事务和等待锁。
  MS/模型复制时使用 `shared-copy.lock` 串行检查空间并复制，避免四路同时检查时低估
  已占空间；耗时的成像和减源等步骤仍并行。
  默认 `ASKAP_SBATCH_MEM=160G`，四槽时每槽预算 40G；WSClean 上限约 30G，
  余下约 10G 留给 Python、MS 操作等。这是程序侧预算，不是每槽独立的硬性内存隔离。
  已设置的环境变量或已有 sbatch 脚本不会随默认值更新而自动改变。
  整个 sbatch 的 RAM 平分给实际槽数，DStools 的 MS 分块使用该额度；项目中的
  `wsclean_slot/wsclean` 为未显式指定资源的调用（尤其 insert）补充 `-j` 和
  `-abs-mem`，create-model 显式设置内存约每槽额度的 75%，为其他操作留余量。
  不修改 SIF。必须一并部署该目录并保留脚本的执行权限；总 tmp 仍为整作业额度，
  WSClean 临时数据保留在各观测独立的 FRED 路径。
- 下载后检查 tar 内 MS 数（一个共享数据产品必须对应一个 MS），或确认已有 MS。
  每观测估时 `MS数 × ASKAP_HOURS_PER_OBS + 未完成目标数 × ASKAP_SHARED_TARGET_HOURS`，
  默认 2h/MS 和 0.5h/目标。已保留的公共 master 可复用时不重复计入建模时间。
  各观测分配到当前估计负载最少的槽；实际申请时间为
  `min(上限, 最忙槽的累计估时 + ASKAP_SOURCE_OVERHEAD_HOURS)`，固定余量默认 2h。
  例如 4 个观测各有 2 个目标：各槽 2+2×0.5=3h，申请 3+2=5h，而非 48h。
- `ASKAP_MAX_JOB_WALLTIME_HOURS` 默认/允许上限为 **48h**，可降低进行测试。
  达到槽容量后拆为下一批；目标过多时拆分该观测，下一批使用保留的公共模型。
  低于单个任务估时的测试上限仍允许首次尝试，必要时续提。
  所有槽共享本次 sbatch 的截止时间（不是每个观测重新获得全部时间）；临近预留
  时间退出 75，由登录 controller 有界续提（`--max-resubmits`），不在计算节点自行提交。
  续提跳过完成/失败的观测，只处理未完成内容；科学失败仍只通过显式重试恢复。
  共享模式不生成 heartbeat 或 resubmit-pending 文件，也不生成 worker 自行提交代码。
  `.worker_finished/.worker_failure` 与提交回执仍保留，它们被 controller 实际使用。
  controller 轮询 manifest 和退出标记：`finished` 推进下一批，`partial` 有界续提；
  常规异常记录未完成目标失败后推进。若 shell 被强杀未留下标记，连续两次确认
  `sacct` 的终止状态后记录异常；空队列或 accounting 查询失败不视作完成。
  数据库等致命状态错误停止推进。登录 controller 必须保持运行，才能自动提交后续批次。
- 正常运行不重试已失败的源/SB。`--retry-existing` 专门选择已有失败记录，
  不包含首次处理的新目标。第一次显式重试从 preprocess 开始独立重建模型；
  再失败只清除该目标的 MS/checksum，回登录节点重新下载一次，再失败即 `exhausted`。
  不删除、不修改伙伴的公共模型或科学产物。
- 首次公共建模失败而尚无目标 MS 副本时，第一次显式重试使用保留的原始 MS，
  不立即重复下载。下载本身的网络重试仍沿用现有次数设置。
- 常规失败记录后继续其他目标/观测。数据库无法可靠写入时停止，不把丢失记录
  当成成功。Slurm accounting 仅用于识别异常终止，不能证明科学处理完成。
- 中断后用同范围、同重试模式、同 observation-key、同 `--parallel-slots` 设置恢复。
  并行数量只在旧计划完成后调整，不在运行中改写已冻结作业。
  提交前后若恰好中断而 job ID 未持久化，会停在 `submitting` 要求检查队列；
  不会猜测并重复提交。不要手动删除状态文件来强行重跑。
- 有旧的部分处理目录时，新副本替换前将其保留为 `SB*_beam*.previous-*`。
  这些安全备份不会自动删除，应在验收/排查后人工清理；磁盘预算会计入它们。

## 部署到独立程序目录（本地 Mac）

先保留现有 Ozstar 作业使用的代码。下面只上传新版到独立目录，不覆盖旧程序：

```bash
cd /Users/qhua0119/Documents/ChatGPT/Ozstar/Ozstar_ASKAP_UCDs/ozstar_askap
# 无密码文件时，按提示隐藏输入 Ozstar 登录密码。
OZSTAR_WORK=/fred/oz299/qhuang/ASKAP-UCDs-shared \
  bash deploy_to_ozstar.sh
```

`OZSTAR_WORK` 控制上传位置；`ASKAP_WORK` 控制运行数据位置，两者不同。
部署脚本保留原有凭据方式；没有环境值/密码文件时提示隐藏输入密码，不写入磁盘。
不需修改账户密码或重新构建 SIF。

## 隔离验证：全表索引 + 一个真实共享观测

以下在 **Ozstar 登录节点** 的 tmux 中操作；处理通过 sbatch，不必进入交互式节点。
环境变量只对当前 shell 有效；重连后需要重新设置，尤其不要把 `ASKAP_WORK` 留空。

```bash
cd /fred/oz299/qhuang/ASKAP-UCDs-shared/ozstar_askap
module load gcc/13.3.0
module load python/3.12.3
export ASKAP_PYTHON_PARENT_MODULE=gcc/13.3.0
export ASKAP_PYTHON_MODULE=python/3.12.3
export ASKAP_PYTHON_BIN=/fred/oz299/qhuang/ASKAP-UCDs/.venv/askap-python/bin/python
export DSTOOLS_CONTAINER=/fred/oz299/qhuang/dstools/dstools-260925.sif
export ASKAP_CATALOGUE=/fred/oz299/qhuang/ASKAP-UCDs/catalogue/UltracoolSheet_Main_index_unbinaryUCD.csv
export ASKAP_TIME_CSV=/fred/oz299/qhuang/ASKAP-UCDs/catalogue/All_combine_data_unrepetition.csv

# 第一次建立验证目录；恢复时请改用之前的明确路径，不要再生成新日期目录。
export ASKAP_WORK=/fred/oz299/qhuang/dstools/shared-validation/run-$(date +%Y%m%d-%H%M%S)
mkdir -p "$ASKAP_WORK/logs"
printf 'Validation ASKAP_WORK=%s\n' "$ASKAP_WORK"

export ASKAP_PARALLEL_SLOTS=1
export ASKAP_CPU_PER_SLOT=8
export ASKAP_CREATE_MODEL_THREADS=8
export ASKAP_SBATCH_MEM=32G
export ASKAP_SBATCH_TMP=10G
export ASKAP_MAX_JOB_WALLTIME_HOURS=2
unset ASKAP_GATE6_FAULT_CONFIG

"$ASKAP_PYTHON_BIN" -m py_compile \
  crosscatalogue.py shared_controller.py shared_worker.py \
  ozstar_main.py process_job.py sb_status.py
"$ASKAP_PYTHON_BIN" -m unittest discover -s tests -v

set -o pipefail
"$ASKAP_PYTHON_BIN" ozstar_main.py \
  --update-crosscatalogue --crosscatalogue-only \
  2>&1 | tee "$ASKAP_WORK/logs/crosscatalogue.log"
printf 'Query exit code: %s\n' "${PIPESTATUS[0]}"
```

全表查询可能较慢，应先看 `crosscatalogue-summary.json` 和两个 CSV。
若大面积 query_failed，不要开始处理，先确认 CASDA 服务情况；无需重复下载 MS。

选择一个参与源较少的共享观测（不硬编码哪个源一定共享）：

```bash
"$ASKAP_PYTHON_BIN" - "$ASKAP_WORK" <<'PY'
import csv, json, sys
from pathlib import Path
work = Path(sys.argv[1])
print((work / 'state/crosscatalogue-summary.json').read_text())
with (work / 'state/crosscatalogue_observations.csv').open() as handle:
    rows = [r for r in csv.DictReader(handle)
            if r['query_status'] == 'available' and r['eligible'] == 'True'
            and 2 <= int(r['shared_source_count']) <= 4]
if not rows:
    raise SystemExit('没有 2–4 个目标的共享观测，请查看 CSV 后人工选择，不要假设已验证共享。')
row = sorted(rows, key=lambda r: (int(r['shared_source_count']), int(r['ucs_number']), r['key']))[0]
pilot = {'ucs': int(row['ucs_number']), 'key': row['key'], 'sources': row['shared_sources']}
(work / 'state/shared-pilot.json').write_text(json.dumps(pilot, indent=2))
print(pilot)
PY

TEST_UCS=$("$ASKAP_PYTHON_BIN" -c \
  'import json,os; print(json.load(open(os.environ["ASKAP_WORK"]+"/state/shared-pilot.json"))["ucs"])')
OBS_KEY=$("$ASKAP_PYTHON_BIN" -c \
  'import json,os; print(json.load(open(os.environ["ASKAP_WORK"]+"/state/shared-pilot.json"))["key"])')

"$ASKAP_PYTHON_BIN" ozstar_main.py \
  --begin "$TEST_UCS" --end "$TEST_UCS" \
  --shared-observation-key "$OBS_KEY" --no-submit

# 检查 state/jobs/shared-*.json 与 .sbatch：ASKAP_WORK 必须是本次隔离目录。
# 确认后运行；2h 上限可能让各目标分到顺序提交的多个作业，仍共享同一模型。
"$ASKAP_PYTHON_BIN" ozstar_main.py \
  --begin "$TEST_UCS" --end "$TEST_UCS" \
  --shared-observation-key "$OBS_KEY" --submit \
  2>&1 | tee "$ASKAP_WORK/logs/shared-pilot.log"
printf 'Controller exit code: %s\n' "${PIPESTATUS[0]}"
```

controller 结束本身不代表所有科学步骤成功。检查每个目标、日志和文件：

```bash
"$ASKAP_PYTHON_BIN" show_failures.py \
  --database "$ASKAP_WORK/state/sb_processing.sqlite" --details

"$ASKAP_PYTHON_BIN" - "$ASKAP_WORK" <<'PY'
import json, sqlite3, sys
from pathlib import Path
work = Path(sys.argv[1])
plan = json.loads((work/'state/shared-controller.json').read_text())
assert plan['status'] == 'complete', plan['status']
assert plan['jobs'], 'No jobs were run'
completed = set()
for name in plan['jobs']:
    job = json.loads(Path(name).read_text())
    print(job['observation']['sb_name'], job['results'], job.get('shared_model_seconds'))
    for source in job['sources']:
        result = job['results'].get(source['source_name'], {})
        assert result.get('status') == 'complete', (source['source_name'], result)
        sb = job['observation']['sb_name']
        folder = work/'products'/source['batch']/source['source_name']/'LongObs'/sb
        expected = [folder/(sb+'.ds')] + [folder/'wsclean_model'/f'wsclean-MFS-{pol}-image.fits' for pol in ('I','V')]
        assert all(p.is_file() for p in expected), expected
        completed.add(source['source_name'])
assert len(completed) >= 2, 'This did not exercise model sharing'
db = sqlite3.connect(str(work/'state/sb_processing.sqlite'))
try:
    for row in db.execute('SELECT source_name,sb_id,beam,model_reused,retry_stage,error_step FROM sb_processing'):
        print(row)
        assert row[3:] == (1, 'complete', None), row
finally:
    db.close()
print('Shared pilot filename/status checks: PASS')
PY

```

在没有超时/失败的干净验证中，manifest 的 `shared_model_built_this_job` 应仅一次为 true。
并行作业成功的子进程 stdout/stderr 不保留；失败的完整处理日志保存在 `logs/failures/`。
不同目标的 insert/extract 坐标和 cutout 中心应各不相同。请人工在 TOPCAT/FITS
查看器中确认每颗源的图像位置；如需科学一致性比较，另用独立下载副本跑原来的
单源处理，对比 DS 与 cutout，不能把“文件名存在”解释为科学数值验证已完成。

## 隔离验证：四观测并行

前面的单观测试验完成后，从 CSV 中选择一颗尚未处理、约 4–8 个可用观测且
共享人数较少的源，将其**数字编号**设为 `TEST_UCS`。仍使用同一个隔离 `ASKAP_WORK`。
先预下载并检查生成的批次，不直接切换生产数据库：

```bash
: "${TEST_UCS:?请先从 CSV 选择并设置 TEST_UCS 数字编号}"
export ASKAP_MAX_JOB_WALLTIME_HOURS=2
# 这是整作业内存；128G / 4 = 32G 每槽，与此前单槽验证额度一致。
export ASKAP_SBATCH_MEM=128G
export ASKAP_SBATCH_TMP=10G
"$ASKAP_PYTHON_BIN" ozstar_main.py \
  --begin "$TEST_UCS" --end "$TEST_UCS" --parallel-slots 4 \
  --prepare-download --no-submit

"$ASKAP_PYTHON_BIN" - "$ASKAP_WORK" <<'PY'
import json, sys
from pathlib import Path
work = Path(sys.argv[1])
plan = json.loads((work/'state/shared-controller.json').read_text())
job = json.loads(Path(plan['active_manifest']).read_text())
print('Slots:', job.get('parallel_slots', 1), 'Walltime:', job['job_walltime_hours'])
print(Path(job['script_path']).read_text())
for entry in job.get('tasks', [{'manifest_path': job['manifest_path'], 'lane': 0}]):
    task = json.loads(Path(entry['manifest_path']).read_text())
    print(entry['lane'], task['observation']['sb_name'], [s['source_name'] for s in task['sources']])
assert job.get('parallel_slots') == 4, '当前待处理观测不足四路，请选择其他源完成本次计划后再测试'
assert 'heartbeat' not in Path(job['script_path']).read_text()
PY

# 确认脚本请求 32 CPU、128G RAM、10G tmp、时间 <=2h，然后恢复同一个计划。
"$ASKAP_PYTHON_BIN" ozstar_main.py \
  --begin "$TEST_UCS" --end "$TEST_UCS" --parallel-slots 4 --submit
```

只因观察不到队列任务不能认定成功。检查 controller 完成状态、`show_failures.py`、
每个观测 manifest 的 `results` 与各目标 DS/I/V；并行批的结果保存在 `tasks` 指向的文件。
`--no-submit` 已冻结计划，后续必须使用相同编号/并行数恢复，不要另开同目录的 controller。
2h 为每次请求上限，完整范围可能分成多次作业；已有 master 不会因跨批而重复建模。

## 验收后正式使用

旧任务完成后再决定切换；代码可继续位于独立的 `ASKAP-UCDs-shared/ozstar_askap`，
无需覆盖旧程序。新 shell 设置模块、Python、SIF、catalogue 等环境变量后：

```bash
export ASKAP_WORK=/fred/oz299/qhuang/ASKAP-UCDs
export ASKAP_PARALLEL_SLOTS=4
export ASKAP_CPU_PER_SLOT=8
export ASKAP_CREATE_MODEL_THREADS=8
export ASKAP_SBATCH_MEM=160G
export ASKAP_SBATCH_TMP=10G
export ASKAP_MAX_JOB_WALLTIME_HOURS=48

"$ASKAP_PYTHON_BIN" ozstar_main.py --update-crosscatalogue --crosscatalogue-only
"$ASKAP_PYTHON_BIN" ozstar_main.py --begin 1501 --end 1550 --parallel-slots 4 --submit

# 已失败观测仅在明确要求时重试：
"$ASKAP_PYTHON_BIN" ozstar_main.py --begin 1501 --end 1550 --parallel-slots 4 --retry-existing --submit
```

不要直接复制隔离验证的处理数据库覆盖生产数据库。生产全表查询可以重跑，旧产物
通过其对应的原数据库记录识别；如果旧版仍有 queued/running/partial 等状态，
新版会拒绝与之混跑。请先确认旧作业确实结束，而不是删除标记规避检查。

回到旧调度时使用 `--legacy-source-mode`，且必须先结束共享计划。
已有 Gate 6/7 保留用于原有重试/传输行为的验证，不自动证明共享新路径通过验收。
