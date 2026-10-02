# Ozstar ASKAP/UCD Pipeline

This directory contains the Ozstar-side ASKAP/UCD pipeline. The normal
production command is run from an Ozstar login node inside `tmux` or `screen`.
The controller is resumable and advances only from manifests, source-state
JSON files, and verified filesystem products.

本目录包含 Ozstar 侧 ASKAP/UCD 流水线。正常生产命令在 Ozstar 登录节点的
`tmux` 或 `screen` 中执行。controller 可以恢复运行，并且只根据 manifest、源状态
JSON 文件和经过验证的文件系统产物推进，不根据 Slurm 队列猜测科学处理是否完成。

## 1. First-Time Installation / 首次安装

### English

1. Put this program directory on Ozstar. From a machine that can SSH to Ozstar,
   the optional deployment wrapper is:

   ```bash
   ./deploy_to_ozstar.sh
   ```

   The wrapper copies the directory below `OZSTAR_WORK` (default
   `/fred/oz299/qhuang/ASKAP-UCDs`). It requires `sshpass` and either a
   mode-600 transfer-password file or the one-process `TRANSFER_PASSWORD`
   environment variable. It never prints a password.

2. Copy the two input CSV files into the persistent catalogue directory:

   ```text
   /fred/oz299/qhuang/ASKAP-UCDs/catalogue/
       UltracoolSheet_Main_index_unbinaryUCD.csv
       All_combine_data_unrepetition.csv
   ```

   Use `ASKAP_CATALOGUE` and `ASKAP_TIME_CSV` when the files are mounted at
   different paths. The catalogue must contain the UCS, name, coordinate,
   proper-motion, and spectral-type columns used by `ozstar_main.py`.

3. Create the persistent layout:

   ```bash
   cd /fred/oz299/qhuang/ASKAP-UCDs/ozstar_askap
   ./prepare_layout.sh
   ```

   The first Python run also creates `state/controllers/`. Do not put
   visibility/MS data in `$TMPDIR`; `ASKAP_WORK` is the persistent root for
   staging, products, state, checkpoints, and logs.

4. Create the host-side Python environment:

   ```bash
   ./setup_host_python.sh
   module load gcc/13.3.0
   module load python/3.12.3
   export ASKAP_PYTHON_PARENT_MODULE=gcc/13.3.0
   export ASKAP_PYTHON_MODULE=python/3.12.3
   export ASKAP_PYTHON_BIN=/fred/oz299/qhuang/ASKAP-UCDs/.venv/askap-python/bin/python
   $ASKAP_PYTHON_BIN -c \
     'import numpy, pandas, astropy, astroquery, keyring; print("Python environment OK")'
   ```

   The setup script installs the packages in `requirements.txt` into the
   persistent venv. It only loads the configured modules. It does **not** run
   `module purge` or `module unload`; retain the existing site module/venv
   handling.

5. Confirm the DStools Apptainer image exists:

   ```text
   /fred/oz299/qhuang/dstools/dstools-260925.sif
   ```

   This is the validated production default. Override it with
   `DSTOOLS_CONTAINER` only when another candidate has passed the same gates.
   DStools, casacore, and WSClean run inside this image; the host venv is for
   orchestration, TAP, ECSV, and state management.

6. Configure credentials without placing secrets in this directory. For CASDA,
   create a mode-600 file containing only the password at:

   ```text
   ~/.config/askap/casda-password
   ```

   Set `CASDA_USERNAME` if necessary. `CASDA_PASSWORD` is a one-process
   override. The client uses an in-memory keyring backend, does not use the
   obsolete `password=` login argument, and does not persist the password.

### 中文

1. 将本程序目录放到 Ozstar。可以在能够 SSH 到 Ozstar 的机器上使用可选部署脚本：

   ```bash
   ./deploy_to_ozstar.sh
   ```

   该脚本把目录复制到 `OZSTAR_WORK` 下，默认是
   `/fred/oz299/qhuang/ASKAP-UCDs`。脚本需要 `sshpass`，以及 mode-600 的传输
   密码文件或只对当前进程生效的 `TRANSFER_PASSWORD` 环境变量；它不会打印密码。

2. 将两个输入 CSV 放入持久化 catalogue 目录：

   ```text
   /fred/oz299/qhuang/ASKAP-UCDs/catalogue/
       UltracoolSheet_Main_index_unbinaryUCD.csv
       All_combine_data_unrepetition.csv
   ```

   如果文件在其他挂载路径，使用 `ASKAP_CATALOGUE` 和 `ASKAP_TIME_CSV`。catalogue
   必须包含 `ozstar_main.py` 使用的 UCS、名称、坐标、自行和光谱型列。

3. 创建持久化目录：

   ```bash
   cd /fred/oz299/qhuang/ASKAP-UCDs/ozstar_askap
   ./prepare_layout.sh
   ```

   第一次运行 Python 程序时也会创建 `state/controllers/`。不要把 visibility/MS
   数据放入 `$TMPDIR`；`ASKAP_WORK` 是 staging、产物、状态、checkpoint 和日志的
   持久化根目录。

4. 创建主机侧 Python 环境：

   ```bash
   ./setup_host_python.sh
   module load gcc/13.3.0
   module load python/3.12.3
   export ASKAP_PYTHON_PARENT_MODULE=gcc/13.3.0
   export ASKAP_PYTHON_MODULE=python/3.12.3
   export ASKAP_PYTHON_BIN=/fred/oz299/qhuang/ASKAP-UCDs/.venv/askap-python/bin/python
   $ASKAP_PYTHON_BIN -c \
     'import numpy, pandas, astropy, astroquery, keyring; print("Python environment OK")'
   ```

   setup 脚本把 `requirements.txt` 中的包安装到持久化 venv。它只加载配置的
   module，不执行 `module purge` 或 `module unload`；保留现有的 site module/venv
   处理方式。

5. 确认 DStools 的 Apptainer 镜像存在：

   ```text
   /fred/oz299/qhuang/dstools/dstools-260925.sif
   ```

   这是已通过验证的正式默认镜像。只有另一个 candidate 通过同样的 gates 后，才使用
   `DSTOOLS_CONTAINER` 覆盖它。DStools、casacore 和 WSClean 在该镜像内运行；
   主机 venv 负责 orchestration、TAP、ECSV 和状态管理。

6. 不要把密码放入本目录。CASDA 密码文件只包含密码本身，并且权限必须是 mode-600：

   ```text
   ~/.config/askap/casda-password
   ```

   必要时设置 `CASDA_USERNAME`。`CASDA_PASSWORD` 只作为当前进程的覆盖值。client
   使用内存 keyring，不使用过时的 `password=` 登录参数，也不持久化密码。

## 2. Every-Run Workflow and Changeable Parameters / 每次运行流程和可修改参数

### English

Run the controller on an Ozstar login node. The range is inclusive, and
`--submit` includes login-node preparation, so a separate preparation command
is not needed for normal production:

```bash
cd /fred/oz299/qhuang/ASKAP-UCDs/ozstar_askap
module load gcc/13.3.0
module load python/3.12.3
export ASKAP_PYTHON_PARENT_MODULE=gcc/13.3.0
export ASKAP_PYTHON_MODULE=python/3.12.3
export ASKAP_PYTHON_BIN=/fred/oz299/qhuang/ASKAP-UCDs/.venv/askap-python/bin/python
export ASKAP_SBATCH_MEM=80G
$ASKAP_PYTHON_BIN ozstar_main.py --begin 1501 --end 1901 --submit
```

The normal sequence is:

1. Load the catalogue and query CASDA TAP within 1 degree for each new source.
   For every returned row, interpret ObsCore `em_min`/`em_max` as wavelength
   bounds in metres, use `c/em_min` as the highest observing frequency, scale
   ASKAP's 1.2-degree FWHM at 1.4 GHz as `1/frequency`, and keep the row only
   when the target lies within `FWHM/2` at that highest frequency. Existing
   quality, spectral-type, exposure, size, release, and nearest-`obs_id`
   filters are retained. Missing or invalid spectral bounds are rejected.
2. Write the complete filtered and deduplicated TAP rows to
   `state/tap_cache/*.ecsv`. Every returned column is retained, including
   `access_estsize` and returned `t_min`/`t_max`.
3. Sum the deduplicated `access_estsize` values. CASDA reports this field in
   KB, so the sum is converted to bytes. A controller block contains at most
   50 UCS numbers and is split before its estimate would exceed the 2 TB
   staging budget (`2 * 1024**4` bytes by default). A single source above the
   budget is recorded as failed and the controller continues. Current staging usage and actual free space below
   `ASKAP_WORK` are checked before download.
4. On the login node, authenticate to CASDA and download each visibility tar
   archive (`.tar`, `.tar.gz`, `.tgz`) together with its `.checksum` file in
   one CASDA download call. The login node does **not** extract archives.
5. Submit one short-name Slurm worker for one prepared UCS source. On the
   compute node, the worker extracts, processes, and crops at most 40 MS units
   per submission. A larger source is resumed only after its prior submission
   has completed its current chunk and returned the dedicated partial code.
6. The login controller polls source state and filesystem products. It advances
   after each source is `complete`, `complete_with_failures`, `no_data`, or `skipped`; a `complete`
   source must have the expected `.ds` and Stokes-I/V FITS filenames and no
   unprocessed MS/archive residue in staging. Product contents are deliberately
   not opened or format-validated.
7. When walltime protection returns the dedicated partial exit code, the
   generated sbatch script automatically resubmits itself up to
   `ASKAP_MAX_AUTO_RESUBMITS` (20 by default). No manual queue-based completion
   decision is required.

For a plan that writes TAP caches and manifests but neither downloads nor
submits, use `--no-submit`. `--prepare-download` is retained for an explicit
login-node preparation-only run. `--retry-existing` deliberately resets
incomplete controller state and retries only failures recorded in the SQLite
table; completed/no-data/skipped sources are not rerun. For each failed SB,
the first explicit retry removes downstream outputs, restores the raw MS from
the FixMS backup tables/columns, and starts again at ASKAP preprocess. If that
processing attempt fails, the same controller removes that SB's MS, archive,
checksum, and source download marker on the login node, downloads it once
again from CASDA, and performs one final preprocess-to-product attempt. A
second processing failure becomes `exhausted` and is not selected by later
`--retry-existing` calls. Normal runs without this flag never retry recorded
failures. Do not use it for an ordinary reconnect. The
`--allow-compute-download` option is a debugging opt-in and is not part of the
normal Ozstar network architecture.

Explicitly retry recorded failures in an inclusive range with:

```bash
$ASKAP_PYTHON_BIN ozstar_main.py --begin 1501 --end 1550 \
  --retry-existing --submit
```

Changeable runtime settings are read when the process starts. The defaults
below are the current operational defaults, not passwords or secret values:

| Area | Setting | Default and meaning |
|---|---|---|
| Range | `--begin`, `--end` | Inclusive UCS range; preferred per-run controls. |
| Range | `ASKAP_UCS_START`, `ASKAP_UCS_END` | Defaults used when CLI range arguments are omitted (`1`, `1000`). |
| Persistent paths | `ASKAP_WORK` | `/fred/oz299/qhuang/ASKAP-UCDs`; contains staging, products, state, and logs. |
| Input paths | `ASKAP_CATALOGUE`, `ASKAP_TIME_CSV` | Override the catalogue and legacy `obs_id` to `t_min` CSV paths. |
| Memory | `ASKAP_SBATCH_MEM` | `80G`; Slurm worker memory. Set this before every run when needed. |
| Scratch | `ASKAP_SBATCH_TMP` | `10G`; small Slurm scratch allocation only, not the MS/product store. |
| Partition | `ASKAP_SBATCH_PARTITION` | Empty by default; set to a site-approved Slurm partition when required. |
| Block/storage | `BLOCK_SIZE` | Fixed code limit of 50 UCS numbers per controller block; not a runtime override. |
| Block/storage | `ASKAP_STAGING_BUDGET_BYTES` | `2 * 1024**4`; estimated staging admission budget. |
| CASDA filtering | `ASKAP_CASDA_MAX_FILE_SIZE_KB` | Maximum filtered archive estimate per row. |
| CASDA beam filtering | `ASKAP_CASDA_INITIAL_QUERY_RADIUS_DEG`, `ASKAP_CASDA_REFERENCE_FWHM_DEG`, `ASKAP_CASDA_REFERENCE_FREQUENCY_GHZ` | Initial TAP radius `1.0` degree; ASKAP FWHM normalization `1.2` degrees at `1.4` GHz. The final radial cutoff is the highest-frequency `FWHM/2`. Changing any value automatically invalidates older TAP caches. |
| CASDA batching | `ASKAP_CASDA_STAGE_BATCH_SIZE`, `ASKAP_CASDA_QUERY_LIMIT` | CASDA staging batch size `20` and TAP row limit `5000`; reaching the row limit is an error rather than a silently truncated result. |
| Download retry | `ASKAP_CASDA_SOURCE_DOWNLOAD_RETRIES`, `ASKAP_CASDA_SOURCE_RETRY_DELAY_SECONDS` | Each source is retried `3` times with a `30` second delay; after exhaustion the controller records `download_failed` and continues. |
| Time estimate | `ASKAP_HOURS_PER_OBS` | `2.0` hours per MS observation wave for worker checks. |
| Time estimate | `ASKAP_SOURCE_OVERHEAD_HOURS` | `2.0` hours per source chunk for extraction, crop, and cleanup. |
| Slurm time | `ASKAP_MAX_MS_PER_JOB`, `ASKAP_MAX_JOB_WALLTIME_HOURS` | Defaults `40` MS and `23` hours: after download the archive member names determine `N`; each request is `ceil(min(N, 40)/4) * 2 h + 2 h`, capped below 24 h. |
| Walltime safety | `ASKAP_WALLTIME_RESERVE_MINUTES` | `20` minutes kept in reserve before a partial exit. |
| CPU | `ASKAP_PARALLEL_SLOTS` | `4` concurrent SB waves. |
| CPU | `ASKAP_CPU_PER_SLOT` | `8` threads per DStools process; total default is `4 x 8 = 32` CPUs. The product must not exceed 32. |
| CPU | `ASKAP_CREATE_MODEL_THREADS` | Defaults to `ASKAP_CPU_PER_SLOT`; DStools model threads. |
| DStools create-model | `ASKAP_CREATE_MODEL_ITERATIONS` | `100000`, passed as `-N 100000`. |
| DStools create-model | `ASKAP_MIN_UV_METRES` | `200`, passed as `--minuvw-m 200`. |
| DStools create-model | `ASKAP_CREATE_MODEL_MULTISCALE` | Enabled by default, passed as `-S`. |
| Controller | `ASKAP_CONTROLLER_INITIAL_DELAY_SECONDS`, `ASKAP_CONTROLLER_POLL_INTERVAL_SECONDS` | `30` seconds and `60` seconds; filesystem polling timing only. CLI equivalents are available. |
| Resubmission | `ASKAP_MAX_AUTO_RESUBMITS` | `20` automatic self-resubmissions after partial worker exit. |
| Python/modules | `ASKAP_PYTHON_BIN`, `ASKAP_PYTHON_PARENT_MODULE`, `ASKAP_PYTHON_MODULE` | Host interpreter and module names used by generated workers. |
| Container | `DSTOOLS_CONTAINER`, `APPTAINER_BIN`, `ASKAP_APPTAINER_BIND` | DStools image, Apptainer executable, and bind mapping. Workers use `--cleanenv --no-home` so host module libraries cannot override the image. |

The following processing controls are also environment-overridable when a
scientific change is intentional: `ASKAP_DSTOOLS_BAND`,
`ASKAP_CREATE_MODEL_ITERATIONS`, `ASKAP_MIN_UV_METRES`,
`ASKAP_CREATE_MODEL_MULTISCALE`,
`ASKAP_EXTRACT_MIN_UV_METRES`, `ASKAP_CROP_ARCMIN`,
`ASKAP_SPTNUM_THRESHOLD`, `ASKAP_DEFAULT_T_MIN`, and `ASKAP_MJD_J2000`.
The fallback `ASKAP_DEFAULT_T_MIN` is MJD `61041.5` (2026-01-01 12:00 UTC).
`ASKAP_SAFETY_FACTOR`, `ASKAP_TARGET_OBS`, `ASKAP_MAX_SOURCES_PER_JOB`, and
`ASKAP_SCAN_WINDOW` remain in `config.py` for compatibility with earlier
packing settings; the current controller uses the fixed 50-UCS limit and live
staging/free-space checks instead of those legacy knobs.

Credentials and connection environment:

| Purpose | Settings |
|---|---|
| CASDA login | `CASDA_USERNAME`, `CASDA_PASSWORD_FILE`, one-process `CASDA_PASSWORD` |
| Product pull initiated on ada | `OZSTAR_SSH_USER`, `OZSTAR_SSH_HOST`, `OZSTAR_PRODUCT_ROOT`, `OZSTAR_PASSWORD_FILE` |
| Required ada storage root | `/import/ada1/qhua0119/Combine_ImagePlot/Ozstar/` |
| Transfer password/tool | `TRANSFER_PASSWORD_FILE`, one-process `TRANSFER_PASSWORD`, `SSHPASS_BIN`, `ASKAP_SSH_CONNECT_TIMEOUT` |

Use protected files by default. Environment passwords are deliberately
short-lived overrides and must not be put in shell history, README files,
manifests, or logs. The wrappers do not enable automatic transfer.

### 中文

controller 在 Ozstar 登录节点运行。范围两端都包含；`--submit` 已经包含登录节点
准备步骤，正常生产不需要另行执行 prepare 命令：

```bash
cd /fred/oz299/qhuang/ASKAP-UCDs/ozstar_askap
module load gcc/13.3.0
module load python/3.12.3
export ASKAP_PYTHON_PARENT_MODULE=gcc/13.3.0
export ASKAP_PYTHON_MODULE=python/3.12.3
export ASKAP_PYTHON_BIN=/fred/oz299/qhuang/ASKAP-UCDs/.venv/askap-python/bin/python
export ASKAP_SBATCH_MEM=80G
$ASKAP_PYTHON_BIN ozstar_main.py --begin 1501 --end 1901 --submit
```

正常顺序如下：

1. 读取 catalogue，并在每个新源周围 1 度内查询 CASDA TAP。把每行 ObsCore
   `em_min`/`em_max` 解释为以米表示的波长边界，以 `c/em_min` 得到最高观测频率，
   再把 ASKAP 在 1.4 GHz 下的 1.2 度 FWHM 按 `1/频率` 缩放；只有目标位于该最高
   频率的 `FWHM/2` 半径内时才保留该行。继续应用原有的质量、光谱型、曝光、大小、
   release 和最近 `obs_id` 筛选；缺失或非法的频谱边界会被排除。
2. 把完整的过滤和去重后的 TAP 行写入 `state/tap_cache/*.ecsv`。TAP 返回的每一列
   都会保留，包括 `access_estsize` 以及返回时存在的 `t_min`/`t_max`。
3. 对去重后的 `access_estsize` 求和。CASDA 的单位是 KB，因此会转换为 bytes。每个
   controller block 最多包含 50 个 UCS 编号，并在估计值即将超过 2 TB staging 上限
   （默认 `2 * 1024**4` bytes）前拆分。单个源超过上限会明确失败。下载前还会检查
   当前 staging 占用和 `ASKAP_WORK` 的实际剩余空间。
4. 登录节点向 CASDA 登录；每个 visibility tar 压缩包（`.tar`、`.tar.gz`、`.tgz`）
   与对应的 `.checksum` 在同一次 CASDA 下载调用中下载。登录节点**不解压**压缩包。
5. 为准备好的 block 提交短名称 Slurm worker。计算节点只解压当前源的压缩包，运行
   DStools、裁剪 FITS、复制经过文件名检查的产物并写入 checkpoint。
6. 登录节点 controller 轮询源状态和文件系统产物。每个源达到 `complete`、
   `complete_with_failures`、`no_data` 或 `skipped` 后才会推进；`complete` 必须有
   预期 `.ds`、Stokes-I/V FITS 文件名，且
   staging 中没有未处理的 MS 或 archive 残留；程序不读取这些产品内容。
7. worker 因墙钟保护返回专用 partial exit code 时，生成的 sbatch 会自动重新提交自己，
   默认最多 `ASKAP_MAX_AUTO_RESUBMITS=20` 次。不需要人工根据队列判断完成。

只生成 TAP cache 和 manifest、不下载也不提交时使用 `--no-submit`。`--prepare-download`
保留给显式的登录节点准备-only 运行。`--retry-existing` 会有意重置不完整 controller
状态，但只重试 SQLite 表中记录为失败、且仍有重试机会的 SB；已经完成、no-data 或
skipped 的源不会重跑。第一次显式重试会删除下游中间产物，用 FixMS 的备份表/列把
MS 恢复到 preprocess 前的状态，然后从 ASKAP preprocess 重新开始。如果这次处理
仍失败，同一个 controller 会在登录节点删除该 SB 的 MS、archive、checksum 和源级
下载完成标记，从 CASDA 重新下载一次，并执行最后一次 preprocess-to-product 尝试。
重新下载后的处理仍失败时，状态变为 `exhausted`，以后即使再次使用
`--retry-existing` 也不会选中。没有该选项的正常流程绝不会重试历史失败。普通断线
重连不要使用 `--retry-existing`。
`--allow-compute-download` 只用于调试，不属于正常的
Ozstar 网络架构。

对一个包含端点的范围显式重试历史失败：

```bash
$ASKAP_PYTHON_BIN ozstar_main.py --begin 1501 --end 1550 \
  --retry-existing --submit
```

每次进程启动时读取以下可修改设置。默认值是当前运行默认值，不是密码或其他 secret：

| 类别 | 设置 | 默认值和含义 |
|---|---|---|
| 范围 | `--begin`、`--end` | 包含端点的 UCS 范围，推荐的单次运行控制项。 |
| 范围 | `ASKAP_UCS_START`、`ASKAP_UCS_END` | 未提供 CLI 范围时使用，默认 `1`、`1000`。 |
| 持久化路径 | `ASKAP_WORK` | `/fred/oz299/qhuang/ASKAP-UCDs`，包含 staging、products、state、logs。 |
| 输入路径 | `ASKAP_CATALOGUE`、`ASKAP_TIME_CSV` | 覆盖 catalogue 和旧 `obs_id` 到 `t_min` CSV 的路径。 |
| 内存 | `ASKAP_SBATCH_MEM` | `80G`，Slurm worker 内存；需要时每次运行前调整。 |
| scratch | `ASKAP_SBATCH_TMP` | `10G`，仅是小型 Slurm scratch，不存放 MS/产物。 |
| 分区 | `ASKAP_SBATCH_PARTITION` | 默认空；需要时设置站点批准的 Slurm partition。 |
| block/storage | `BLOCK_SIZE` | 固定的每 block 最多 50 个 UCS 编号，不能作为运行时覆盖值。 |
| block/storage | `ASKAP_STAGING_BUDGET_BYTES` | `2 * 1024**4`，估计 staging 准入上限。 |
| CASDA 筛选 | `ASKAP_CASDA_MAX_FILE_SIZE_KB` | 每行 archive 估计大小的过滤上限。 |
| CASDA 波束筛选 | `ASKAP_CASDA_INITIAL_QUERY_RADIUS_DEG`、`ASKAP_CASDA_REFERENCE_FWHM_DEG`、`ASKAP_CASDA_REFERENCE_FREQUENCY_GHZ` | 初始 TAP 半径 `1.0` 度；ASKAP FWHM 归一化为 1.4 GHz 下 `1.2` 度；最终径向截止值为最高频率的 `FWHM/2`。修改任一参数都会自动使旧 TAP cache 失效。 |
| CASDA 批量 | `ASKAP_CASDA_STAGE_BATCH_SIZE`、`ASKAP_CASDA_QUERY_LIMIT` | CASDA staging 批量 `20`、TAP 行数上限 `5000`；一旦返回行数达到上限会报错，而不是静默使用可能被截断的结果。 |
| 下载重试 | `ASKAP_CASDA_SOURCE_DOWNLOAD_RETRIES`、`ASKAP_CASDA_SOURCE_RETRY_DELAY_SECONDS` | 每个源默认重试 `3` 次、间隔 `30` 秒；仍失败则记录 `download_failed` 并继续下一个源。 |
| 时间估计 | `ASKAP_HOURS_PER_OBS` | 每个 MS observation wave `2.0` 小时，用于 worker wave 判断。 |
| 时间估计 | `ASKAP_SOURCE_OVERHEAD_HOURS` | 每个 source chunk `2.0` 小时，用于解压、裁剪和清理。 |
| Slurm 时间 | `ASKAP_MAX_MS_PER_JOB`、`ASKAP_MAX_JOB_WALLTIME_HOURS` | 默认 `40` MS、`23` 小时；下载后由 archive 成员名确定 `N`，每次申请为 `ceil(min(N,40)/4) × 2 h + 2 h`，保持低于 24 小时。 |
| 墙钟安全 | `ASKAP_WALLTIME_RESERVE_MINUTES` | partial 退出前保留 `20` 分钟。 |
| CPU | `ASKAP_PARALLEL_SLOTS` | `4` 个并发 SB wave。 |
| CPU | `ASKAP_CPU_PER_SLOT` | 每个 DStools 进程 `8` 线程；默认总计 `4 x 8 = 32` CPU，不能超过 32。 |
| CPU | `ASKAP_CREATE_MODEL_THREADS` | 默认等于 `ASKAP_CPU_PER_SLOT`，DStools model 线程数。 |
| DStools create-model | `ASKAP_CREATE_MODEL_ITERATIONS` | 默认 `100000`，对应 `-N 100000`。 |
| DStools create-model | `ASKAP_MIN_UV_METRES` | 默认 `200`，对应 `--minuvw-m 200`。 |
| DStools create-model | `ASKAP_CREATE_MODEL_MULTISCALE` | 默认启用，对应 `-S`。 |
| controller | `ASKAP_CONTROLLER_INITIAL_DELAY_SECONDS`、`ASKAP_CONTROLLER_POLL_INTERVAL_SECONDS` | `30` 秒和 `60` 秒，只影响文件系统轮询；也有 CLI 选项。 |
| 自动重提交 | `ASKAP_MAX_AUTO_RESUBMITS` | partial worker 后自动 self-resubmit 的次数，默认 `20`。 |
| Python/module | `ASKAP_PYTHON_BIN`、`ASKAP_PYTHON_PARENT_MODULE`、`ASKAP_PYTHON_MODULE` | 生成 worker 使用的主机解释器和 module。 |
| 容器 | `DSTOOLS_CONTAINER`、`APPTAINER_BIN`、`ASKAP_APPTAINER_BIND` | DStools 镜像、Apptainer 可执行文件和 bind 映射。worker 使用 `--cleanenv --no-home`，避免宿主 module 库覆盖镜像。 |

如果确实需要科学参数变化，也可以设置 `ASKAP_DSTOOLS_BAND`、
`ASKAP_CREATE_MODEL_ITERATIONS`、`ASKAP_MIN_UV_METRES`、
`ASKAP_EXTRACT_MIN_UV_METRES`、`ASKAP_CROP_ARCMIN`、
`ASKAP_SPTNUM_THRESHOLD`、`ASKAP_DEFAULT_T_MIN` 和 `ASKAP_MJD_J2000`。
fallback `ASKAP_DEFAULT_T_MIN` 是 MJD `61041.5`（2026-01-01 12:00 UTC）。
`ASKAP_SAFETY_FACTOR`、`ASKAP_TARGET_OBS`、`ASKAP_MAX_SOURCES_PER_JOB` 和
`ASKAP_SCAN_WINDOW` 是为兼容旧 packing 设置而保留的；当前 controller 使用固定的
50-UCS 上限和实时 staging/剩余空间检查，不使用这些旧参数改变 block。

凭据和连接环境：

| 用途 | 设置 |
|---|---|
| CASDA 登录 | `CASDA_USERNAME`、`CASDA_PASSWORD_FILE`、当前进程的 `CASDA_PASSWORD` |
| 在 ada 发起的产物拉取 | `OZSTAR_SSH_USER`、`OZSTAR_SSH_HOST`、`OZSTAR_PRODUCT_ROOT`、`OZSTAR_PASSWORD_FILE` |
| ada 统一存储根目录 | `/import/ada1/qhua0119/Combine_ImagePlot/Ozstar/` |
| 传输密码/工具 | `TRANSFER_PASSWORD_FILE`、当前进程的 `TRANSFER_PASSWORD`、`SSHPASS_BIN`、`ASKAP_SSH_CONNECT_TIMEOUT` |

默认使用受保护的密码文件。环境密码只应作为短期覆盖值，不能放入 shell history、
README、manifest 或日志。wrapper 不会自动传输。

## 3. Post-Run Checks and Transfer to ada / 运行后检查和传输到 ada

### English

Before transferring, confirm the controller JSON at
`state/controllers/UCS<begin>-<end>.json` is `complete`. Check source state
files in `state/sources/` and inspect `logs/` and the generated Slurm output for
failures. A block is not complete merely because a Slurm job exited: every
source must be terminal, and every complete source must pass product and
staging-residue validation.

Per-SB download outcomes are stored separately in
`state/sources/UCSXXXX.downloads.json`. Each entry identifies the source and
SB key, latest status (`downloaded`, `download_failed`, or checksum status),
attempt count, redacted URL, and retry history.

The authoritative processing table is `state/sb_processing.sqlite`. It has one
row per UCS/SB/beam and `Y`/`N`/empty columns for CASDA query, download,
extraction, preprocessing, each DStools step, FITS crops, product copy, and
cleanup. Successful rows retain elapsed time only. For a failed SB, its full
accumulated DStools command/output log is retained at `logs/failures/`; the
table stores the failed step, error, log path, `retry_stage`, and the two retry
counters. A failed source therefore
does not stop later sources; `complete_with_failures` means to inspect this
table before selectively retrying that source. Staging is deleted only after
the DS plus both Stokes-I and Stokes-V images have reached the persistent
product tree, or when an explicit bounded retry deliberately discards one
failed SB before its fresh CASDA download. Initial and final failed attempts
otherwise retain their staging inputs for diagnosis.

The ada-compatible product tree is:

```text
products/UCS1501-1550/UCSXXXX/LongObs/SBXXXXX_beamYY/
    SBXXXXX_beamYY.ds
    wsclean_model/
        wsclean-MFS-I-image.fits
        wsclean-MFS-V-image.fits
```

Ozstar cannot open an SSH connection to ada, while ada can connect to Ozstar.
The transfer is therefore always explicit and is initiated on an **ada login
node**. Copy `pull_askap_products.py` to
`/import/ada1/qhua0119/Combine_ImagePlot/Ozstar/bin/`, then run:

```bash
cd /import/ada1/qhua0119/Combine_ImagePlot/Ozstar/bin
python3 pull_askap_products.py --all --dry-run
python3 pull_askap_products.py --all
```

`--all` discovers every complete observation below the Ozstar product root and
transfers only the exact `.ds`, MFS Stokes-I, and MFS Stokes-V triple. A single
batch can instead be selected with `--batch UCS1501-1550`. The default `rsync`
mode is incremental and restartable. The program freezes the remote
path/size/SHA-256 inventory, verifies all three ada files, rereads the Ozstar
inventory to detect source changes, and only then records the observation as
`complete`.

Remote inventory generation deliberately uses Ozstar's `/usr/bin/python3` and
the standard library only. Do not point it at the project virtualenv: a direct
non-interactive SSH command does not load the GCC/Python modules required by
that virtualenv's `libpython3.12.so.1.0`.

The authoritative ada ledger is
`/import/ada1/qhua0119/Combine_ImagePlot/Ozstar/state/transfer_ledger.sqlite`.
Each row records DS, I, and V separately as `Y` or `N`; `overall_status` can be
`complete` only when all three are `Y`. Immutable JSON receipts are retained
under the same ada root and copied back to
`/fred/oz299/qhuang/ASKAP-UCDs/state/transfer_receipts/`. Receipts support a
later explicit cleanup review, but this transfer program never deletes Ozstar
or ada products.

Authentication may use the normal interactive SSH prompt, an SSH key, the
mode-600 file
`/import/ada1/qhua0119/Combine_ImagePlot/Ozstar/state/ozstar-transfer-password`,
or the one-process `TRANSFER_PASSWORD`. Password values are not embedded in
source, arguments, manifests, receipts, or output. `transfer_to_ada.sh` now
exits with an explanation so an accidental Ozstar-side push cannot hang.

### 中文

传输前确认 `state/controllers/UCS<begin>-<end>.json` 的状态为 `complete`。检查
`state/sources/` 中的源状态文件，并查看 `logs/` 和生成的 Slurm 输出是否有失败。
不能仅凭 Slurm job 退出就认为 block 完成：每个源都必须处于终态，而且 `complete`
源必须通过产物和 staging 残留检查。

每个源的 SB 下载结果单独保存在
`state/sources/UCSXXXX.downloads.json`。每条记录包含源名和 SB 键、最新状态
（`downloaded`、`download_failed` 或 checksum 状态）、尝试次数、去敏后的 URL 和重试历史。

权威处理表为 `state/sb_processing.sqlite`。只有 `.ds`、Stokes-I 和 Stokes-V
都复制到持久化产物树后，程序才会删除对应 SB 的 staging 数据。
表中还保存 `retry_stage`、原 MS 处理重试次数和重新下载重试次数；最终失败是
`exhausted`。
除非显式 bounded retry 正在删除某个失败 SB 以强制 CASDA 重新下载，否则第一次和
最终失败的 staging 输入都会保留，以便诊断。

ada-compatible 产物目录如下：

```text
products/UCS1501-1550/UCSXXXX/LongObs/SBXXXXX_beamYY/
    SBXXXXX_beamYY.ds
    wsclean_model/
        wsclean-MFS-I-image.fits
        wsclean-MFS-V-image.fits
```

由于 Ozstar 不能主动连接 ada 的 SSH，而 ada 可以连接 Ozstar，传输必须在 **ada
登录节点**显式发起。把 `pull_askap_products.py` 放到
`/import/ada1/qhua0119/Combine_ImagePlot/Ozstar/bin/` 后执行：

```bash
cd /import/ada1/qhua0119/Combine_ImagePlot/Ozstar/bin
python3 pull_askap_products.py --all --dry-run
python3 pull_askap_products.py --all
```

`--all` 自动发现 Ozstar 上所有完整观测，只传输精确的 `.ds`、MFS Stokes-I 和
MFS Stokes-V 三件套；也可以使用 `--batch UCS1501-1550` 只处理一个 batch。默认
`rsync` 支持增量与中断续传。程序先冻结远端路径/大小/SHA-256 清单，在 ada 校验三个
文件，再重新读取 Ozstar 清单以排除传输期间的源变化，全部相同后才把该观测记为
`complete`。

远端清单固定使用 Ozstar 的 `/usr/bin/python3`，而且只依赖标准库。不要把它改成项目
虚拟环境 Python：非交互 SSH 不会加载该虚拟环境所需的 GCC/Python module，因此无法
找到 `libpython3.12.so.1.0`。

ada 权威台账位于
`/import/ada1/qhua0119/Combine_ImagePlot/Ozstar/state/transfer_ledger.sqlite`。
每行分别记录 DS/I/V 的 `Y` 或 `N`，只有三项都是 `Y` 时 `overall_status` 才能成为
`complete`。不可变 JSON 回执保存在同一 ada 根目录下，并回传至 Ozstar 的
`/fred/oz299/qhuang/ASKAP-UCDs/state/transfer_receipts/`。回执用于之后的显式空间清理
审核；传输程序本身绝不删除 Ozstar 或 ada 的产物。

认证可以使用交互式 SSH、SSH key、mode-600 文件
`/import/ada1/qhua0119/Combine_ImagePlot/Ozstar/state/ozstar-transfer-password`，或当前
进程的 `TRANSFER_PASSWORD`。密码不会写入源码、参数、manifest、回执或输出。
`transfer_to_ada.sh` 现在会直接说明网络方向并退出，防止误用 Ozstar 推送造成挂起。

## 4. Architecture and Runtime Summary / 架构和运行时总结

### English

```text
Ozstar login node
  catalogue -> CASDA TAP filter/deduplicate -> full-row ECSV cache
  CASDA login -> download tar/tar.gz/tgz + .checksum only
  filesystem-state controller -> prepare one block -> submit/poll
                                |
                                v
Ozstar compute node
  extract only the current source's archives -> DStools -> crop FITS
  named-product checks/copy -> state checkpoints -> partial self-resubmit if needed
                                |
                                v
Ozstar persistent products <- ada-initiated SHA-256-verified pull <- ada
```

The confirmed runtime boundaries are:

- CASDA TAP, CASDA login, and archive download run on the login node.
- The login node never extracts a CASDA archive.
- A compute worker extracts only the current source's already-downloaded
  archives and runs DStools. Compute-node CASDA download is disabled by default
  and exists only behind `--allow-compute-download` for debugging.
- A block has at most 50 UCS numbers. Its 2 TB staging admission estimate is
  the sum of filtered, release-checked, nearest-row-deduplicated CASDA
  `access_estsize` values, interpreted as KB and converted to bytes. Existing
  staging usage and actual persistent free space are also enforced.
- Full filtered TAP rows are preserved in ECSV. If CASDA returns `t_min` or
  `t_max`, those columns remain available to the worker.
- Proper-motion `t_min` precedence is CASDA ECSV first, legacy `obs_id -> t_min`
  CSV second, and fallback MJD `61041.5` third.
- The controller is filesystem-state driven. It does not use `squeue` or
  `sacct` as a completion source. A partial worker writes checkpoints and the
  generated sbatch script automatically resubmits itself within the configured
  limit.
- The current resource model is two hours per MS observation, four concurrent
  slots of eight CPU threads (`4 x 8`), 80G Slurm memory, at most 40 MS and a
  23-hour request per source chunk, a 20-minute worker reserve, and a compact
  Slurm job name.
- Transfer is explicitly initiated on ada, password-safe, and verified.
  Processing never starts an automatic transfer and transfer never deletes.
- Shell setup loads modules but performs no `module purge` and no
  `module unload`.

Persistent state and products are organized as follows:

```text
${ASKAP_WORK}/
  catalogue/                  input CSV files
  staging/                    archives and current-source MS trees
  products/                   ada-compatible DS/FITS products
  state/controllers/          range-level controller state
  state/jobs/                 block manifests and generated sbatch scripts
  state/sources/               per-source state and checkpoints
  state/tap_cache/             full filtered ECSV rows
  logs/                       worker, SB, and transfer-related logs
```

The program files are intentionally small roles: `ozstar_main.py` controls
planning/submission/polling; `casda_query.py` performs TAP filtering, keyring
login, and archive downloads; `prepare_download.py` performs login-node
preparation; `staging.py` performs basic tar extraction on compute nodes;
`process_job.py` runs DStools and checkpoints; `sb_status.py` owns the durable
per-SB retry state machine; `reset_askap_ms.py` restores FixMS backups before
the one existing-MS retry; `pipeline_utils.py` validates state/products;
`crop_fits.py` preserves useful celestial WCS while cropping;
`pull_askap_products.py` performs the supported ada-initiated verified pull;
and `transfer_products.py` remains only for legacy/reverse maintenance.

### 中文

```text
Ozstar 登录节点
  catalogue -> CASDA TAP 筛选/去重 -> 完整行 ECSV cache
  CASDA 登录 -> 只下载 tar/tar.gz/tgz + .checksum
  文件系统状态 controller -> 准备一个 block -> 提交/轮询
                              |
                              v
Ozstar 计算节点
  只解压当前源的 archive -> DStools -> FITS 裁剪
  具名产物检查/复制 -> 状态 checkpoint -> 必要时 partial 自动 self-resubmit
                              |
                              v
Ozstar 持久化产物 <- ada 发起的 SHA-256 校验拉取 <- ada
```

已确认的运行边界如下：

- CASDA TAP、CASDA 登录和 archive 下载在登录节点执行。
- 登录节点不解压 CASDA archive。
- 计算 worker 只解压当前源已经下载的 archive，并运行 DStools。默认禁止计算节点
  下载 CASDA；只有调试选项 `--allow-compute-download` 才会启用该路径。
- 每个 block 最多 50 个 UCS 编号。2 TB staging 准入估计是经过筛选、release 检查、
  按最近行去重后的 CASDA `access_estsize` 之和，单位按 KB 解释后转换成 bytes；同时
  强制检查已有 staging 占用和持久化文件系统实际剩余空间。
- 过滤后的 TAP 完整行保存为 ECSV。如果 CASDA 返回 `t_min` 或 `t_max`，这些列会保留
  并提供给 worker。
- 自行修正的 `t_min` 优先级是 CASDA ECSV，其次是旧的 `obs_id -> t_min` CSV，最后是
  fallback MJD `61041.5`。
- controller 由文件系统状态驱动，不用 `squeue` 或 `sacct` 作为完成依据。partial worker
  写入 checkpoint，生成的 sbatch 会在配置次数内自动 self-resubmit。
- 当前资源模型是每个 MS observation 2 小时、4 个并发 slot、每 slot 8 个 CPU 线程
  （`4 x 8`）、80G Slurm 内存、默认每个 source chunk 最多 40 个 MS、23 小时 walltime、20 分钟 worker reserve，
  以及短 Slurm 名称。
- 传输必须在 ada 显式发起、密码安全并校验。处理命令不会自动传输，传输程序也不会
  自动删除任一侧产物。
- shell setup 只加载 module，不执行 `module purge` 或 `module unload`。

持久化状态和产物目录如下：

```text
${ASKAP_WORK}/
  catalogue/                  输入 CSV
  staging/                    archive 和当前源 MS 树
  products/                   ada-compatible DS/FITS 产物
  state/controllers/          范围级 controller 状态
  state/jobs/                 block manifest 和生成的 sbatch
  state/sources/              每源状态和 checkpoint
  state/tap_cache/            完整过滤后 ECSV 行
  logs/                       worker、SB 和传输相关日志
```

程序文件职责保持单一：`ozstar_main.py` 负责规划、提交和轮询；`casda_query.py` 负责
TAP 筛选、keyring 登录和 archive 下载；`prepare_download.py` 负责登录节点准备；
`staging.py` 负责计算节点基础 tar 解压；`process_job.py` 运行 DStools 并写 checkpoint；
`sb_status.py` 管理持久化的逐 SB 重试状态机；`reset_askap_ms.py` 在唯一一次原 MS
重试前恢复 FixMS 备份；`pipeline_utils.py` 验证状态和产物；`crop_fits.py` 保留有效
天球 WCS 并裁剪；`pull_askap_products.py` 执行受支持的 ada 主动校验拉取；
`transfer_products.py` 只保留用于旧式/反向维护。

## Diagnostic: Legacy CASDA Download Comparison / 诊断：旧 CASDA 下载路径对比

`casda_remote_compare.py` is not part of production processing. It reproduces
the legacy `Download_ASKAP_Visibility_Remote.py` path with the same target
coordinates, `t_exptime > 100` query, release filter, `stage_data`, and
`Casda.download_files`. Run it in the legacy/local `astro` environment when a
Pawsey 403 needs comparison:

```bash
python casda_remote_compare.py --limit 1
python casda_remote_compare.py --obs-id 50508 --limit 1
python casda_remote_compare.py --obs-id 64751 --filename-contains VAST_1147+06 --limit 1
```

Use `--limit 40` only for an intentional full legacy-batch test. Never publish
the complete staged URLs because they contain temporary access signatures.

`casda_remote_compare.py` 不是生产处理程序。它使用与旧
`Download_ASKAP_Visibility_Remote.py` 相同的坐标、`t_exptime > 100` 查询、release
filter、`stage_data` 和 `Casda.download_files`，用于对比 Pawsey 403：

```bash
python casda_remote_compare.py --limit 1
python casda_remote_compare.py --obs-id 50508 --limit 1
```

只有在确实需要整批旧流程测试时才使用 `--limit 40`。不要公开完整 staged URL，
因为其中包含临时访问签名。

## Single-source validation / 单源处理验证

Use the backup program for a short end-to-end validation without touching the
original `ozstar_askap` directory. The validation prepares one source on the
login node, writes a dedicated log, and submits a two-hour worker with
8 CPUs (`1 x 8` DStools slot). It does not enable compute-node CASDA access.

在 backup 程序目录中运行以下命令可以验证一个源的完整流程，不会修改原始
`ozstar_askap` 目录。程序会在登录节点准备一个源，写入独立日志，并提交一个
2 小时、8 CPU（`1 x 8` DStools slot）的 worker；不会开启计算节点 CASDA 下载。

```bash
cd /fred/oz299/qhuang/ASKAP-UCDs/ozstar_askap_backup
module load gcc/13.3.0
module load python/3.12.3
export ASKAP_PYTHON_PARENT_MODULE=gcc/13.3.0
export ASKAP_PYTHON_MODULE=python/3.12.3
export ASKAP_PYTHON_BIN=/fred/oz299/qhuang/ASKAP-UCDs/.venv/askap-python/bin/python

tmux new -s askap-validation
$ASKAP_PYTHON_BIN single_source_validation.py --ucs 1514 --submit
```

The program prints the validation log, manifest, sbatch path, and job ID. Check:

```bash
state/jobs/validation-*.json
logs/validation-*.log
logs/slurm-*.out
logs/slurm-*.err
products/UCS*/UCS1514/LongObs/
state/sources/UCS1514.json
```

程序会输出验证日志、manifest、sbatch 路径和 job ID。重点检查：登录节点准备是否
成功、计算节点是否能启动、`dstools-create-model` 是否使用预期参数、是否生成
`.ds` 和 FITS，以及 source state 是否正确更新。

The validation defaults to zero automatic resubmits so a two-hour test cannot loop
indefinitely. Use `--max-resubmits 1` only when testing the partial self-resubmit
mechanism itself.

验证程序默认不自动续提交，避免 2 小时测试无限循环。只有在专门测试 partial
self-resubmit 时才使用 `--max-resubmits 1`。

For a one-row comparison using the deployed production venv, production
keyring adapter, current `UCS1501.ecsv`, `build_casda_client()`, `stage_data()`,
and one paired archive/checksum `download_files()` call, run on Ozstar.  The
diagnostic also rejects unquoted staged URLs so the Astroquery 0.4.11
pre-signed-URL regression is detected before downloading:

```bash
$ASKAP_PYTHON_BIN casda_production_compare.py \
  --source UCS1501 \
  --obs-id 63406
```

The diagnostic writes only below `WORK/diagnostics/`, not to production
`staging/` or `products/`. Do not publish complete signed URLs printed by
lower-level Astroquery progress output.

如果需要使用部署后的生产 venv、生产 keyring 适配器、当前
`UCS1501.ecsv`、`build_casda_client()`、`stage_data()` 和一次成对提交 archive 与
checksum URL 的 `download_files()` 做一对一对照，可以在 Ozstar 运行。诊断程序也会
在下载前拒绝未正确编码的 staged URL，从而检查 Astroquery 0.4.11 的预签名 URL
问题：

```bash
$ASKAP_PYTHON_BIN casda_production_compare.py \
  --source UCS1501 \
  --obs-id 63406
```

诊断文件只写入 `WORK/diagnostics/`，不会写入生产 `staging/` 或 `products/`。
不要公开底层 Astroquery 输出中的完整预签名 URL。

## Failure report and Gate 6 retry validation / 失败报告与 Gate 6 重试验收

`show_failures.py` opens `state/sb_processing.sqlite` read-only. Its compact
form prints one unresolved observation per line as
`UCSxxxx_SBxxxx —— failed_step`; it never changes retry state. Use `--details`
for the beam, bounded-retry stage/counters, error, and retained failure log,
or `--json` for machine-readable output:

```bash
$ASKAP_PYTHON_BIN show_failures.py
$ASKAP_PYTHON_BIN show_failures.py --retryable-only --details
$ASKAP_PYTHON_BIN show_failures.py --source UCS1514 --json
```

`gate6_retry_validation.py` is a login-node end-to-end test of the bounded
retry state machine. It copies exactly one row from an existing production TAP
cache into a new isolated work tree, downloads that one real observation, and
uses normal `sbatch` submissions. A guarded Gate-6-only hook injects a
create-model failure in the initial run and the raw-MS retry. The script then
verifies that a normal rerun does nothing, an explicit retry resets and starts
again at preprocess, the second failure deletes that isolated MS/checksum and
causes one fresh CASDA download, and the final run produces DS plus Stokes-I/V.
The workers are fixed at 2 hours, 8 CPUs, 32G RAM, and 10G tmp. Run the driver
inside `tmux`; it may wait for three Slurm jobs:

```bash
cd /fred/oz299/qhuang/ASKAP-UCDs/ozstar_askap
module load gcc/13.3.0
module load python/3.12.3
export ASKAP_PYTHON_BIN=/fred/oz299/qhuang/ASKAP-UCDs/.venv/askap-python/bin/python

tmux new -s askap-gate6
$ASKAP_PYTHON_BIN gate6_retry_validation.py \
  --ucs 1514 --obs-id 35429 --submit
```

The default output tree is below
`/fred/oz299/qhuang/dstools/gate6-validation/`; the safety checks reject a
Gate 6 work directory inside `/fred/oz299/qhuang/ASKAP-UCDs`. On success the
driver writes `state/gate6-result.json` with `status: PASS` and leaves all
validation logs/products for inspection. The fault environment variable is
only embedded in Gate 6 sbatch files and ordinary production jobs remain
unaffected.

`show_failures.py` 只读打开 `state/sb_processing.sqlite`。默认每行显示一项未解决
失败，格式为 `UCSxxxx_SBxxxx —— 失败步骤`，不会改变重试状态。`--details` 会同时
显示 beam、bounded retry 阶段与次数、错误和保留的失败日志；`--json` 提供机器可读
结果。

`gate6_retry_validation.py` 在登录节点验证完整重试状态机。它只把生产 TAP cache 中
指定的一行复制到全新的隔离工作区，并通过正常 `sbatch` 处理这个真实观测。仅 Gate 6
可用、带多重路径与源/SB 限制的钩子会在首次处理和原 MS 重试的 create-model 前制造
失败。程序随后确认：不带 `--retry-existing` 的普通重跑不会提交任务；显式重试会重置
MS 并从 preprocess 开始；再次失败后会删除隔离区内的 MS/checksum、重新从 CASDA 下载
一次；最后一次处理会生成 DS 与 Stokes-I/V。每个 worker 固定申请 2 小时、8 CPU、
32G 内存和 10G tmp。建议在 `tmux` 中执行上述命令，因为程序需要等待三个 Slurm
作业。默认测试目录位于 `/fred/oz299/qhuang/dstools/gate6-validation/`，安全检查拒绝
把 Gate 6 工作区放入生产 `/fred/oz299/qhuang/ASKAP-UCDs`。通过后结果写入隔离目录的
`state/gate6-result.json`，测试日志与产物会保留供检查；普通生产 sbatch 不会包含
Gate 6 故障变量。

## Gate 7 ada transfer validation / Gate 7 ada 传输验收

Run Gate 7 only after Gate 6 reports `PASS`. Gate 7 now runs on an **ada login
node** and pulls the real DS and Stokes-I/V triple from the isolated Gate 6
tree on Ozstar. It exercises the same source snapshot, rsync/scp, SHA-256,
SQLite-ledger, and receipt-upload code used by production. Every ada file is
stored below `/import/ada1/qhua0119/Combine_ImagePlot/Ozstar/`; the driver
refuses an existing validation directory and never deletes either copy.

```bash
ADA_ROOT=/import/ada1/qhua0119/Combine_ImagePlot/Ozstar
mkdir -p "$ADA_ROOT/bin" "$ADA_ROOT/state"

scp qhuang@ozstar.swin.edu.au:/fred/oz299/qhuang/ASKAP-UCDs/ozstar_askap/pull_askap_products.py \
  "$ADA_ROOT/bin/"
scp qhuang@ozstar.swin.edu.au:/fred/oz299/qhuang/ASKAP-UCDs/ozstar_askap/gate7_transfer_validation.py \
  "$ADA_ROOT/bin/"

# Only needed when using sshpass instead of an SSH key or interactive prompt.
test ! -f "$ADA_ROOT/state/ozstar-transfer-password" || \
  chmod 600 "$ADA_ROOT/state/ozstar-transfer-password"

cd "$ADA_ROOT/bin"
python3 gate7_transfer_validation.py \
  --gate6-work /fred/oz299/qhuang/dstools/gate6-validation/gate6-UCS1514-SB35429-20260928T042619Z \
  --submit
```

The default result tree is
`/import/ada1/qhua0119/Combine_ImagePlot/Ozstar/gate7-validation/`. A passing
run writes `state/gate7-result.json`, an isolated `transfer_ledger.sqlite`, the
source manifest, and a local receipt; the receipt is also copied to Ozstar's
`state/transfer_receipts/gate7/`. It must contain exactly one observation with
`DS=Y`, `I=Y`, `V=Y`, and `overall_status=complete`. Gate 7 does not use Slurm.

Gate 7 只能在 Gate 6 返回 `PASS` 后运行。现在它在 **ada 登录节点**主动拉取 Gate 6
隔离产物树中的真实 DS 与 Stokes-I/V 三件套，并完整使用生产传输相同的源清单、
rsync/scp、SHA-256、SQLite 台账和回执回传逻辑。所有 ada 文件均位于
`/import/ada1/qhua0119/Combine_ImagePlot/Ozstar/` 下；程序拒绝已有的验证目录，且不会
删除任一侧文件。默认结果位于该根目录的 `gate7-validation/`。通过时会生成
`state/gate7-result.json`、隔离 SQLite 台账、源 manifest 与本地回执，并把回执复制
到 Ozstar 的 `state/transfer_receipts/gate7/`。台账必须精确包含一个观测，状态为
`DS=Y`、`I=Y`、`V=Y`、`overall_status=complete`。Gate 7 不使用 Slurm。
