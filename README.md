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
     'import numpy, pandas, astropy, h5py, astroquery, keyring; print("Python environment OK")'
   ```

   The setup script installs the packages in `requirements.txt` into the
   persistent venv. It only loads the configured modules. It does **not** run
   `module purge` or `module unload`; retain the existing site module/venv
   handling.

5. Confirm the DStools Apptainer image exists:

   ```text
   /fred/oz299/qhuang/dstools/dstools-v2.0.0.sif
   ```

   Override it with `DSTOOLS_CONTAINER` only when the intended image has been
   validated. DStools, casacore, and WSClean run inside this image; the host
   venv is for orchestration, TAP, ECSV, and state management.

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
     'import numpy, pandas, astropy, h5py, astroquery, keyring; print("Python environment OK")'
   ```

   setup 脚本把 `requirements.txt` 中的包安装到持久化 venv。它只加载配置的
   module，不执行 `module purge` 或 `module unload`；保留现有的 site module/venv
   处理方式。

5. 确认 DStools 的 Apptainer 镜像存在：

   ```text
   /fred/oz299/qhuang/dstools/dstools-v2.0.0.sif
   ```

   只有在验证过目标镜像后才使用 `DSTOOLS_CONTAINER` 覆盖它。DStools、casacore
   和 WSClean 在该镜像内运行；主机 venv 负责 orchestration、TAP、ECSV 和状态管理。

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

1. Load the catalogue and query CASDA TAP for each new source. The existing
   quality, spectral-type, exposure, radius, size, release, and nearest-
   `obs_id` filters are retained.
2. Write the complete filtered and deduplicated TAP rows to
   `state/tap_cache/*.ecsv`. Every returned column is retained, including
   `access_estsize` and returned `t_min`/`t_max`.
3. Sum the deduplicated `access_estsize` values. CASDA reports this field in
   KB, so the sum is converted to bytes. A controller block contains at most
   50 UCS numbers and is split before its estimate would exceed the 2 TB
   staging budget (`2 * 1024**4` bytes by default). A single source above the
   budget fails clearly. Current staging usage and actual free space below
   `ASKAP_WORK` are checked before download.
4. On the login node, authenticate to CASDA and download only visibility tar
   archives (`.tar`, `.tar.gz`, `.tgz`) and their `.checksum` files. The login
   node does **not** extract archives.
5. Submit one short-name Slurm worker for the prepared block. On the compute
   node, the worker extracts only the current source's archives, runs DStools,
   crops the FITS images, atomically promotes products, and writes checkpoints.
6. The login controller polls source state and filesystem products. It advances
   only after each source is `complete`, `no_data`, or `skipped`; a `complete`
   source must also have valid expected `.ds` and Stokes-I FITS products and no
   unprocessed MS/archive residue in staging.
7. When walltime protection returns the dedicated partial exit code, the
   generated sbatch script automatically resubmits itself up to
   `ASKAP_MAX_AUTO_RESUBMITS` (20 by default). No manual queue-based completion
   decision is required.

For a plan that writes TAP caches and manifests but neither downloads nor
submits, use `--no-submit`. `--prepare-download` is retained for an explicit
login-node preparation-only run. `--retry-existing` deliberately resets
incomplete controller state; do not use it for an ordinary reconnect. The
`--allow-compute-download` option is a debugging opt-in and is not part of the
normal Ozstar network architecture.

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
| CASDA batching | `ASKAP_CASDA_STAGE_BATCH_SIZE`, `ASKAP_CASDA_QUERY_LIMIT` | CASDA staging batch size `20` and TAP row limit `5000`. |
| Download retry | `ASKAP_CASDA_SOURCE_DOWNLOAD_RETRIES`, `ASKAP_CASDA_SOURCE_RETRY_DELAY_SECONDS` | Each source is retried `3` times with a `30` second delay; after exhaustion the controller records `download_failed` and continues. |
| Time estimate | `ASKAP_HOURS_PER_OBS` | `3.0` hours per observation for packing and worker wave checks. |
| Time estimate | `ASKAP_SOURCE_OVERHEAD_HOURS` | `0.5` hours per source in the controller estimate. |
| Slurm time | `ASKAP_JOB_WALLTIME_HOURS` | `48` hours for generated workers; this is not a completion criterion. |
| Walltime safety | `ASKAP_WALLTIME_RESERVE_MINUTES` | `20` minutes kept in reserve before a partial exit. |
| CPU | `ASKAP_PARALLEL_SLOTS` | `4` concurrent SB waves. |
| CPU | `ASKAP_CPU_PER_SLOT` | `8` threads per DStools process; total default is `4 x 8 = 32` CPUs. The product must not exceed 32. |
| CPU | `ASKAP_CREATE_MODEL_THREADS` | Defaults to `ASKAP_CPU_PER_SLOT`; DStools model threads. |
| Controller | `ASKAP_CONTROLLER_INITIAL_DELAY_SECONDS`, `ASKAP_CONTROLLER_POLL_INTERVAL_SECONDS` | `30` seconds and `60` seconds; filesystem polling timing only. CLI equivalents are available. |
| Resubmission | `ASKAP_MAX_AUTO_RESUBMITS` | `20` automatic self-resubmissions after partial worker exit. |
| Python/modules | `ASKAP_PYTHON_BIN`, `ASKAP_PYTHON_PARENT_MODULE`, `ASKAP_PYTHON_MODULE` | Host interpreter and module names used by generated workers. |
| Container | `DSTOOLS_CONTAINER`, `APPTAINER_BIN` | DStools image and Apptainer executable. |

The following processing controls are also environment-overridable when a
scientific change is intentional: `ASKAP_DSTOOLS_BAND`,
`ASKAP_CREATE_MODEL_ITERATIONS`, `ASKAP_MIN_UV_METRES`,
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
| Transfer from Ozstar to ada | `ADA_SSH_USER`, `ADA_SSH_HOST`, `ADA_VISIBILITY_ROOT`, `ADA_PASSWORD_FILE` |
| Transfer from ada to Ozstar | `OZSTAR_SSH_USER`, `OZSTAR_SSH_HOST`, `OZSTAR_PRODUCT_ROOT`, `OZSTAR_PASSWORD_FILE` |
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

1. 读取 catalogue，并为每个新源查询 CASDA TAP。保留原有的质量、光谱型、曝光、
   半径、大小、release 和最近 `obs_id` 筛选。
2. 把完整的过滤和去重后的 TAP 行写入 `state/tap_cache/*.ecsv`。TAP 返回的每一列
   都会保留，包括 `access_estsize` 以及返回时存在的 `t_min`/`t_max`。
3. 对去重后的 `access_estsize` 求和。CASDA 的单位是 KB，因此会转换为 bytes。每个
   controller block 最多包含 50 个 UCS 编号，并在估计值即将超过 2 TB staging 上限
   （默认 `2 * 1024**4` bytes）前拆分。单个源超过上限会明确失败。下载前还会检查
   当前 staging 占用和 `ASKAP_WORK` 的实际剩余空间。
4. 登录节点向 CASDA 登录，只下载 visibility tar 压缩包（`.tar`、`.tar.gz`、`.tgz`）
   和对应的 `.checksum`。登录节点**不解压**压缩包。
5. 为准备好的 block 提交短名称 Slurm worker。计算节点只解压当前源的压缩包，运行
   DStools、裁剪 FITS、原子地提升产物并写入 checkpoint。
6. 登录节点 controller 轮询源状态和文件系统产物。只有每个源为 `complete`、`no_data`
   或 `skipped` 才会推进；`complete` 还必须有有效的预期 `.ds`、Stokes-I FITS，且
   staging 中没有未处理的 MS 或 archive 残留。
7. worker 因墙钟保护返回专用 partial exit code 时，生成的 sbatch 会自动重新提交自己，
   默认最多 `ASKAP_MAX_AUTO_RESUBMITS=20` 次。不需要人工根据队列判断完成。

只生成 TAP cache 和 manifest、不下载也不提交时使用 `--no-submit`。`--prepare-download`
保留给显式的登录节点准备-only 运行。`--retry-existing` 会有意重置不完整 controller
状态，普通断线重连不要使用。`--allow-compute-download` 只用于调试，不属于正常的
Ozstar 网络架构。

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
| CASDA 批量 | `ASKAP_CASDA_STAGE_BATCH_SIZE`、`ASKAP_CASDA_QUERY_LIMIT` | CASDA staging 批量 `20`、TAP 行数上限 `5000`。 |
| 下载重试 | `ASKAP_CASDA_SOURCE_DOWNLOAD_RETRIES`、`ASKAP_CASDA_SOURCE_RETRY_DELAY_SECONDS` | 每个源默认重试 `3` 次、间隔 `30` 秒；仍失败则记录 `download_failed` 并继续下一个源。 |
| 时间估计 | `ASKAP_HOURS_PER_OBS` | 每个 observation `3.0` 小时，用于 packing 和 worker wave 判断。 |
| 时间估计 | `ASKAP_SOURCE_OVERHEAD_HOURS` | controller 估计中每源 `0.5` 小时。 |
| Slurm 时间 | `ASKAP_JOB_WALLTIME_HOURS` | 生成 worker 的 `48` 小时，不是完成条件。 |
| 墙钟安全 | `ASKAP_WALLTIME_RESERVE_MINUTES` | partial 退出前保留 `20` 分钟。 |
| CPU | `ASKAP_PARALLEL_SLOTS` | `4` 个并发 SB wave。 |
| CPU | `ASKAP_CPU_PER_SLOT` | 每个 DStools 进程 `8` 线程；默认总计 `4 x 8 = 32` CPU，不能超过 32。 |
| CPU | `ASKAP_CREATE_MODEL_THREADS` | 默认等于 `ASKAP_CPU_PER_SLOT`，DStools model 线程数。 |
| controller | `ASKAP_CONTROLLER_INITIAL_DELAY_SECONDS`、`ASKAP_CONTROLLER_POLL_INTERVAL_SECONDS` | `30` 秒和 `60` 秒，只影响文件系统轮询；也有 CLI 选项。 |
| 自动重提交 | `ASKAP_MAX_AUTO_RESUBMITS` | partial worker 后自动 self-resubmit 的次数，默认 `20`。 |
| Python/module | `ASKAP_PYTHON_BIN`、`ASKAP_PYTHON_PARENT_MODULE`、`ASKAP_PYTHON_MODULE` | 生成 worker 使用的主机解释器和 module。 |
| 容器 | `DSTOOLS_CONTAINER`、`APPTAINER_BIN` | DStools 镜像和 Apptainer 可执行文件。 |

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
| Ozstar 到 ada | `ADA_SSH_USER`、`ADA_SSH_HOST`、`ADA_VISIBILITY_ROOT`、`ADA_PASSWORD_FILE` |
| ada 到 Ozstar | `OZSTAR_SSH_USER`、`OZSTAR_SSH_HOST`、`OZSTAR_PRODUCT_ROOT`、`OZSTAR_PASSWORD_FILE` |
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

The ada-compatible product tree is:

```text
products/UCS1501-1550/UCSXXXX/LongObs/SBXXXXX_beamYY/
    SBXXXXX_beamYY.ds
    wsclean_model/
        wsclean-MFS-I-image.fits
        wsclean-MFS-V-image.fits
```

The transfer is always explicit and starts from the Ozstar side:

```bash
cd /fred/oz299/qhuang/ASKAP-UCDs/ozstar_askap
./transfer_to_ada.sh --batch UCS1501-1550 --dry-run
./transfer_to_ada.sh --batch UCS1501-1550
```

The wrapper transfers only `.ds` files and the MFS Stokes-I/V FITS files, then
compares the local and remote SHA-256 inventories. Use `--method rsync` for a
large or interrupted transfer. `--delete-source` is never implicit and should
be used only after the remote inventory matches and all local source states are
`complete`:

```bash
./transfer_to_ada.sh --batch UCS1501-1550 --method rsync --delete-source
```

The transfer password is read from a mode-600 file on the machine initiating
the transfer, or from one-process `TRANSFER_PASSWORD`. Password values are not
embedded in source, arguments, manifests, or output. The reverse wrapper
`transfer_to_ozstar.sh` is available for an explicit ada-to-Ozstar transfer;
its deletion safeguard requires a separate deliberate option.

### 中文

传输前确认 `state/controllers/UCS<begin>-<end>.json` 的状态为 `complete`。检查
`state/sources/` 中的源状态文件，并查看 `logs/` 和生成的 Slurm 输出是否有失败。
不能仅凭 Slurm job 退出就认为 block 完成：每个源都必须处于终态，而且 `complete`
源必须通过产物和 staging 残留检查。

每个源的 SB 下载结果单独保存在
`state/sources/UCSXXXX.downloads.json`。每条记录包含源名和 SB 键、最新状态
（`downloaded`、`download_failed` 或 checksum 状态）、尝试次数、去敏后的 URL 和重试历史。

ada-compatible 产物目录如下：

```text
products/UCS1501-1550/UCSXXXX/LongObs/SBXXXXX_beamYY/
    SBXXXXX_beamYY.ds
    wsclean_model/
        wsclean-MFS-I-image.fits
        wsclean-MFS-V-image.fits
```

传输必须显式执行，并从 Ozstar 侧开始：

```bash
cd /fred/oz299/qhuang/ASKAP-UCDs/ozstar_askap
./transfer_to_ada.sh --batch UCS1501-1550 --dry-run
./transfer_to_ada.sh --batch UCS1501-1550
```

wrapper 只传输 `.ds` 和 MFS Stokes-I/V FITS，然后比较本地与远端的 SHA-256 清单。
大型或中断后的传输可使用 `--method rsync`。`--delete-source` 永远不是隐含行为，
只有在远端清单匹配且所有本地源状态都是 `complete` 后才使用：

```bash
./transfer_to_ada.sh --batch UCS1501-1550 --method rsync --delete-source
```

传输密码从发起传输的机器上的 mode-600 文件读取，或使用当前进程的
`TRANSFER_PASSWORD`。密码值不会写入源码、参数、manifest 或输出。
`transfer_to_ozstar.sh` 可用于显式的 ada-to-Ozstar 反向传输；反向删除也需要另外的
明确选项和人工确认。

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
  atomic products/checkpoints -> source state -> partial self-resubmit if needed
                                |
                                v
Ozstar persistent products -> explicit SHA-256-verified transfer -> ada
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
- The current resource model is 3 hours per observation, four concurrent slots
  of eight CPU threads (`4 x 8`), 80G Slurm memory, a 48-hour generated walltime
  default, a 20-minute worker reserve, and a compact Slurm job name.
- Transfer to ada is explicit, password-safe, and verified. Processing never
  starts an automatic transfer.
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
login, and safe archive downloads; `prepare_download.py` performs login-node
preparation; `staging.py` safely extracts on compute nodes;
`process_job.py` runs DStools and checkpoints; `pipeline_utils.py` validates
state/products; `crop_fits.py` preserves useful celestial WCS while cropping;
and `transfer_products.py` performs explicit verified transfer.

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
  原子产物/checkpoint -> 源状态 -> 必要时 partial 自动 self-resubmit
                              |
                              v
Ozstar 持久化产物 -> 显式 SHA-256 校验传输 -> ada
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
- 当前资源模型是每个 observation 3 小时、4 个并发 slot、每 slot 8 个 CPU 线程
  （`4 x 8`）、80G Slurm 内存、默认 48 小时生成 walltime、20 分钟 worker reserve，
  以及短 Slurm 名称。
- 到 ada 的传输必须显式执行、密码安全并校验。处理命令不会自动传输。
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
TAP 筛选、keyring 登录和安全 archive 下载；`prepare_download.py` 负责登录节点准备；
`staging.py` 负责计算节点安全解压；`process_job.py` 运行 DStools 并写 checkpoint；
`pipeline_utils.py` 验证状态和产物；`crop_fits.py` 保留有效天球 WCS 并裁剪；
`transfer_products.py` 执行显式且经过验证的传输。

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

For a one-row comparison using the deployed production venv, production
keyring adapter, current `UCS1501.ecsv`, `build_casda_client()`, `stage_data()`,
and single-data-URL `download_files()`, run on Ozstar:

```bash
$ASKAP_PYTHON_BIN casda_production_compare.py \
  --source UCS1501 \
  --obs-id 63406
```

The diagnostic writes only below `WORK/diagnostics/`, not to production
`staging/` or `products/`. Do not publish complete signed URLs printed by
lower-level Astroquery progress output.

如果需要使用部署后的生产 venv、生产 keyring 适配器、当前
`UCS1501.ecsv`、`build_casda_client()`、`stage_data()` 和单个 data URL 的
`download_files()` 做一对一对照，可以在 Ozstar 运行：

```bash
$ASKAP_PYTHON_BIN casda_production_compare.py \
  --source UCS1501 \
  --obs-id 63406
```

诊断文件只写入 `WORK/diagnostics/`，不会写入生产 `staging/` 或 `products/`。
不要公开底层 Astroquery 输出中的完整预签名 URL。
