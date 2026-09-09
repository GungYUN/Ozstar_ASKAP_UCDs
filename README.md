# Ozstar ASKAP/UCD pipeline

This directory is the Ozstar-side implementation described in
`../Ozstar_ASKAP_Migration_Plan.md`.

The persistent work root is:

```text
/fred/oz299/qhuang/ASKAP-UCDs/
```

The program directory, catalogues, staging data, final products and state
files are all below that root.  The pipeline does not use `$TMPDIR` for MS
storage.

## Files

| File | Purpose |
|---|---|
| `ozstar_main.py` | Query TAP, pack sources, write a manifest and submit Slurm |
| `process_job.py` | Compute-node worker: download, extract, DStools, crop and checkpoint |
| `casda_query.py` | CASDA filtering, staging and safe per-file download |
| `crop_fits.py` | 10 arcmin FITS crop with a valid celestial WCS |
| `transfer_products.py` | Password-assisted SCP/rsync in either direction |
| `transfer_to_ada.sh` | Convenience wrapper for Ozstar → ada |
| `transfer_to_ozstar.sh` | Convenience wrapper for ada → Ozstar |
| `deploy_to_ozstar.sh` | Copy this program directory to the Ozstar work root |
| `prepare_layout.sh` | Create the persistent Ozstar directory layout |
| `setup_host_python.sh` | Create a venv host Python environment on Fred |
| `config.py` | Central paths and resource settings |

## Initial layout

Copy the two CSV files to the default locations, or set `ASKAP_CATALOGUE` and
`ASKAP_TIME_CSV` in the environment:

```text
/fred/oz299/qhuang/ASKAP-UCDs/catalogue/
    UltracoolSheet_Main_index_unbinaryUCD.csv
    All_combine_data_unrepetition.csv
```

Create the directories before the first run:

```bash
./prepare_layout.sh
```

To deploy/update this program directory from ada or a local Linux/macOS host,
run `deploy_to_ozstar.sh`.  It uses the same mode-600 Ozstar password file and
does not put the password in the SCP command:

```bash
./deploy_to_ozstar.sh
```

The DStools container must exist at:

```text
/fred/oz299/qhuang/dstools/dstools-v2.0.0.sif
```

The host Python environment needs `numpy`, `pandas`, `astropy`, `h5py` and
`astroquery`.  DStools, casacore and WSClean are provided by the Apptainer
container.  Create a venv on Fred with:

```bash
./setup_host_python.sh
module load python/3.12.3
export ASKAP_PYTHON_MODULE=python/3.12.3
export ASKAP_PYTHON_BIN=/fred/oz299/qhuang/ASKAP-UCDs/.venv/askap-python/bin/python
```

The script only executes `module load`; it does not run `module purge`,
`module unload` or remove any existing environment.  If the current shell
already has the `mamba` module loaded and the Python module reports a conflict,
start a new login shell and run the script there without loading `mamba`.
Always use the absolute venv interpreter, or set `ASKAP_PYTHON_BIN` to another
prepared interpreter, before running `ozstar_main.py`.  The generated Slurm
script loads the configured Python module and checks the imports before starting
a job.

## Credentials

No password is stored in the program.  CASDA uses a mode-600 file containing
only the CASDA password, or the `CASDA_PASSWORD` environment variable:

```text
~/.config/askap/casda-password
```

For SSH transfer, `sshpass` is required.  The recommended setup is a
mode-600 password file on the machine initiating each direction:

```bash
mkdir -p ~/.config/askap
read -r -s TRANSFER_PASSWORD
printf '%s\n' "$TRANSFER_PASSWORD" > ~/.config/askap/transfer-password
unset TRANSFER_PASSWORD
chmod 600 ~/.config/askap/transfer-password
```

The file used for each direction can be changed with:

```bash
export ADA_PASSWORD_FILE="$HOME/.config/askap/ada-password"
export OZSTAR_PASSWORD_FILE="$HOME/.config/askap/ozstar-password"
```

`to-ada` uses the password for the ada account.  `to-ozstar` uses the
password for the Ozstar account.  As an alternative, set
`TRANSFER_PASSWORD` only for the lifetime of the transfer command.  The
password is passed to `sshpass` without appearing in the SCP command line or
the program log.

SSH host keys should be accepted and verified manually once before an
automated transfer.  The transfer tool intentionally does not disable host
key checking.

## Plan and submit a job

Run on the Ozstar login node.  First use a dry plan:

```bash
cd /fred/oz299/qhuang/ASKAP-UCDs/ozstar_askap
module load python/3.12.3
export ASKAP_PYTHON_MODULE=python/3.12.3
export ASKAP_PYTHON_BIN=/fred/oz299/qhuang/ASKAP-UCDs/.venv/askap-python/bin/python
$ASKAP_PYTHON_BIN ozstar_main.py --begin 1 --end 50 --no-submit
```

When the manifest is correct, submit it:

```bash
$ASKAP_PYTHON_BIN ozstar_main.py --begin 1 --end 50 --submit
```

The same range can be set at the top of `ozstar_main.py` by changing
`UCS_START`, `UCS_END` and `SUBMIT`.  `--retry-existing` is required when a
previous `complete`, `queued` or `running` state should deliberately be
replanned.

The default packer uses 4 slots × 8 CPU, estimates 4 hours per observation,
targets 40 observations and requests 48 hours.  These values are all in
`config.py` and can be overridden with environment variables.

Each source is downloaded in waves of at most four observations.  A wave is
processed before the next wave is downloaded.  Therefore a source with many
observations does not leave all of its MS files in `staging/` at once.

## Transfer products to ada

Run on Ozstar after the Slurm job has produced products:

```bash
cd /fred/oz299/qhuang/ASKAP-UCDs/ozstar_askap
./transfer_to_ada.sh --batch UCS1-50
```

Multiple batches can be transferred in one invocation:

```bash
./transfer_to_ada.sh --batch UCS1-50 --batch UCS51-100
```

The default source is `products/`; the default ada destination is:

```text
/import/ada1/qhua0119/Visibility/
```

Only `.ds` files and the two WSClean MFS image names are transferred and
verified.  Verification compares every relative product path and SHA-256
digest; the source is retained unless `--delete-source` is supplied:

```bash
./transfer_to_ada.sh --batch UCS1-50 --delete-source
```

Deletion happens only after the remote product paths and SHA-256 digests match
the local batch.
For Ozstar → ada deletion, every source in the batch must also have a
`complete` state in `state/sources/`.  Reverse-transfer deletion is refused by
default because ada has no Ozstar worker state; only use
`--allow-delete-without-state` after manual verification.

## Reverse transfer

The same code can copy ada-compatible batch trees back to Ozstar.  Run the
program on ada, with this directory available there:

```bash
python3 transfer_products.py \
  --direction to-ozstar \
  --source-root /import/ada1/qhua0119/Visibility \
  --batch UCS1-50
```

The default remote destination is
`/fred/oz299/qhuang/ASKAP-UCDs/products/`.  Use `--remote-root` to select a
different Ozstar directory.

## ada plotting

After transfer, confirm the expected tree exists under ada's `Visibility/`
directory.  Run the existing ada main with download, decompression and
DStools disabled, and plotting/combining enabled:

```python
STEPS = {
    "download_visibility": False,
    "decompress_move": False,
    "dstool_process": False,
    "plot_images": True,
    "combine_images": True,
}
```

The 10 arcmin WCS-preserving images are sufficient for the existing 1 arcmin,
3 arcmin and 60 arcsec plotting products.

## State and recovery

State is stored below `state/`:

```text
state/tap_cache/       TAP ECSV results
state/sources/         per-source status and SB checkpoints
state/jobs/             manifests and generated sbatch scripts
```

An existing `.ds` plus Stokes-I FITS product is treated as complete for that
SB.  Failed SB directories remain in `staging/` for a later retry.  A job
that reaches its walltime reserve exits with `partial`; the next manifest can
resume the same source.

---

# 中文使用说明

本目录是运行在 Ozstar 上的 ASKAP/UCD 处理程序组。程序负责 CASDA 查询和下载、
解压、DStools 处理、FITS 裁剪以及 Slurm 提交；最终的 DS/Cutout/Contour 绘图和
图像拼接仍在 HPC-ada 上使用原有程序完成。

## 中文 1：固定路径和目录

Ozstar 的持久化工作目录为：

```text
/fred/oz299/qhuang/ASKAP-UCDs/
```

程序、目录、catalogue、临时处理数据、最终产品和状态文件均位于该目录下。MS
数据不放在 `$TMPDIR` 中。

主要目录结构为：

```text
/fred/oz299/qhuang/ASKAP-UCDs/
    ozstar_askap/       # 程序
    catalogue/          # 两个 CSV 文件
    staging/            # 当前正在处理的 tar/MS
    products/           # 已完成的 .ds 和裁剪 FITS
    state/              # TAP cache、manifest、source 状态、SB checkpoint
    logs/               # Slurm 和 SB 日志
```

首先创建目录：

```bash
cd /fred/oz299/qhuang/ASKAP-UCDs/ozstar_askap
./prepare_layout.sh
```

需要存在以下两个 CSV：

```text
/fred/oz299/qhuang/ASKAP-UCDs/catalogue/UltracoolSheet_Main_index_unbinaryUCD.csv
/fred/oz299/qhuang/ASKAP-UCDs/catalogue/All_combine_data_unrepetition.csv
```

第二个 CSV 用于读取每个 SB 的 `t_min`，计算自行运动修正。建议不要省略。

## 中文 2：部署和 host Python 环境

从 ada 或本地 Linux/macOS 主机更新程序目录：

```bash
./deploy_to_ozstar.sh
```

该程序组需要一个 host Python 环境，用于运行 TAP 查询、manifest 生成和计算节点
上的调度脚本。DStools、casacore 和 WSClean 仍由 Apptainer 容器提供。

建议重新登录一个干净的 Ozstar shell，不要在已经加载 `mamba` 的 shell 中强行
加载 Python module。下面的脚本只执行 `module load`，不会执行 `module purge`、
`module unload`，也不会删除已有环境：

```bash
cd /fred/oz299/qhuang/ASKAP-UCDs/ozstar_askap
./setup_host_python.sh
```

然后在当前 shell 中加载与 venv 创建时相同的 Python module：

```bash
module load python/3.12.3
export ASKAP_PYTHON_MODULE=python/3.12.3
export ASKAP_PYTHON_BIN=/fred/oz299/qhuang/ASKAP-UCDs/.venv/askap-python/bin/python
```

验证环境：

```bash
$ASKAP_PYTHON_BIN -c \
'import numpy, pandas, astropy, h5py, astroquery; print("Python environment OK")'
```

以后不要用没有 numpy 的系统 `python3` 或 `python` 运行 `ozstar_main.py`。如果
集群提供的 Python 版本不是 `3.12.3`，先运行 `module spider python`，然后设置
正确的 `ASKAP_PYTHON_MODULE`。

DStools 容器必须位于：

```text
/fred/oz299/qhuang/dstools/dstools-v2.0.0.sif
```

## 中文 3：密码和 SSH 传输

程序不会保存密码。CASDA 密码可以放在以下 mode-600 文件中：

```bash
mkdir -p ~/.config/askap
read -r -s CASDA_PASSWORD
printf '%s\n' "$CASDA_PASSWORD" > ~/.config/askap/casda-password
unset CASDA_PASSWORD
chmod 600 ~/.config/askap/casda-password
```

Ozstar 向 ada 传输时，需要保存 ada 账号的密码：

```bash
read -r -s ADA_PASSWORD
printf '%s\n' "$ADA_PASSWORD" > ~/.config/askap/ada-password
unset ADA_PASSWORD
chmod 600 ~/.config/askap/ada-password
export ADA_PASSWORD_FILE="$HOME/.config/askap/ada-password"
```

传输程序需要 `sshpass`。密码通过 `sshpass` 自动填写，不会出现在源码或打印的
SCP 命令中。第一次连接两台服务器时，建议先人工确认 SSH host key。

## 中文 4：处理源编号和光谱型筛选

`--begin` 和 `--end` 都是包含端点的 UCS 编号范围。例如处理 UCS1501 至 UCS1550：

```bash
--begin 1501 --end 1550
```

当前代码使用原 ada 程序的筛选逻辑：

```text
sptnumabs_formula >= 20
```

光谱型不满足条件、没有可用 CASDA 数据或已完成的源会被自动跳过。

## 中文 5：先生成处理计划

在 Ozstar 登录节点执行：

```bash
cd /fred/oz299/qhuang/ASKAP-UCDs/ozstar_askap
module load python/3.12.3
export ASKAP_PYTHON_MODULE=python/3.12.3
export ASKAP_PYTHON_BIN=/fred/oz299/qhuang/ASKAP-UCDs/.venv/askap-python/bin/python

$ASKAP_PYTHON_BIN ozstar_main.py \
  --begin 1501 \
  --end 1550 \
  --no-submit
```

`--no-submit` 只进行以下操作：

- 读取源 catalogue；
- 通过 TAP 查询每个源的观测数；
- 应用 `sptnumabs_formula >= 20` 和 CASDA 数据筛选；
- 按估计观测数打包任务；
- 生成 manifest 和 Slurm 脚本；
- 不下载数据，也不占用计算节点。

manifest 位于：

```text
/fred/oz299/qhuang/ASKAP-UCDs/state/jobs/
```

可以查看最新的任务文件：

```bash
ls -lt /fred/oz299/qhuang/ASKAP-UCDs/state/jobs/
```

默认配置为 4 个并行槽、每槽 8 CPU、每次目标约 40 次观测、48 小时墙钟，具体
参数位于 `config.py`，也可以通过环境变量覆盖。

## 中文 6：提交 Slurm 任务

确认 manifest 中的源列表和观测数无误后提交：

```bash
$ASKAP_PYTHON_BIN ozstar_main.py \
  --begin 1501 \
  --end 1550 \
  --submit
```

生成的 Slurm 作业会自动：

- 加载 `apptainer`；
- 加载 `ASKAP_PYTHON_MODULE` 指定的 Python module；
- 使用 `ASKAP_PYTHON_BIN` 指定的 venv Python；
- 检查 numpy、pandas、astropy、h5py 和 astroquery；
- 在计算节点运行 `process_job.py`。

查看任务：

```bash
squeue -u qhuang
```

任务结束后查看：

```bash
sacct -j JOBID --format=JobID,State,Elapsed,ExitCode
```

如果作业因为达到墙钟保护而返回 `partial`，不一定表示数据损坏；应检查
`state/sources/` 中每个源的状态。

## 中文 7：Ozstar 端实际处理逻辑

每个源按以下顺序处理：

1. 从 CASDA 下载当前波次的最多 4 个观测；
2. 将 tar 文件解压并移动到 `SBXXXXX_beamYY/`；
3. 运行 `dstools-askap-preprocess`；
4. 运行 `dstools-create-model`，每个任务使用 8 CPU；
5. 运行自动位置 mask 的 `dstools-insert-model`；
6. 运行 `dstools-subtract-model`；
7. 运行 `dstools-extract-ds` 生成 `.ds`；
8. 将 Stokes I/V FITS 裁剪到 10′×10′；
9. 将 `.ds` 和 FITS 写入 `products/`；
10. 删除该 SB 的 MS 和中间文件。

同一源最多同时处理 4 个 SB。当前源处理完一波后，才下载下一波，因此不会把
一个源的全部原始 MS 同时堆积在磁盘上。

## 中文 8：继续处理后续源

一次 Slurm 任务通常只处理部分源。前一个任务完成后，重复运行相同范围的计划：

```bash
$ASKAP_PYTHON_BIN ozstar_main.py \
  --begin 1501 \
  --end 1550 \
  --no-submit
```

检查新的 manifest 后再提交：

```bash
$ASKAP_PYTHON_BIN ozstar_main.py \
  --begin 1501 \
  --end 1550 \
  --submit
```

程序会根据 source state 跳过 `complete`、`queued` 和 `running` 源，并继续选择
后面的未完成源。前一个任务仍在 `queued` 或 `running` 时，不要重复提交同一范围。

如果希望重新查询 CASDA，而不是使用已有 TAP cache，可以使用：

```bash
$ASKAP_PYTHON_BIN ozstar_main.py \
  --begin 1501 \
  --end 1550 \
  --refresh-cache \
  --no-submit
```

`--retry-existing` 只在确实需要重新计划已经处于 `complete`、`queued` 或 `running`
状态的源时使用，正常断点续跑不需要该参数。

## 中文 9：输出目录

UCS1501–UCS1550 属于同一个 50 源目录：

```text
/fred/oz299/qhuang/ASKAP-UCDs/products/UCS1501-1550/
```

单个源的输出结构类似：

```text
products/UCS1501-1550/UCSXXXX/LongObs/SBXXXXX_beamYY/
    SBXXXXX_beamYY.ds
    wsclean_model/
        wsclean-MFS-I-image.fits
        wsclean-MFS-V-image.fits
```

成功完成的源会从 `staging/` 删除原始 MS，只保留上述最终产品。

## 中文 10：传输到 HPC-ada

在 Ozstar 上，先进行 dry-run：

```bash
cd /fred/oz299/qhuang/ASKAP-UCDs/ozstar_askap
./transfer_to_ada.sh \
  --batch UCS1501-1550 \
  --dry-run
```

确认无误后正式传输：

```bash
./transfer_to_ada.sh \
  --batch UCS1501-1550
```

默认目标目录为：

```text
/import/ada1/qhua0119/Visibility/UCS1501-1550/
```

程序只传输 `.ds`、`wsclean-MFS-I-image.fits` 和
`wsclean-MFS-V-image.fits`，并比较本地与远端的相对路径和 SHA-256 digest。

在所有需要的数据都确认传输完成前，不要使用：

```bash
--delete-source
```

Ozstar → ada 删除源文件时，程序还会检查对应 source state 是否为 `complete`。

## 中文 11：反向传输

如果需要把 ada 上已有的 ada-compatible 数据树传回 Ozstar，在 ada 上运行：

```bash
python3 transfer_products.py \
  --direction to-ozstar \
  --source-root /import/ada1/qhua0119/Visibility \
  --batch UCS1501-1550
```

默认目标为：

```text
/fred/oz299/qhuang/ASKAP-UCDs/products/
```

## 中文 12：在 ada 上绘图

传输完成后，确认 ada 上存在：

```text
/import/ada1/qhua0119/Visibility/UCS1501-1550/
```

修改 ada 原来的 `Main_SpecType.py`：

```python
Batch_Begins = [1501]
Number = 50

STEPS = {
    "download_visibility": False,
    "decompress_move": False,
    "dstool_process": False,
    "plot_images": True,
    "combine_images": True,
}
```

然后运行原来的 ada 主程序。第 4 步会生成 DS、Cutout 和 Contour 图，第 5 步
会进行最终图像拼接。10′ FITS 足够支持原程序需要的 1′、3′ 和 60″ 图像。

## 中文 13：状态、日志和断点恢复

状态文件位置：

```text
state/tap_cache/       TAP 查询结果
state/sources/         每个源的状态和 SB checkpoint
state/jobs/            manifest 和生成的 sbatch 脚本
logs/                  Slurm 日志和每个 SB 的 DStools 日志
```

一个 SB 只有在有效 `.ds` 和 Stokes-I FITS 都存在时才会被视为完成。失败的 SB
会留在 `staging/`，下一次提交时继续处理。任务达到墙钟保护后会退出为
`partial`，下一次使用相同编号范围即可恢复。
