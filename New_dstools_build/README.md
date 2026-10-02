# OzSTAR DStools dev SIF：完全离线计算节点构建包

本目录取代先前依赖计算节点联网的四文件版本。OzSTAR 计算节点按完全无外网处理：所有下载、依赖解析和数据冻结都在登录节点完成；Slurm 作业只读取 `/fred` 中已经校验的文件并编译 SIF。

## 文件

- `versions.env`：人工选择的版本、完整 commit、模块和依赖清单。
- `prep_sources.sh`：登录节点联网准备程序。
- `askap-dstools.def`：完全离线的 Apptainer definition。
- `build_sif.sbatch`：计算节点构建作业，不进行网络探测或下载。
- `patches/0001-visibility-weighting-unit.patch`：保留 ASKAP 的 unit visibility weighting。
- `patches/0002-unique-integrations.patch`：按唯一时间戳计算 integrations。
- `patches/0003-wsclean-header-only-boost-system.patch`：让 WSClean 3.5
  不再要求 Boost 1.92 已转为 header-only 的 `system` 二进制组件。
- `patches/0004-wsclean-radler-offline-fetchcontent.patch`：将预先冻结的
  xtensor 系列源码路径传入 Radler 的独立 CMake 配置，防止计算节点尝试联网。
- `patches/0005-wsclean-consistent-abi-libraries.patch`：避免 WSClean 的最终
  链接混用系统 OpenSSL 与冻结的 `/opt/askap` OpenSSL。构建选项还会让
  `chgcentre` 等独立可执行文件使用同一组冻结的 OpenSSL 和 C++ 运行库。
- `patches/0006-dstools-subprocess-drain-and-check.patch`：修复 DStools 在
  stdout/stderr 其中一个管道先关闭时提前返回的问题；现在会排空两个管道、
  等待 WSClean，并把非零退出码传回 pipeline。

## 0. 上传完整 recipe 目录

在本地工作站执行。必须上传整个目录，包括 `patches/`，并排除 `.DS_Store`：

```bash
BUILD_ROOT=/fred/oz299/qhuang/dstools/sif-build-20260918-offline-v1

ssh qhuang@ozstar.swin.edu.au \
  "mkdir -p $BUILD_ROOT/recipe $BUILD_ROOT/src/dstools-dev"

rsync -av --exclude='.DS_Store' \
  /Users/qhua0119/Documents/ChatGPT/Ozstar/New_dstools_build/ \
  qhuang@ozstar.swin.edu.au:$BUILD_ROOT/recipe/

rsync -av \
  /Users/qhua0119/Documents/ChatGPT/Ozstar/dstools/OZSTAR_SIF_BUILD_PLAN.md \
  qhuang@ozstar.swin.edu.au:$BUILD_ROOT/recipe/
```

再上传权威 DStools dev 快照：

```bash
rsync -av \
  --exclude='__pycache__' --exclude='*.pyc' \
  --exclude='.DS_Store' --exclude='.venv' \
  /Users/qhua0119/Documents/ChatGPT/DStools/dstools-dev/ \
  qhuang@ozstar.swin.edu.au:$BUILD_ROOT/src/dstools-dev/
```

不要覆盖或删除现有生产镜像：

```text
/fred/oz299/qhuang/dstools/dstools-v2.0.0.sif
```

## 1. 登录节点准备全部离线输入

```bash
cd /fred/oz299/qhuang/dstools/sif-build-20260918-offline-v1/recipe
bash prep_sources.sh
```

该步骤会在 `/fred/oz299/qhuang/dstools/sif-build-20260918-offline-v1` 下准备：

- 固定的 bootstrap SIF；
- casacore 3.7.1、python-casacore 3.7.1、WSClean 3.5 与 submodules；
- 应用三个强制 patch 后的 DStools dev 构建树；
- Jammy `.deb` 完整依赖闭包与本地 APT repository；
- 可重定位的 Python 3.12、NumPy 2.2.6 和 Python 3.12 Boost.Python 环境；
- 完整锁定的 Python wheelhouse，不包含 python-casacore wheel；
- 将 PyPI 只提供源码包的 `astroplan 0.10.1` 在登录节点构建为
  `py3-none-any` wheel，并检查其不含本地二进制文件；除这一项外仍禁止
  Python 依赖从源码隐式构建；
- 修正 `rm-lite 2025.5.1` 发布元数据中将实际依赖 `tqdm` 误写为 `tdqm`
  的上游问题；修正版 wheel 使用 `+askap1` 本地版本标记，且官方输入 wheel
  会先按 DStools `poetry.lock` 中的 SHA-256 校验；
- casacore measures 固定快照；
- CASA `casarundata + measures`，并记录实际数据版本；
- Astropy 官方天文台台址表 `coordinates/sites.json`，在登录节点下载并记录
  SHA-256，再作为镜像内只读数据提供给 `EarthLocation.of_site()`；DStools
  导入时需要 GMRT、VLA、MeerKAT 和 ASKAP，计算节点不能联网获取。登录
  节点优先使用已验证可达的 Astropy GitHub Raw 地址，备用数据站也设置了
  连接和总时限，避免无期限等待；
- 保留 Git 符号链接的四份源码 tar 归档；casacore 仓库跟踪的
  `casacore -> .` 不会被 Apptainer `%files` 解引用为循环目录；
- 从 WSClean 3.5 固定的 aocommon CMake 文件读取 xtensor 相关版本，
  在登录节点预取 `xtl`、`xsimd`、`xtensor`、`xtensor-blas`、`xtensor-fftw`，
  冻结实际 commit 和离线源码 tar；计算节点由 CMake 指向这些本地源码，
  不会尝试联网 FetchContent；
- 构建时仅对容器内 WSClean 源码应用固定的 Boost.System、Radler 离线
  FetchContent 和 ABI 链接补丁；Python 解释器、库、头文件以及 WSClean
  链接期的 C++/OpenSSL 库都指向 `/opt/askap`；
- 所有计算节点输入（包括源码归档）的 SHA-256 manifest。

登录节点步骤可能下载约数 GB 内容，尤其是 CASA 数据和 `casatools` wheels。任何下载、依赖解析、patch 或 checksum 错误都会停止，不应跳过后继续提交构建。

Poetry 在导出依赖时可能显示
`poetry-plugin-export will not be installed by default` 警告；本 recipe 已显式固定并安装
`poetry-plugin-export==1.8.0`，因此该提示不是错误，也不需要手动修改 Poetry 配置。

所有持久源码、下载、缓存、配置和产物均写在上述 `BUILD_ROOT`，不会写入小容量 home。唯一例外是 Slurm 构建作业通过 `#SBATCH --tmp=32G` 申请的节点本地临时盘：它只供 Apptainer 解包/组装 SIF，作业结束后自动清理；最终镜像、日志和 manifest 仍全部位于 `/fred/oz299/qhuang/dstools/...`。

创建可重定位 Python 环境时，micromamba 1.4.6 会强制登记
`$HOME/.conda/environments.txt`。脚本将容器内的 `/home/qhuang` 映射到
`$BUILD_ROOT/config/container-home`，所以该登记文件实际仍位于 `/fred`，不会写入
OzSTAR 的真实 home。该版本使用 `micromamba env export --explicit` 冻结精确包
URL；如果前一次运行已完成环境创建但在导出阶段中断，脚本会验证并复用完整的
`work/conda-askap.part.*` 环境。

本地 APT repository 的 `Packages` 索引直接从下载好的 `.deb` 控制记录及文件
校验值生成，不会为了调用 `dpkg-scanpackages` 而在准备容器中安装或升级
`dpkg-dev`。索引完成后还会禁用网络软件源，并以 `--simulate --no-download`
验证本地仓库能够独立解析全部构建依赖。

## 2. 在计算节点执行完全离线的构建

只有 `prep_sources.sh` 明确显示完成后才执行：

```bash
BUILD_ROOT=/fred/oz299/qhuang/dstools/sif-build-20260918-offline-v1
sha256sum -c --quiet "$BUILD_ROOT/manifests/build-input-files.sha256"
```

目前排错阶段建议在已申请的交互式计算节点运行；该节点必须有 `JOBFS`，
并将它传给脚本作为临时盘：

```bash
sinteractive --time=02:00:00 --ntasks=1 --cpus-per-task=8 --mem=32G --tmp=32G
```

这是当前已测试可申请的交互式资源上限；如果仍在原交互式节点内，无需重复申请。
在交互式节点以 `bash build_sif.sbatch` 运行时，脚本头部的 `#SBATCH` 行只是注释，
实际资源与时限由上述 `sinteractive` 申请决定。

```bash
BUILD_ROOT=/fred/oz299/qhuang/dstools/sif-build-20260918-offline-v1
: "${JOBFS:?JOBFS is required on the compute node}"
cd "$BUILD_ROOT/recipe"
set -o pipefail
SLURM_TMPDIR="$JOBFS" TMPDIR="$JOBFS" \
  bash build_sif.sbatch 2>&1 | tee "$BUILD_ROOT/logs/build-interactive-${SLURM_JOB_ID}.log"
build_rc=${PIPESTATUS[0]}
echo "Build exit code: $build_rc"
```

确认镜像构建流程稳定后，也可在登录节点通过
`sbatch "$BUILD_ROOT/recipe/build_sif.sbatch"` 提交相同脚本；不要在登录节点
直接执行构建。以上两种方式均使用 Slurm 计算资源，而非登录节点。

作业开始时会重新验证所有离线输入。definition 内只使用：

- `file:/opt/offline/apt`；
- `pip --no-index --find-links=/opt/offline/wheels`；
- 本地源码 tar、数据 archive 和已打包 Python 环境。源码 tar 在
  `%post` 中解包，保留 casacore 等仓库里的符号链接。

它不会访问 Ubuntu、PyPI、conda-forge、GitHub、GitLab、Docker Hub 或 ASTRON。
APT 安装步骤允许从镜像内的 `file:/opt/offline/apt` 本地仓库读取 `.deb`；
不使用 `--no-download`，因为该选项只允许使用已经进入 APT 缓存的包，
并不表示“仅从本地仓库安装”。

## 3. 构建输出

候选镜像写入：

```text
/fred/oz299/qhuang/dstools/sif-build-20260918-offline-v1/artifacts/
```

日志写入：

```text
/fred/oz299/qhuang/dstools/sif-build-20260918-offline-v1/logs/build-<JOBID>.out
/fred/oz299/qhuang/dstools/sif-build-20260918-offline-v1/logs/build-<JOBID>.err
```

构建成功只表示镜像内部自检通过。仍需按照 `OZSTAR_SIF_BUILD_PLAN.md` 执行 Gate 1–5，尤其是用已知的 preprocess 后 SB21677 MS 执行 WSClean 回归测试。在全部 Gate 通过前，不修改 production pipeline 的 `DSTOOLS_CONTAINER`。

构建脚本现在还会在提升候选镜像前，以普通用户身份和
`--cleanenv --no-home` 分别测试 DStools/python-casacore 与 CASA 导入。
构建期、SIF `%test` 和普通用户验收还会复现 stderr 提前 EOF 的情形，确认
DStools 继续读取 stdout，并确认子进程非零退出会抛出错误。
构建返回 `0` 后，仍可独立复核这个候选镜像（两个 Python 进程不可合并）：

```bash
BUILD_ROOT=/fred/oz299/qhuang/dstools/sif-build-20260918-offline-v1
module load apptainer
sha256sum -c "$BUILD_ROOT/manifests/candidate-sif.sha256"
CANDIDATE=$(awk 'NR==1 {print $2}' "$BUILD_ROOT/manifests/candidate-sif.sha256")
apptainer exec --cleanenv --no-home "$CANDIDATE" \
  /opt/askap/bin/python -c \
  'import casacore.tables; from dstools.utils import LOCATIONS; print(sorted(LOCATIONS))'
apptainer exec --cleanenv --no-home "$CANDIDATE" \
  /opt/askap/bin/python -c \
  'from casatasks import tclean, phaseshift, uvsub, mstransform; print("CASA tasks OK")'
apptainer exec --cleanenv --no-home "$CANDIDATE" \
  /opt/wsclean/bin/wsclean --version
```

构建会在 WSClean 安装后立即运行 `wsclean --version` 并打印输出；CASA 数据
版本则直接采用登录节点准备阶段冻结的 manifest。这样若运行时检查失败，日志会
显示具体命令和退出码，不会只留下笼统的 `%post exit status 255`。
镜像在构建、自检和运行时均设置 `OPENBLAS_NUM_THREADS=1`。这是 WSClean
针对多线程 OpenBLAS 的启动要求，不限制 WSClean 自身的 `-j` 并行线程数。
`%test` 使用正文首行的 `#!/bin/bash` 选择解释器；Apptainer 的 `-c /bin/bash`
选项仅适用于 `%post`，不可放在 `%test` 标题上。
构建与自检分别在两个 Python 进程中导入 `python-casacore` 和 `casatasks`，与
DStools 的 CASA 子进程隔离设计一致；不要在同一进程中同时导入这两套绑定。

## 重新运行规则

同一个 `BUILD_ROOT` 代表同一组冻结输入：

- 完整且 hash 相同的下载会复用；
- SIF 先写入带 Slurm job ID 的隐藏临时文件，完整构建和自检成功后才提升为 `candidate.sif`；
- 提升前额外以普通 OzSTAR 用户和 `--cleanenv --no-home` 导入 DStools、
  python-casacore 和 CASA；避免 fakeroot 自检掩盖镜像内数据权限错误；
- 候选镜像名称包含冻结输入清单的指纹；只修正 definition 或构建脚本并重新运行
  `prep_sources.sh` 时，会生成新的候选镜像，保留已有候选镜像；
- source commit、DStools 快照、patch、controller 版本或 dependency solve 变化时会停止；
- 需要升级任何版本时，新建一个新的日期/迭代 `BUILD_ROOT`，并同步修改 `versions.env` 和 `build_sif.sbatch` 的 `#SBATCH --output/--error` 路径。

这样可以避免在同一候选构建目录里静默混合两组软件环境。
