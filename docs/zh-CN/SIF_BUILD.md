# 4. SIF 构建（可选）

[索引](../../README.md) · [English](../en/SIF_BUILD.md) · [安装](INSTALLATION.md)

日常处理使用已验收 `dstools-260925.sif`，没有构建替代镜像的需求就跳过本页。
更新主机脚本不需要重建。新镜像作为独立 candidate，不原地修改已验收镜像。

## 1. 构建输入与权限

配方位于 [`New_dstools_build/`](../../New_dstools_build/)，固定 DStools **dev快照**、
Python 3.12、WSClean 3.5、casacore/python-casacore 3.7.1、NumPy 2.2.6。
精确 Python/Boost 依赖求解与产物 hash 在准备阶段冻结，不自动跟随上游最新commit。

| 配方输入 | 作用 |
| --- | --- |
| `versions.env` | 源URL/commit、dev快照hash、模块、依赖与构建根目录 |
| `prep_sources.sh` | 登录节点联网准备、锁与输入清单 |
| `askap-dstools.def` | 离线容器定义 |
| `build_sif.sbatch` | Slurm离线编译/构建与普通用户检查 |
| `patches/0001`–`0006` | visibility weighting、integrations、Boost.System、Radler离线依赖、ABI一致性、子进程处理 |

上传**整个配方**，不能只传四个顶层文件。仓库不内置上游dev快照，必须提供与
`versions.env` 相符的精确快照。缺补丁或源hash不符时停止准备，不要编造hash来绕过。

不在主机使用root/sudo安装，Apptainer使用站点支持的 `--fakeroot`。若不可用，停止
并向站点确认支持的构建方式，不要求或假定自己具有额外权限。登录节点联网准备，
计算节点完全离线编译。持久化输入、缓存、配置、日志、镜像均位于
`/fred/oz299/qhuang/dstools/` 下；仅Slurm分配的临时盘为临时存储。

## 2. 选择新构建目录并上传

版本/源码/依赖求解变化时使用新根目录。示例为
`/fred/oz299/qhuang/dstools/sif-build-20261013-offline-v1`；如果已有另一组冻结构建，
不要复用该目录。

上传前在本地编辑配方：

```bash
cd /Users/qhua0119/Documents/ChatGPT/Ozstar/Ozstar_ASKAP_UCDs/New_dstools_build
nano versions.env
nano build_sif.sbatch
```

**两个文件**中的 `BUILD_ROOT` 都改为新根目录，sbatch 的 `--output`/`--error`
改为其 `logs/` 路径。检查 `CONTROLLER_PYTHON_BIN` 和模块名，已有主机解释器需要
GCC/Python模块。除非明确制作一致的新固定版本配方，否则保留全部源码版本锁定。

在Mac执行，SSH/rsync会提示输入Ozstar密码：

```bash
BUILD_ROOT=/fred/oz299/qhuang/dstools/sif-build-20261013-offline-v1
ssh qhuang@ozstar.swin.edu.au \
  "mkdir -p '$BUILD_ROOT/recipe' '$BUILD_ROOT/src/dstools-dev' '$BUILD_ROOT/logs'"
rsync -av --exclude='.DS_Store' \
  /Users/qhua0119/Documents/ChatGPT/Ozstar/Ozstar_ASKAP_UCDs/New_dstools_build/ \
  "qhuang@ozstar.swin.edu.au:$BUILD_ROOT/recipe/"
rsync -av --exclude='__pycache__' --exclude='*.pyc' --exclude='.DS_Store' --exclude='.venv' \
  /Users/qhua0119/Documents/ChatGPT/DStools/dstools-dev/ \
  "qhuang@ozstar.swin.edu.au:$BUILD_ROOT/src/dstools-dev/"
```

这只复制到新构建树，不修改主机环境或已验收SIF。上面的源码目录必须包含精确
选定的上游快照。

## 3. 登录节点准备

在Ozstar登录节点的tmux中执行：

```bash
BUILD_ROOT=/fred/oz299/qhuang/dstools/sif-build-20261013-offline-v1
cd "$BUILD_ROOT/recipe"
set -o pipefail
bash prep_sources.sh 2>&1 | tee "$BUILD_ROOT/logs/preparation.log"
prep_rc=${PIPESTATUS[0]}
echo "Preparation exit code: $prep_rc"
```

仅在退出0且明确显示准备完成后继续。准备阶段冻结bootstrap SIF、源码/submodules
与补丁、完整本地APT仓库、wheelhouse/合并Python锁、可重定位Python/Boost、
CASA/casacore measures、Astropy台址表和WSClean FetchContent依赖。全部缓存和
准备容器的home映射均在构建目录内，不向真实主机系统安装包。

任何授权配方编辑后，先重新运行准备，再校验/提交：

```bash
sha256sum -c --quiet "$BUILD_ROOT/manifests/build-input-files.sha256"
```

不能忽略hash不符，或手动修改清单来认可未知输入。sbatch脚本本身也被冻结，
修改资源会改变它的hash。

## 4. 计算节点离线构建

准备成功后，从登录节点执行：

```bash
cd "$BUILD_ROOT/recipe"
sbatch build_sif.sbatch
```

仓库脚本申请8CPU、32G内存、32G tmp、12小时。编译和镜像组装应在计算资源进行，
不是登录节点。仅使用本地deb仓库、离线wheel、源码/数据归档和打包Python环境，
不需要计算节点联网。

也可选择交互式构建，申请指令**固定为**：

```bash
sinteractive --time=02:00:00 --ntasks=1 --cpus-per-task=8 --mem=32G --tmp=32G
```

在分配到的节点执行：

```bash
BUILD_ROOT=/fred/oz299/qhuang/dstools/sif-build-20261013-offline-v1
build_interactive() {
  test -n "${JOBFS:-}" || { echo "JOBFS is required"; return 1; }
  cd "$BUILD_ROOT/recipe" || return 1
  set -o pipefail
  SLURM_TMPDIR="$JOBFS" TMPDIR="$JOBFS" bash build_sif.sbatch 2>&1 \
    | tee "$BUILD_ROOT/logs/build-interactive-${SLURM_JOB_ID}.log"
  local rc=${PIPESTATUS[0]}
  echo "Build exit code: $rc"
  return "$rc"
}
if build_interactive; then
  echo "Build command: PASS"
else
  echo "Build command: FAIL; keep the logs and stop."
fi
```

通过Bash运行时 `#SBATCH` 头部只是注释，实际受交互分配的2小时限制，而非脚本的
12小时请求。`JOBFS` 将站点scratch路径传给Apptainer临时设置，不搬移MS/科学产物。

## 5. 候选镜像与采用

镜像/日志位于 `artifacts/` 和 `logs/`，`manifests/candidate-sif.sha256` 指定候选。
文件名包含源码/输入指纹，构建拒绝覆盖已有候选。普通用户导入和子进程检查是
构建的一部分，检查失败不是构建成功。

```bash
module load apptainer
sha256sum -c "$BUILD_ROOT/manifests/candidate-sif.sha256"
CANDIDATE=$(awk 'NR==1 {print $2}' "$BUILD_ROOT/manifests/candidate-sif.sha256")
apptainer exec --cleanenv --no-home "$CANDIDATE" \
  /opt/askap/bin/python -c 'import casacore.tables; from dstools.utils import LOCATIONS; print(sorted(LOCATIONS))'
apptainer exec --cleanenv --no-home "$CANDIDATE" \
  /opt/askap/bin/python -c 'from casatasks import tclean,phaseshift,uvsub,mstransform; print("CASA OK")'
apptainer exec --cleanenv --no-home "$CANDIDATE" /opt/wsclean/bin/wsclean --version
```

python-casacore与CASA保持**独立进程**导入。OpenBLAS单线程不关闭WSClean自身
线程。共用casacore、一致的C++/OpenSSL库、可读离线运行数据、portable CPU选项
都是有意保留的正确性/兼容设置。

新构建通过内部检查后，采用前仍需隔离的真实MS读取与端到端处理检查；仅阅读
本指南不要求重复执行旧镜像已经通过的测试。保留已验收SIF，将验证替代镜像
复制为另一个版本名，核对副本hash，再为新计划显式设置 `DSTOOLS_CONTAINER`。
不要更换活跃计划的镜像。
