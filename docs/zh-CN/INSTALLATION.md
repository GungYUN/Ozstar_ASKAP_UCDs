# 1. 安装

[索引](../../README.md) · [English](../en/INSTALLATION.md) · [下一步：使用](USAGE.md)

## 1. 环境要求

- Ozstar：Linux x86_64、Slurm、Apptainer、GNU tar、持久化 FRED 空间。
  登录节点查询和下载，计算 worker 不应依赖外网。
- 主机 controller：GCC 13.3.0 / Python 3.12.3 模块，以及 `numpy`、`pandas`、
  `astropy`、`astroquery`、`keyring`。
- 已验收 SIF：`/fred/oz299/qhuang/dstools/dstools-260925.sif`，包含 DStools dev、
  WSClean 3.5 和兼容的 casacore/CASA。新建 SIF 不是必经步骤，见[SIF 构建](SIF_BUILD.md)。
- 部署工作站：SSH/scp；部署 wrapper 另需 `sshpass`。
- Ada：Python 3.7+、SSH、rsync、sshpass、sha256sum。拉取工具仅依赖标准库，
  文件传输不要求 Ada 安装 DStools。

示例使用当前账户路径，其他账户需统一调整。代码放到
`ASKAP-UCDs-shared/ozstar_askap`，**新的生产数据**放到 `ASKAP-UCDs-production`。
这一显式设置覆盖代码默认的 `/fred/oz299/qhuang/ASKAP-UCDs`，避免混入旧项目或
测试状态；不搬移现有数据。

## 2. 从 Mac 部署到 Ozstar

在完整仓库中执行：

```bash
cd /Users/qhua0119/Documents/ChatGPT/Ozstar/Ozstar_ASKAP_UCDs/ozstar_askap
OZSTAR_WORK=/fred/oz299/qhuang/ASKAP-UCDs-shared bash deploy_to_ozstar.sh
```

没有凭据文件或环境覆盖时，脚本提示隐藏输入 Ozstar 密码，不保存此次输入。
已有密码文件必须为600权限。脚本上传本程序目录和父目录的单源下载程序，
不上传外部 catalogue 或运行数据。本地数据与密码请放在代码目录之外。
不要覆盖正在运行作业使用的代码；`wsclean_slot/` 必须一并部署并保留执行权限。

部署选项汇总于[使用第1节](USAGE.md#1-会话环境与公共设置)。
`OZSTAR_WORK` 是部署根目录，`ASKAP_WORK` 是运行数据根目录。

## 3. 输入表与已验收 SIF

使用现有 catalogue，或提供等价文件并调整路径：

```text
/fred/oz299/qhuang/ASKAP-UCDs/catalogue/UltracoolSheet_Main_index_unbinaryUCD.csv
/fred/oz299/qhuang/ASKAP-UCDs/catalogue/All_combine_data_unrepetition.csv
```

源表必须包含 `UCS`、`UCS_name`、`ra_j2000_formula`、`dec_j2000_formula`、
`pmra_formula`、`pmdec_formula`、`sptnumabs_formula`。不要直接将仓库中的旧表
快照作为当前生产表。历元 CSV 在 CASDA 缺少有效 `t_min` 时作为后备。

在 Ozstar 登录节点执行：

```bash
module load apptainer
sha256sum -c /fred/oz299/qhuang/dstools/dstools-260925.sif.sha256
apptainer exec --cleanenv --no-home \
  /fred/oz299/qhuang/dstools/dstools-260925.sif \
  /opt/wsclean/bin/wsclean --version
```

预期 hash 校验通过并显示 WSClean 3.5。仅更新主机脚本时不需要更换已验收 SIF。

## 4. 主机 Python 与运行目录

先检查已有 controller 解释器，不主动升级：

```bash
module load gcc/13.3.0
module load python/3.12.3
/fred/oz299/qhuang/ASKAP-UCDs/.venv/askap-python/bin/python -c \
  'import numpy,pandas,astropy,astroquery,keyring; print("Host Python OK")'
```

仅在需要新建主机环境时执行：

```bash
cd /fred/oz299/qhuang/ASKAP-UCDs-shared/ozstar_askap
ASKAP_WORK=/fred/oz299/qhuang/ASKAP-UCDs-production bash setup_host_python.sh
```

脚本安装主机依赖，不安装 DStools/casacore/WSClean，仅加载模块、不 purge/unload。
如果创建了新环境，请在使用指南中改用它打印的解释器路径，而非示例中的已有环境。

创建持久化目录：

```bash
cd /fred/oz299/qhuang/ASKAP-UCDs-shared/ozstar_askap
ASKAP_WORK=/fred/oz299/qhuang/ASKAP-UCDs-production bash prepare_layout.sh
```

除 `df` 外，还需检查个人/项目容量与 inode 配额。MS、模型副本、检查点和
WSClean 临时数据均在 FRED，Slurm tmp 仅作小型 scratch。

## 5. Ozstar 的 CASDA 凭据

在 controller 的 shell 中隐藏输入 CASDA 密码：

```bash
read -r -s -p "CASDA password: " CASDA_PASSWORD
printf '\n'
export CASDA_PASSWORD
```

默认用户名为 `qhua0119@uni.sydney.edu.au`。也可设置 `CASDA_PASSWORD_FILE`，
指向受保护、非符号链接、仅包含密码的文件；默认是
`$HOME/.config/askap/casda-password`。不要将密码写入代码、命令参数、Git 或日志，
处理凭据时不要启用 `set -x`。只有登录节点需要 CASDA 凭据。仅通过环境保存的密码
在重连后需重新输入，另一台主机上的 tmux 不会恢复当前 shell。

## 6. 在 Ada 安装拉取脚本

在 Ada 获取已部署脚本，SSH 提示输入 Ozstar 密码：

```bash
ADA_ROOT=/import/ada1/qhua0119/Combine_ImagePlot/Ozstar
mkdir -p "$ADA_ROOT/bin"
scp -4 \
  qhuang@ozstar.swin.edu.au:/fred/oz299/qhuang/ASKAP-UCDs-shared/ozstar_askap/pull_askap_products.py \
  "$ADA_ROOT/bin/"
cd "$ADA_ROOT/bin"
python3 -m py_compile pull_askap_products.py
python3 pull_askap_products.py --help
sha256sum pull_askap_products.py
```

将脚本 hash 与工作站/Ozstar 副本比较。rsync 命令应含 `--ignore-times`，用于强制
传输已按 hash 选出的待修复文件。由 Ada 连接 Ozstar，不进行反向推送。
可使用 SSH key、已有的600权限 `$ADA_ROOT/state/ozstar-transfer-password` 文件，
或按[使用第6节](USAGE.md#6-在-ada-拉取全部完整产物)隐藏输入密码。

## 7. 后续操作

按[使用](USAGE.md)恢复环境、建立索引、提交小范围生产任务并拉取输出。
不要将测试数据库、故障配置或 controller 计划复制到生产目录。
