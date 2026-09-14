#!/usr/bin/env bash
# Build the persistent host-side orchestration Python environment.
# 创建持久化的主机侧 orchestration Python 环境。
set -Eeuo pipefail

# Keep orchestration on persistent Fred space; MS data still never goes to TMPDIR.
# orchestration 放在持久化 Fred 空间；MS 数据仍然不放入 TMPDIR。
# This script only loads modules: it never purges or unloads existing modules.
# 本脚本只加载 module：绝不执行 module purge 或 module unload。
# DStools/casacore/WSClean remain inside the Apptainer image.
# DStools/casacore/WSClean 保持在 Apptainer 镜像内。
ROOT="${ASKAP_WORK:-/fred/oz299/qhuang/ASKAP-UCDs}"
ENV_PREFIX="${ASKAP_PYTHON_ENV:-${ROOT}/.venv/askap-python}"
PYTHON_PARENT_MODULE="${ASKAP_PYTHON_PARENT_MODULE:-gcc/13.3.0}"
PYTHON_MODULE="${ASKAP_PYTHON_MODULE:-python/3.12.3}"

module load "${PYTHON_PARENT_MODULE}"
module load "${PYTHON_MODULE}"

# Create a venv with site packages, then install the declared host dependencies.
# 创建带 site packages 的 venv，再安装声明的主机依赖。
if [[ ! -x "${ENV_PREFIX}/bin/python" ]]; then
  mkdir -p "${ROOT}/.venv"
  python -m venv --system-site-packages "${ENV_PREFIX}"
elif ! "${ENV_PREFIX}/bin/python" -c \
  'import astropy, astroquery, h5py, keyring, numpy, pandas' >/dev/null 2>&1; then
  # pip below repairs an existing but incomplete environment.
  # 下面的 pip 会修复已存在但不完整的环境。
  true
fi

"${ENV_PREFIX}/bin/python" -m pip install --disable-pip-version-check \
  --no-cache-dir numpy pandas astropy h5py astroquery keyring
"${ENV_PREFIX}/bin/python" -c \
  'import astropy, astroquery, h5py, keyring, numpy, pandas; print("ASKAP host Python environment is ready")'
# Print the environment exports needed by the controller and generated worker.
# 打印 controller 和生成 worker 所需的环境变量导出语句。
printf 'Use this interpreter before planning jobs:\n'
printf 'module load %s\n' "${PYTHON_PARENT_MODULE}"
printf 'module load %s\n' "${PYTHON_MODULE}"
printf 'export ASKAP_PYTHON_MODULE=%s\n' "${PYTHON_MODULE}"
printf 'export ASKAP_PYTHON_BIN=%s\n' "${ENV_PREFIX}/bin/python"
