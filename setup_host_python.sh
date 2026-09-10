#!/usr/bin/env bash
set -Eeuo pipefail

# Keep the host-side orchestration environment on the persistent Fred space.
# This script only loads modules; it never purges or unloads existing modules.
# DStools/casacore/WSClean remain inside the Apptainer image.
ROOT="${ASKAP_WORK:-/fred/oz299/qhuang/ASKAP-UCDs}"
ENV_PREFIX="${ASKAP_PYTHON_ENV:-${ROOT}/.venv/askap-python}"
PYTHON_PARENT_MODULE="${ASKAP_PYTHON_PARENT_MODULE:-gcc/13.3.0}"
PYTHON_MODULE="${ASKAP_PYTHON_MODULE:-python/3.12.3}"

module load "${PYTHON_PARENT_MODULE}"
module load "${PYTHON_MODULE}"

if [[ ! -x "${ENV_PREFIX}/bin/python" ]]; then
  mkdir -p "${ROOT}/.venv"
  python -m venv --system-site-packages "${ENV_PREFIX}"
elif ! "${ENV_PREFIX}/bin/python" -c \
  'import astropy, astroquery, h5py, numpy, pandas' >/dev/null 2>&1; then
  true
fi

"${ENV_PREFIX}/bin/python" -m pip install --disable-pip-version-check \
  --no-cache-dir numpy pandas astropy h5py astroquery
"${ENV_PREFIX}/bin/python" -c \
  'import astropy, astroquery, h5py, numpy, pandas; print("ASKAP host Python environment is ready")'
printf 'Use this interpreter before planning jobs:\n'
printf 'module load %s\n' "${PYTHON_PARENT_MODULE}"
printf 'module load %s\n' "${PYTHON_MODULE}"
printf 'export ASKAP_PYTHON_MODULE=%s\n' "${PYTHON_MODULE}"
printf 'export ASKAP_PYTHON_BIN=%s\n' "${ENV_PREFIX}/bin/python"
