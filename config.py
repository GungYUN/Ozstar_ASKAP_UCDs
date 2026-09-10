"""Central configuration for the Ozstar ASKAP batch pipeline.

The same file is used on the Ozstar login node and on compute nodes.  Keep
site-specific paths here or override them with environment variables.  No
password is stored in this module.
"""

from __future__ import annotations

import os
from pathlib import Path


def _path_from_env(name: str, default: Path) -> Path:
    return Path(os.environ.get(name, str(default))).expanduser()


# The persistent Ozstar work area.  Do not move visibility/MS data to $TMPDIR.
WORK = _path_from_env(
    "ASKAP_WORK",
    Path("/fred/oz299/qhuang/ASKAP-UCDs"),
)
PROGRAM_DIR = Path(__file__).resolve().parent
STAGING_ROOT = WORK / "staging"
PRODUCT_ROOT = WORK / "products"
STATE_ROOT = WORK / "state"
LOG_ROOT = WORK / "logs"
WSCLEAN_TEMP_ROOT = STAGING_ROOT / "_wsclean_tmp"

# Catalogues should be copied to WORK/catalogue, or set these to the mounted
# catalogue locations on Ozstar.
CATALOGUE_PATH = _path_from_env(
    "ASKAP_CATALOGUE",
    WORK / "catalogue/UltracoolSheet_Main_index_unbinaryUCD.csv",
)
TIME_CSV_PATH = _path_from_env(
    "ASKAP_TIME_CSV",
    WORK / "catalogue/All_combine_data_unrepetition.csv",
)

# User-controlled processing range.  The CLI can override these values, but a
# normal production run only requires changing these two integers.
UCS_START = int(os.environ.get("ASKAP_UCS_START", "1"))
UCS_END = int(os.environ.get("ASKAP_UCS_END", "1000"))

# Batch packing and resource controls.
HOURS_PER_OBS = float(os.environ.get("ASKAP_HOURS_PER_OBS", "4.0"))
SOURCE_OVERHEAD_HOURS = float(os.environ.get("ASKAP_SOURCE_OVERHEAD_HOURS", "0.5"))
PARALLEL_SLOTS = int(os.environ.get("ASKAP_PARALLEL_SLOTS", "4"))
CPU_PER_SLOT = int(os.environ.get("ASKAP_CPU_PER_SLOT", "8"))
JOB_WALLTIME_HOURS = float(os.environ.get("ASKAP_JOB_WALLTIME_HOURS", "48"))
SAFETY_FACTOR = float(os.environ.get("ASKAP_SAFETY_FACTOR", "0.85"))
TARGET_OBS = int(os.environ.get("ASKAP_TARGET_OBS", "40"))
MAX_SOURCES_PER_JOB = int(os.environ.get("ASKAP_MAX_SOURCES_PER_JOB", "20"))
SCAN_WINDOW = int(os.environ.get("ASKAP_SCAN_WINDOW", "50"))
WALLTIME_RESERVE_MINUTES = int(
    os.environ.get("ASKAP_WALLTIME_RESERVE_MINUTES", "20")
)

# ASKAP/DStools processing settings.  These match the original ada workflow.
CONTAINER = _path_from_env(
    "DSTOOLS_CONTAINER",
    Path("/fred/oz299/qhuang/dstools/dstools-v2.0.0.sif"),
)
APPTAINER_BIN = os.environ.get("APPTAINER_BIN", "apptainer")
BAND = os.environ.get("ASKAP_DSTOOLS_BAND", "AK_low")
CREATE_MODEL_ITERATIONS = int(
    os.environ.get("ASKAP_CREATE_MODEL_ITERATIONS", "50000")
)
CREATE_MODEL_THREADS = int(
    os.environ.get("ASKAP_CREATE_MODEL_THREADS", str(CPU_PER_SLOT))
)
MIN_UV_METRES = float(os.environ.get("ASKAP_MIN_UV_METRES", "0"))
EXTRACT_MIN_UV_METRES = float(
    os.environ.get("ASKAP_EXTRACT_MIN_UV_METRES", "500")
)
CROP_ARCMIN = float(os.environ.get("ASKAP_CROP_ARCMIN", "10"))
DEFAULT_T_MIN = float(os.environ.get("ASKAP_DEFAULT_T_MIN", "60800"))
MJD_J2000 = float(os.environ.get("ASKAP_MJD_J2000", "51545"))
SPTNUM_THRESHOLD = float(os.environ.get("ASKAP_SPTNUM_THRESHOLD", "20"))
CASDA_MAX_FILE_SIZE_KB = float(
    os.environ.get("ASKAP_CASDA_MAX_FILE_SIZE_KB", str(50 * 1024 * 1024))
)
CASDA_STAGE_BATCH_SIZE = int(os.environ.get("ASKAP_CASDA_STAGE_BATCH_SIZE", "20"))
CASDA_QUERY_LIMIT = int(os.environ.get("ASKAP_CASDA_QUERY_LIMIT", "5000"))

# Slurm settings.  --tmp is only a small scratch allocation; the persistent
# ASKAP-UCDs tree stores all MS, WSClean products, checkpoints and logs.
SBATCH_MEM = os.environ.get("ASKAP_SBATCH_MEM", "80G")
SBATCH_TMP = os.environ.get("ASKAP_SBATCH_TMP", "10G")
SBATCH_PARTITION = os.environ.get("ASKAP_SBATCH_PARTITION", "")
PYTHON_BIN = os.environ.get("ASKAP_PYTHON_BIN", "python3")
PYTHON_PARENT_MODULE = os.environ.get(
    "ASKAP_PYTHON_PARENT_MODULE", "gcc/13.3.0"
)
PYTHON_MODULE = os.environ.get("ASKAP_PYTHON_MODULE", "python/3.12.3")

# CASDA credentials are read only at runtime.  Prefer a protected file over an
# environment variable.  The password file must contain only the password.
CASDA_USERNAME = os.environ.get(
    "CASDA_USERNAME", "qhua0119@uni.sydney.edu.au"
)
CASDA_PASSWORD_FILE = _path_from_env(
    "CASDA_PASSWORD_FILE",
    Path("~/.config/askap/casda-password"),
)
CASDA_PASSWORD_ENV = "CASDA_PASSWORD"

# SSH/SCP transfer settings.  Passwords are never embedded in this file.
ADA_SSH_USER = os.environ.get("ADA_SSH_USER", "qhua0119")
ADA_SSH_HOST = os.environ.get("ADA_SSH_HOST", "ada.physics.usyd.edu.au")
ADA_VISIBILITY_ROOT = os.environ.get(
    "ADA_VISIBILITY_ROOT", "/import/ada1/qhua0119/Visibility"
)
OZSTAR_SSH_USER = os.environ.get("OZSTAR_SSH_USER", "qhuang")
OZSTAR_SSH_HOST = os.environ.get("OZSTAR_SSH_HOST", "ozstar.swin.edu.au")
OZSTAR_PRODUCT_ROOT = os.environ.get(
    "OZSTAR_PRODUCT_ROOT", str(PRODUCT_ROOT)
)
TRANSFER_PASSWORD_FILE = _path_from_env(
    "TRANSFER_PASSWORD_FILE",
    Path("~/.config/askap/transfer-password"),
)
ADA_PASSWORD_FILE = _path_from_env(
    "ADA_PASSWORD_FILE", TRANSFER_PASSWORD_FILE
)
OZSTAR_PASSWORD_FILE = _path_from_env(
    "OZSTAR_PASSWORD_FILE", TRANSFER_PASSWORD_FILE
)
SSHPASS_BIN = os.environ.get("SSHPASS_BIN", "sshpass")
SSH_CONNECT_TIMEOUT = int(os.environ.get("ASKAP_SSH_CONNECT_TIMEOUT", "30"))


def batch_name(ucs_number: int) -> str:
    """Return the ada-compatible 50-source batch name for a UCS number."""

    start = ((int(ucs_number) - 1) // 50) * 50 + 1
    return f"UCS{start}-{start + 49}"


def ensure_directories() -> None:
    """Create persistent pipeline directories if they do not exist."""

    if PARALLEL_SLOTS * CPU_PER_SLOT > 32:
        raise ValueError(
            "PARALLEL_SLOTS * CPU_PER_SLOT must not exceed Ozstar's 32 CPU limit"
        )

    for path in (
        WORK,
        STAGING_ROOT,
        PRODUCT_ROOT,
        STATE_ROOT,
        LOG_ROOT,
        WSCLEAN_TEMP_ROOT,
        STATE_ROOT / "tap_cache",
        STATE_ROOT / "sources",
        STATE_ROOT / "jobs",
    ):
        path.mkdir(parents=True, exist_ok=True)
