"""Central configuration for the Ozstar ASKAP batch pipeline.

English: The same configuration is imported on the Ozstar login node and on
compute nodes. Site-specific paths and runtime controls are defined here and
may be overridden with environment variables. Password values are never
stored in this module.

中文：登录节点和计算节点都会导入同一个配置。站点路径和运行控制项在此定义，
也可以用环境变量覆盖。本模块绝不保存密码值。
"""

from __future__ import annotations

import os
from pathlib import Path


def _path_from_env(name: str, default: Path) -> Path:
    """Resolve an environment-overridable path.

    English: Expand a user home marker after selecting the environment value
    or the persistent-site default.

    中文：先选择环境变量或持久化站点默认值，再展开用户 home 目录标记。
    """

    return Path(os.environ.get(name, str(default))).expanduser()


# Persistent Ozstar work area; visibility/MS data must not move to $TMPDIR.
# 持久化的 Ozstar 工作区；visibility/MS 数据不得移动到 $TMPDIR。
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

# Copy catalogues to WORK/catalogue, or point these values at mounted files.
# catalogue 可以复制到 WORK/catalogue，也可以指向 Ozstar 上的挂载文件。
CATALOGUE_PATH = _path_from_env(
    "ASKAP_CATALOGUE",
    WORK / "catalogue/UltracoolSheet_Main_index_unbinaryUCD.csv",
)
TIME_CSV_PATH = _path_from_env(
    "ASKAP_TIME_CSV",
    WORK / "catalogue/All_combine_data_unrepetition.csv",
)

# Inclusive user-controlled range; CLI arguments take precedence.
# 这是包含端点的用户范围；CLI 参数优先于这里的环境默认值。
UCS_START = int(os.environ.get("ASKAP_UCS_START", "1"))
UCS_END = int(os.environ.get("ASKAP_UCS_END", "1000"))

# Batch packing and resource controls.
# block packing 和资源控制项。
HOURS_PER_OBS = float(os.environ.get("ASKAP_HOURS_PER_OBS", "3.0"))
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
# The controller deliberately caps every block at 50 UCS numbers.
# controller 有意将每个 block 限制为最多 50 个 UCS 编号。
BLOCK_SIZE = 50

# CASDA reports access_estsize in KB. This is an estimated archive staging
# budget, not a quota override; live WORK/staging checks are also required.
# CASDA 的 access_estsize 单位是 KB。这是 archive staging 的估计预算，不是配额
# 覆盖；下载 block 前仍必须检查 WORK/staging 的实时状态。
STAGING_BUDGET_BYTES = int(
    os.environ.get("ASKAP_STAGING_BUDGET_BYTES", str(2 * 1024**4))
)
CONTROLLER_INITIAL_DELAY_SECONDS = float(
    os.environ.get("ASKAP_CONTROLLER_INITIAL_DELAY_SECONDS", "30")
)
CONTROLLER_POLL_INTERVAL_SECONDS = float(
    os.environ.get("ASKAP_CONTROLLER_POLL_INTERVAL_SECONDS", "60")
)
MAX_AUTO_RESUBMITS = int(os.environ.get("ASKAP_MAX_AUTO_RESUBMITS", "20"))
PARTIAL_EXIT_CODE = int(os.environ.get("ASKAP_PARTIAL_EXIT_CODE", "75"))

# ASKAP/DStools processing settings; these match the original ada workflow.
# ASKAP/DStools 处理设置；这些设置与原 ada 流程保持一致。
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
# MJD for 2026-01-01 12:00:00 UTC. Used only after CASDA and legacy CSV values
# are unavailable or non-finite.
# 对应 2026-01-01 12:00:00 UTC 的 MJD；仅在 CASDA 和旧 CSV 都没有有限值时使用。
DEFAULT_T_MIN = float(os.environ.get("ASKAP_DEFAULT_T_MIN", "61041.5"))
MJD_J2000 = float(os.environ.get("ASKAP_MJD_J2000", "51545"))
SPTNUM_THRESHOLD = float(os.environ.get("ASKAP_SPTNUM_THRESHOLD", "20"))
CASDA_MAX_FILE_SIZE_KB = float(
    os.environ.get("ASKAP_CASDA_MAX_FILE_SIZE_KB", str(50 * 1024 * 1024))
)
CASDA_STAGE_BATCH_SIZE = int(os.environ.get("ASKAP_CASDA_STAGE_BATCH_SIZE", "20"))
CASDA_QUERY_LIMIT = int(os.environ.get("ASKAP_CASDA_QUERY_LIMIT", "5000"))
CASDA_SOURCE_DOWNLOAD_RETRIES = int(
    os.environ.get("ASKAP_CASDA_SOURCE_DOWNLOAD_RETRIES", "3")
)
CASDA_SOURCE_RETRY_DELAY_SECONDS = float(
    os.environ.get("ASKAP_CASDA_SOURCE_RETRY_DELAY_SECONDS", "30")
)

# Slurm settings. --tmp is only small scratch; persistent WORK stores MS,
# WSClean products, checkpoints, and logs.
# Slurm 设置。--tmp 只是小型 scratch；持久化 WORK 保存 MS、WSClean 产物、checkpoint
# 和日志。
SBATCH_MEM = os.environ.get("ASKAP_SBATCH_MEM", "80G")
SBATCH_TMP = os.environ.get("ASKAP_SBATCH_TMP", "10G")
SBATCH_PARTITION = os.environ.get("ASKAP_SBATCH_PARTITION", "")
PYTHON_BIN = os.environ.get("ASKAP_PYTHON_BIN", "python3")
PYTHON_PARENT_MODULE = os.environ.get(
    "ASKAP_PYTHON_PARENT_MODULE", "gcc/13.3.0"
)
PYTHON_MODULE = os.environ.get("ASKAP_PYTHON_MODULE", "python/3.12.3")

# CASDA credentials are read only at runtime. Prefer a protected file; the
# environment variable is a one-process override. The file contains only the password.
# CASDA 凭据只在运行时读取。优先使用受保护文件；环境变量只覆盖当前进程。
# 密码文件只能包含密码本身。
CASDA_USERNAME = os.environ.get(
    "CASDA_USERNAME", "qhua0119@uni.sydney.edu.au"
)
CASDA_PASSWORD_FILE = _path_from_env(
    "CASDA_PASSWORD_FILE",
    Path("~/.config/askap/casda-password"),
)
CASDA_PASSWORD_ENV = "CASDA_PASSWORD"

# SSH/SCP transfer settings; password values are never embedded here.
# SSH/SCP 传输设置；这里绝不嵌入密码值。
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
    """Return the ada-compatible 50-source batch name for a UCS number.

    English: This preserves the original ada directory naming even when the
    controller splits a block earlier because of storage.

    中文：即使 controller 因存储限制提前拆分 block，也保留原 ada 目录命名。
    """

    start = ((int(ucs_number) - 1) // 50) * 50 + 1
    return f"UCS{start}-{start + 49}"


def ensure_directories() -> None:
    """Create persistent pipeline directories if they do not exist.

    English: Also enforce Ozstar's 32-CPU worker limit before any run starts.

    中文：在运行开始前同时检查 Ozstar 的 32 CPU worker 上限。
    """

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
        STATE_ROOT / "controllers",
    ):
        path.mkdir(parents=True, exist_ok=True)
