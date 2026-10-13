#!/usr/bin/env bash
# Deploy the complete program directory to Ozstar without embedding a password.
# 将整个程序目录部署到 Ozstar，且不在脚本中嵌入密码。
set -Eeuo pipefail

# Resolve paths from the wrapper location so it can be launched elsewhere.
# 根据 wrapper 所在位置解析路径，因此可以从其他目录启动。
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
STANDALONE_DOWNLOADER="${SCRIPT_DIR}/../Download_ASKAP_Visibility_Remote.py"
REMOTE_USER="${OZSTAR_SSH_USER:-qhuang}"
REMOTE_HOST="${OZSTAR_SSH_HOST:-ozstar.swin.edu.au}"
REMOTE_ROOT="${OZSTAR_WORK:-/fred/oz299/qhuang/ASKAP-UCDs}"
PASSWORD_FILE="${OZSTAR_PASSWORD_FILE:-${TRANSFER_PASSWORD_FILE:-$HOME/.config/askap/transfer-password}}"
SSHPASS_ARGS=()

# The regression suite compares the pipeline with the parent-level downloader.
# 回归测试需要根目录的单源下载程序；上传前确认它存在，避免部署遗漏。
[[ -f "${STANDALONE_DOWNLOADER}" ]] || {
  printf 'Standalone downloader not found: %s\nRun deployment from a complete project checkout.\n' "${STANDALONE_DOWNLOADER}" >&2
  exit 1
}

# sshpass is required only on the machine initiating deployment.
# sshpass 只需安装在发起部署的机器上。缺少依赖时不要先索取密码。
command -v sshpass >/dev/null || {
  printf 'sshpass is required on the machine running this script\n' >&2
  exit 1
}

# Prefer an environment value or a mode-600 file; otherwise prompt privately.
# 优先使用环境值或 mode-600 文件；两者都没有时，隐藏输入一次密码。
if [[ -n "${TRANSFER_PASSWORD:-}" ]]; then
  export SSHPASS="${TRANSFER_PASSWORD}"
  SSHPASS_ARGS=(-e)
elif [[ -f "${PASSWORD_FILE}" ]]; then
  [[ "$(stat -f '%Lp' "${PASSWORD_FILE}" 2>/dev/null || stat -c '%a' "${PASSWORD_FILE}")" == "600" ]] || {
    printf 'Password file must have mode 600: %s\n' "${PASSWORD_FILE}" >&2
    exit 1
  }
  SSHPASS_ARGS=(-f "${PASSWORD_FILE}")
else
  [[ -t 0 ]] || {
    printf 'No password file found; run in an interactive terminal to enter the Ozstar password, or set TRANSFER_PASSWORD.\n' >&2
    exit 1
  }
  read -r -s -p "Ozstar password for ${REMOTE_USER}@${REMOTE_HOST}: " SSHPASS
  printf '\n' >&2
  [[ -n "${SSHPASS}" ]] || {
    printf 'Password must not be empty.\n' >&2
    exit 1
  }
  export SSHPASS
  SSHPASS_ARGS=(-e)
fi
# The entered password is not saved to disk or printed in commands.
# 输入的密码不写入磁盘，不打印到命令中；退出时清除脚本进程的临时环境值。
trap 'unset SSHPASS' EXIT

# Create the remote parent, then copy the program directory itself.
# 先创建远端父目录，再复制整个程序目录本身。
sshpass "${SSHPASS_ARGS[@]}" ssh -o ConnectTimeout=30 \
  "${REMOTE_USER}@${REMOTE_HOST}" "mkdir -p -- '${REMOTE_ROOT}'"
sshpass "${SSHPASS_ARGS[@]}" scp -r -p -o ConnectTimeout=30 \
  "${SCRIPT_DIR}" "${REMOTE_USER}@${REMOTE_HOST}:${REMOTE_ROOT}/"
# Keep the expected project layout, without uploading data/catalogues/state.
# 保留测试需要的目录结构，不上传任何观测表、数据或运行状态。
sshpass "${SSHPASS_ARGS[@]}" scp -p -o ConnectTimeout=30 \
  "${STANDALONE_DOWNLOADER}" "${REMOTE_USER}@${REMOTE_HOST}:${REMOTE_ROOT}/"
