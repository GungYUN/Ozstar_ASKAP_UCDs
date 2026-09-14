#!/usr/bin/env bash
# Deploy the complete program directory to Ozstar without embedding a password.
# 将整个程序目录部署到 Ozstar，且不在脚本中嵌入密码。
set -Eeuo pipefail

# Resolve paths from the wrapper location so it can be launched elsewhere.
# 根据 wrapper 所在位置解析路径，因此可以从其他目录启动。
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REMOTE_USER="${OZSTAR_SSH_USER:-qhuang}"
REMOTE_HOST="${OZSTAR_SSH_HOST:-ozstar.swin.edu.au}"
REMOTE_ROOT="${OZSTAR_WORK:-/fred/oz299/qhuang/ASKAP-UCDs}"
PASSWORD_FILE="${OZSTAR_PASSWORD_FILE:-${TRANSFER_PASSWORD_FILE:-$HOME/.config/askap/transfer-password}}"
SSHPASS_ARGS=()

# Prefer a one-process environment value; otherwise require a mode-600 file.
# 优先使用当前进程环境值，否则要求 mode-600 密码文件。
if [[ -n "${TRANSFER_PASSWORD:-}" ]]; then
  export SSHPASS="${TRANSFER_PASSWORD}"
  SSHPASS_ARGS=(-e)
else
  [[ -f "${PASSWORD_FILE}" ]] || {
    printf 'Password file not found: %s\n' "${PASSWORD_FILE}" >&2
    exit 1
  }
  [[ "$(stat -f '%Lp' "${PASSWORD_FILE}" 2>/dev/null || stat -c '%a' "${PASSWORD_FILE}")" == "600" ]] || {
    printf 'Password file must have mode 600: %s\n' "${PASSWORD_FILE}" >&2
    exit 1
  }
  SSHPASS_ARGS=(-f "${PASSWORD_FILE}")
fi
# sshpass is required only on the machine initiating deployment.
# sshpass 只需安装在发起部署的机器上。
command -v sshpass >/dev/null || {
  printf 'sshpass is required on the machine running this script\n' >&2
  exit 1
}

# Create the remote parent, then copy the program directory itself.
# 先创建远端父目录，再复制整个程序目录本身。
sshpass "${SSHPASS_ARGS[@]}" ssh -o ConnectTimeout=30 \
  "${REMOTE_USER}@${REMOTE_HOST}" "mkdir -p -- '${REMOTE_ROOT}'"
sshpass "${SSHPASS_ARGS[@]}" scp -r -p -o ConnectTimeout=30 \
  "${SCRIPT_DIR}" "${REMOTE_USER}@${REMOTE_HOST}:${REMOTE_ROOT}/"
