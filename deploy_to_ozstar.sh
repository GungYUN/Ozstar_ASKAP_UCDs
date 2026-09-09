#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REMOTE_USER="${OZSTAR_SSH_USER:-qhuang}"
REMOTE_HOST="${OZSTAR_SSH_HOST:-ozstar.swin.edu.au}"
REMOTE_ROOT="${OZSTAR_WORK:-/fred/oz299/qhuang/ASKAP-UCDs}"
PASSWORD_FILE="${OZSTAR_PASSWORD_FILE:-${TRANSFER_PASSWORD_FILE:-$HOME/.config/askap/transfer-password}}"

[[ -f "${PASSWORD_FILE}" ]] || {
  printf 'Password file not found: %s\n' "${PASSWORD_FILE}" >&2
  exit 1
}
[[ "$(stat -f '%Lp' "${PASSWORD_FILE}" 2>/dev/null || stat -c '%a' "${PASSWORD_FILE}")" == "600" ]] || {
  printf 'Password file must have mode 600: %s\n' "${PASSWORD_FILE}" >&2
  exit 1
}
command -v sshpass >/dev/null || {
  printf 'sshpass is required on the machine running this script\n' >&2
  exit 1
}

sshpass -f "${PASSWORD_FILE}" ssh -o ConnectTimeout=30 \
  "${REMOTE_USER}@${REMOTE_HOST}" "mkdir -p -- '${REMOTE_ROOT}'"
sshpass -f "${PASSWORD_FILE}" scp -r -p -o ConnectTimeout=30 \
  "${SCRIPT_DIR}" "${REMOTE_USER}@${REMOTE_HOST}:${REMOTE_ROOT}/"
