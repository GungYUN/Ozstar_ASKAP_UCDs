#!/usr/bin/env bash
# Explicit ada-to-Ozstar transfer wrapper; it does not run during processing.
# 显式的 ada-to-Ozstar 传输 wrapper；处理流程不会自动运行它。
set -Eeuo pipefail

# Resolve the Python implementation relative to this wrapper.
# 相对于 wrapper 位置解析 Python 实现。
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec python3 "${SCRIPT_DIR}/transfer_products.py" --direction to-ozstar "$@"
