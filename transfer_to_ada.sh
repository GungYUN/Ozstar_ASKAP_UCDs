#!/usr/bin/env bash
# Explicit Ozstar-to-ada product transfer wrapper; processing never calls it automatically.
# 显式的 Ozstar-to-ada 产物传输 wrapper；处理流程不会自动调用它。
set -Eeuo pipefail

# Resolve the Python implementation relative to this wrapper.
# 相对于 wrapper 位置解析 Python 实现。
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec python3 "${SCRIPT_DIR}/transfer_products.py" --direction to-ada "$@"
