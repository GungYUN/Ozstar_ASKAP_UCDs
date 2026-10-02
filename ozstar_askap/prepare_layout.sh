#!/usr/bin/env bash
# Create the persistent directories used by planning, staging, and products.
# 创建规划、staging 和产物使用的持久化目录。
set -Eeuo pipefail

# Keep all visibility/MS data and state under the configured persistent root.
# 所有 visibility/MS 数据和状态都放在配置的持久化根目录下。
ROOT="${ASKAP_WORK:-/fred/oz299/qhuang/ASKAP-UCDs}"
# The Python configuration creates state/controllers on its first run.
# Python 配置在第一次运行时创建 state/controllers。
mkdir -p \
  "${ROOT}/catalogue" \
  "${ROOT}/staging" \
  "${ROOT}/products" \
  "${ROOT}/state/tap_cache" \
  "${ROOT}/state/sources" \
  "${ROOT}/state/jobs" \
  "${ROOT}/logs" \
  "${ROOT}/staging/_wsclean_tmp"

# Report the root only; this script does not download or process data.
# 只报告根目录；本脚本不下载也不处理数据。
printf 'ASKAP-UCDs layout ready at %s\n' "${ROOT}"
