#!/usr/bin/env bash
set -Eeuo pipefail

ROOT="${ASKAP_WORK:-/fred/oz299/qhuang/ASKAP-UCDs}"
mkdir -p \
  "${ROOT}/catalogue" \
  "${ROOT}/staging" \
  "${ROOT}/products" \
  "${ROOT}/state/tap_cache" \
  "${ROOT}/state/sources" \
  "${ROOT}/state/jobs" \
  "${ROOT}/logs" \
  "${ROOT}/staging/_wsclean_tmp"

printf 'ASKAP-UCDs layout ready at %s\n' "${ROOT}"
