#!/usr/bin/env bash
# Ozstar cannot open SSH connections to ada.  The supported transfer direction
# is therefore an ada-initiated pull; fail clearly instead of timing out.
# Ozstar 无法主动 SSH 连接 ada；受支持方式是在 ada 上主动拉取，因此这里明确退出。
set -Eeuo pipefail

printf '%s\n' \
  'Ozstar-to-ada push is disabled because ada:22 is unreachable from Ozstar.' \
  'Run pull_askap_products.py on an ada login node instead:' \
  '  python3 pull_askap_products.py --all --dry-run' \
  '  python3 pull_askap_products.py --all' >&2
exit 2
