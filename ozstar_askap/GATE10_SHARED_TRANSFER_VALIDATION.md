# Gate10：共享产物的 Ada 拉取、校验与回执验收

在 **Ada 登录节点**运行 `gate10_transfer_validation.py`，复用 Gate8 已完成的
产物，不重新下载 MS、不重新建模、不提交 sbatch，不需要交互式计算节点。
`--submit` 仅授权真实文件传输、测试副本修复及回执上传，不是 Slurm 提交。
程序仅依赖 Python 3.7+ 标准库及 SSH、rsync、sshpass；Ozstar 远端使用
`/usr/bin/python3`，无需加载项目 Python 模块或使用 SIF。

## 样本与隔离范围

| Gate8 子目录 | 观测 | 目标 | 文件数 |
| --- | --- | --- | --- |
| edge | SB67772_beam18 | UCS225 | 3 |
| shared3 | SB74372_beam28 | UCS689、UCS690、UCS693 | 9 |
| hipass | SB83483_beam20 | UCS1872 | 3 |

默认源根目录为
`/fred/oz299/qhuang/dstools/shared-validation/gate8-central-20261009`。
三个子目录的 `products/` 分别扫描，不能将整个 Gate8 工作根目录作为单个
product root。扫描前只读核对三个 controller、job 和 SQLite 的完成记录。
程序不改动这些记录，也不会将缺少 I/V/DS 的测试误报为“空数据成功”。

Ada 的全部测试文件位于
`/import/ada1/qhua0119/Combine_ImagePlot/Ozstar/gate10-validation/gate10-20261013/`。
按 case 保留产品路径，共用此测试目录下独立的 `state/transfer_ledger.sqlite`。
不使用正式 Ada 台账；测试回执仅上传至
`/fred/oz299/qhuang/dstools/gate10-validation/gate10-20261013/receipts/<case>/`，
不写入生产传输回执目录。两端文件均不删除。

## 自动验收内容

1. 拉取所有五套 DS/I/V，逐文件验证 SHA256。第一次应传输15个文件。
2. 台账精确包含5个唯一观测，每个均为 `Y/Y/Y`、`overall_status=complete`，
   且 error 为空；只有三文件全部校验且回执上传成功后，才记录为 complete。
3. 读取上传至 Ozstar 的回执，与 Ada 本地回执内容比较；核对每套三文件的
   状态、路径、大小和 SHA256，回执明确标为 `gate10-validation`。
4. 三个 case 各重复拉取一次：传输文件数应为0，产物字节与 mtime 不变，
   台账行数及首次验证时间不变。新的传输尝试/不可变回执属于正常审计记录，
   不等于重复观测记录或重复传输。
5. 在 **Ada 的隔离 UCS690 Stokes-V 副本**中翻转一个字节，保持大小与 mtime。
   dry-run 必须发现恰好一个文件需修复，真实拉取仅修复该文件，SHA256 恢复。
   不改动 Ozstar 文件。传输失败且测试改动仍完整保留时，会撤销这一字节修改。
6. 结束时再次核对 Ozstar 三组产物快照，要求全部保持原字节；台账仍仅5行。

为支持第5项，`pull_askap_products.py` 的 rsync 调用增加了 `--ignore-times`。
**该参数只作用于 Python 已按 SHA256 筛出的待传输清单**，不是强制传输全部文件。
否则同大小、同 mtime 的损坏文件会被 rsync 快速检查跳过。
本地回归另外模拟缺失 V、传输失败及源文件在传输中变化，检查不会错误记为成功。
Gate10 不验证 DS/FITS 科学内容；成像质量仍需人工确认。

## 1. Mac：仅部署传输脚本和说明到 Ozstar

确认没有正在运行的 Ada 拉取，再更新传输脚本。无需全量覆盖处理程序。
下方 `scp` 会要求输入 Ozstar 密码；不会要求 Mac 密码文件。

```bash
cd /Users/qhua0119/Documents/ChatGPT/Ozstar/Ozstar_ASKAP_UCDs/ozstar_askap
shasum -a 256 gate10_transfer_validation.py pull_askap_products.py

scp gate10_transfer_validation.py pull_askap_products.py \
  GATE10_SHARED_TRANSFER_VALIDATION.md \
  qhuang@ozstar.swin.edu.au:/fred/oz299/qhuang/ASKAP-UCDs-shared/ozstar_askap/
```

这一步只更新三个文件；不涉及 Gate8 数据、SIF、controller/worker 或观测表。

## 2. Ada：准备密码并获取相同脚本

以下命令使用 Bash（Ada 通常默认如此）。已有 mode-600 密码文件时复用；
没有时隐藏输入一次密码，仅用于当前 shell，不创建新密码文件。
不要开启 `set -x`，不要在命令行直接写密码。

```bash
ADA_ROOT=/import/ada1/qhua0119/Combine_ImagePlot/Ozstar
PASSWORD_FILE="$ADA_ROOT/state/ozstar-transfer-password"
mkdir -p "$ADA_ROOT/bin"

if test -s "$PASSWORD_FILE"; then
  chmod 600 "$PASSWORD_FILE"
  GATE10_SSH_AUTH=(-f "$PASSWORD_FILE")
else
  read -r -s -p "Ozstar 登录密码：" GATE10_PASSWORD
  printf '\n'
  export TRANSFER_PASSWORD="$GATE10_PASSWORD"
  export SSHPASS="$GATE10_PASSWORD"
  unset GATE10_PASSWORD
  GATE10_SSH_AUTH=(-e)
fi

sshpass "${GATE10_SSH_AUTH[@]}" scp -4 \
  qhuang@ozstar.swin.edu.au:/fred/oz299/qhuang/ASKAP-UCDs-shared/ozstar_askap/gate10_transfer_validation.py \
  qhuang@ozstar.swin.edu.au:/fred/oz299/qhuang/ASKAP-UCDs-shared/ozstar_askap/pull_askap_products.py \
  "$ADA_ROOT/bin/"
```

若上传失败，先停止并返回错误，不继续执行旧脚本。
之后在同一终端预检查；函数失败不会退出 tmux 的 shell：

```bash
gate10_preflight() {
  cd /import/ada1/qhua0119/Combine_ImagePlot/Ozstar/bin || return 1
  command -v python3 || return 1
  command -v rsync || return 1
  command -v sshpass || return 1
  python3 --version || return 1
  sha256sum gate10_transfer_validation.py pull_askap_products.py || return 1
  python3 -m py_compile gate10_transfer_validation.py pull_askap_products.py || return 1
  python3 gate10_transfer_validation.py --help || return 1
  sshpass "${GATE10_SSH_AUTH[@]}" ssh -4 -x -o ConnectTimeout=30 \
    qhuang@ozstar.swin.edu.au \
    '/usr/bin/python3 --version && command -v rsync && command -v sha256sum' || return 1
  df -h /import/ada1/qhua0119/Combine_ImagePlot/Ozstar || return 1
}
if gate10_preflight; then
  echo "Gate10 preflight: PASS"
else
  echo "Gate10 preflight: FAIL，请先停止并检查错误。"
fi
```

对比 Ada 与 Mac 的两个 SHA256，要求相同。容量检查之外，还需确认个人配额。
只传裁剪产物，不复制 HIPASS 的15GB归档/MS，因此不会重现 Gate8 长时间下载。

## 3. Ada：运行

仅在预检查通过后执行。固定目录必须是新的/空目录；若之前已有结果或失败日志，
先检查它，不覆盖、不删除，不使用相同目录盲目重跑。

```bash
cd /import/ada1/qhua0119/Combine_ImagePlot/Ozstar/bin
GATE10_WORK=/import/ada1/qhua0119/Combine_ImagePlot/Ozstar/gate10-validation/gate10-20261013

python3 gate10_transfer_validation.py \
  --gate8-work /fred/oz299/qhuang/dstools/shared-validation/gate8-central-20261009 \
  --work "$GATE10_WORK" \
  --submit

gate10_rc=$?
echo "Gate10 exit code: $gate10_rc"
unset TRANSFER_PASSWORD SSHPASS
```

driver 自带日志，无需额外 tee。可以在 Ada 的 tmux 中运行，但不需要 sbatch。
默认读取前述密码文件；没有密码文件时，使用上节当前 shell 的环境密码。
预期结束输出 `Gate10 shared-product Ada pull validation: PASS`。
若失败，保留两端文件与台账，返回日志/结果排查，不手动清理或修改数据库。

## 4. Ada：新终端检查结果

```bash
GATE10_WORK=/import/ada1/qhua0119/Combine_ImagePlot/Ozstar/gate10-validation/gate10-20261013
tail -n 80 "$GATE10_WORK/logs/gate10-driver.log"
cat "$GATE10_WORK/state/gate10-result.json"

python3 - "$GATE10_WORK" <<'PY'
import json
import sqlite3
import sys
from pathlib import Path

work = Path(sys.argv[1])
result = json.loads((work / 'state/gate10-result.json').read_text())
assert result['status'] == 'PASS', result
assert result['observation_count'] == 5 and result['product_file_count'] == 15
assert result['initial_files_transferred'] == 15
assert result['repeat_files_transferred'] == 0 and result['repair_files_transferred'] == 1
assert result['source_snapshot_unchanged'] and not result['source_deleted']
connection = sqlite3.connect((work / 'state/transfer_ledger.sqlite').resolve().as_uri() + '?mode=ro', uri=True)
try:
    rows = connection.execute('''SELECT source_name, sb_id, beam,
        ds_status, stokes_i_status, stokes_v_status, overall_status, error
        FROM transfer_ledger ORDER BY source_name''').fetchall()
finally:
    connection.close()
expected = {('UCS225', 'SB67772', 'beam18'), ('UCS689', 'SB74372', 'beam28'),
            ('UCS690', 'SB74372', 'beam28'), ('UCS693', 'SB74372', 'beam28'),
            ('UCS1872', 'SB83483', 'beam20')}
assert len(rows) == 5 and {row[:3] for row in rows} == expected, rows
for row in rows:
    print(row)
    assert row[3:] == ('Y', 'Y', 'Y', 'complete', None), row
print('Gate10 result and SQLite verification: PASS')
PY
```

最终有五套产物、七份不可变回执：三个首次、三个重复及一次修复。
Gate10 的回执仅用于本次测试，不授权清理生产/Gate8 文件。
通过后再根据正式处理工作目录确定生产 `--source-root` 与生产回执路径；
不要直接套用旧项目 `/fred/.../ASKAP-UCDs/products` 去扫描共享测试或新版目录。
