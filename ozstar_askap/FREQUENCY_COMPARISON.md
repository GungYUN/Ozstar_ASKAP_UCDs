# Top 与 central 频率的独立索引对比

正式默认已改为 `central`，仍保留 top/central 控制项。以下是独立对比流程，
不是正式部署或数据搬迁操作。本测试不改 SIF、不下载 MS、不提交 Slurm、不修改处理数据库，
也不覆盖已完成的 top 索引。仅重新查询全表，生成另一套索引和差异报告。

## 计算定义

- `νmax = c/em_min`，`νmin = c/em_max`；波长先按单位转换为米。
- top 使用 `νmax`；central 使用 `(νmin + νmax)/2`，不是平均波长的倒数。
- 两者均使用 `半径 = 0.6° × 1.4 GHz / νselected`，1° 初查范围不变。
- quality、公开状态、曝光、大小、SB 排除等筛选和“每 SB 最近合格 beam”不变。
  central 半径通常更大，但可能选择另一个更近的 beam，因此最终产品集合
  不一定是 top 集合的简单超集。
- CLI：`--radius-frequency-mode {top,central}`；环境变量：
  `ASKAP_CASDA_RADIUS_FREQUENCY_MODE`，默认 `central`，CLI 优先。
  两种模式缓存不能混用；旧 top 索引/缓存仍按 top 解释，切回 top 时可复用。
- 两个 CSV 保留原字段，观测表新增 `frequency_central_ghz`、
  `radius_frequency_mode`、`radius_frequency_ghz`，summary 也记录模式。

## 1. 本地 Mac 部署到共享测试代码目录

```bash
cd /Users/qhua0119/Documents/ChatGPT/Ozstar/Ozstar_ASKAP_UCDs/ozstar_askap
# 无密码文件时，按提示隐藏输入 Ozstar 登录密码。
OZSTAR_WORK=/fred/oz299/qhuang/ASKAP-UCDs-shared bash deploy_to_ozstar.sh
```

此命令沿用现有部署凭据，不修改原 `/fred/oz299/qhuang/ASKAP-UCDs` 中的程序。

## 2. Ozstar 登录节点：独立查询目录

建议先 `tmux new -s askap-central`，在 tmux 内执行下面整段。
断开时用 Ctrl-b、d；重新连接使用 `tmux attach -t askap-central`。

```bash
cd /fred/oz299/qhuang/ASKAP-UCDs-shared/ozstar_askap
module load gcc/13.3.0
module load python/3.12.3
export ASKAP_PYTHON_BIN=/fred/oz299/qhuang/ASKAP-UCDs/.venv/askap-python/bin/python

TOP_WORK=/fred/oz299/qhuang/dstools/shared-validation/run-20261006-150725
CENTRAL_WORK=/fred/oz299/qhuang/dstools/shared-validation/central-frequency-20261008

export ASKAP_WORK="$CENTRAL_WORK"
export ASKAP_CATALOGUE=/fred/oz299/qhuang/ASKAP-UCDs/catalogue/UltracoolSheet_Main_index_unbinaryUCD.csv
test -s "$TOP_WORK/state/crosscatalogue.json" || exit 1
test -s "$ASKAP_CATALOGUE" || exit 1
mkdir -p "$CENTRAL_WORK/logs"

"$ASKAP_PYTHON_BIN" -m py_compile config.py casda_query.py crosscatalogue.py \
  ozstar_main.py compare_crosscatalogues.py
"$ASKAP_PYTHON_BIN" -m unittest discover -s tests -v || exit 1

set -o pipefail
"$ASKAP_PYTHON_BIN" ozstar_main.py \
  --radius-frequency-mode central \
  --update-crosscatalogue --crosscatalogue-only --no-submit \
  2>&1 | tee -a "$CENTRAL_WORK/logs/crosscatalogue-central.log"
query_rc=${PIPESTATUS[0]}
echo "Central query exit code: $query_rc"
cat "$CENTRAL_WORK/state/crosscatalogue-summary.json"
```

预期 `radius_frequency_mode=central`，`total_sources=3020`；检查
`query_failed/unknown`，不要预先假定观测数量必然增加多少。
未完成时在**同一个** `CENTRAL_WORK` 重跑相同命令即可恢复检查点。
完成后再次使用 `--update-crosscatalogue` 会开启新一轮全查询。
若只重试失败项：

```bash
"$ASKAP_PYTHON_BIN" ozstar_main.py --radius-frequency-mode central \
  --retry-crosscatalogue-failures --crosscatalogue-only --no-submit
```

## 3. 完成后，对比原 top 与新 central

```bash
TOP_WORK=/fred/oz299/qhuang/dstools/shared-validation/run-20261006-150725
CENTRAL_WORK=/fred/oz299/qhuang/dstools/shared-validation/central-frequency-20261008
COMPARE_DIR="$CENTRAL_WORK/comparison-$(date +%Y%m%d-%H%M%S)"

"$ASKAP_PYTHON_BIN" compare_crosscatalogues.py \
  --top-work "$TOP_WORK" --central-work "$CENTRAL_WORK" \
  --output-dir "$COMPARE_DIR"
```

程序只读两个已发布 JSON，核对 catalogue、筛选配置和模式，拒绝覆盖旧报告。
查询失败/未知、沿用失败前旧结果的源记作 `uncomparable_query`，不计入观测差异。

- `source_comparison.csv`：每源观测数、共同/仅 top/仅 central 数、替换的 SB 数。
- `observation_differences.csv`：每个不同产品一行，标明出现在哪一侧，
  区分 `beam_changed`、`product_changed`、`observation_added_or_removed`。
  一个 SB 替换会对应一条 only_top 和一条 only_central，不是两个新 SB。
- `comparison-summary.json`：全表汇总；`*_count` 是源—观测关联数，
  同一物理产品对应多个源时分别计数；`replaced_sb_count` 是源—SB 替换数。
  共享组统计分别查看两套 `crosscatalogue-summary.json`。

**两次联网查询时间不同，CASDA 本身新增/修改观测也可能造成差异；
不能把所有差异直接归因于频率选择。** 此测试比较两个时刻的实际筛选结果。

## 4. Mac 下载表格供 TOPCAT 检查

下面命令创建新的本地审阅目录；Ozstar 上的原始结果保留不动。

```bash
REVIEW_ROOT=/Users/qhua0119/Documents/ChatGPT/Ozstar/观测记录表格
REVIEW_DIR="$REVIEW_ROOT/frequency-review-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$REVIEW_DIR/top" "$REVIEW_DIR/central"
scp 'qhuang@ozstar.swin.edu.au:/fred/oz299/qhuang/dstools/shared-validation/run-20261006-150725/state/crosscatalogue*.csv' "$REVIEW_DIR/top/"
scp 'qhuang@ozstar.swin.edu.au:/fred/oz299/qhuang/dstools/shared-validation/central-frequency-20261008/state/crosscatalogue*.csv' "$REVIEW_DIR/central/"
scp -r 'qhuang@ozstar.swin.edu.au:/fred/oz299/qhuang/dstools/shared-validation/central-frequency-20261008/comparison-*' "$REVIEW_DIR/"
```

测试结束后，正式运行前重新设置正式 `ASKAP_WORK`。默认使用 central，
也可显式加 `--radius-frequency-mode central`；需要旧策略时加
`--radius-frequency-mode top`。取消环境变量恢复的是 central 默认值。
不要把独立查询测试目录误当作原有正式处理目录，也不要自动覆盖原处理状态。
