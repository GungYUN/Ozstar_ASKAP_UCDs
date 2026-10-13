# Gate8：central 默认策略的真实小样本验收（环节1）

本环节仅测试正常下载、共享模型和目标处理，不进行故障注入、显式重试、
断点恢复测试或 Ada 传输。继续使用已验收的 `dstools-260925.sif`，无需重建。
复用已发布的 central 索引，不重新查询全表；各样本具有独立的工作目录。
本文只增加测试说明，不修改生产处理逻辑或原始 CSV。
Gate8 是本轮测试的统一名称；此前 `central-stage1-20261009` 中的预检查日志
可以保留，新的测试使用下方 `gate8-central-20261009`，无需搬移或删除旧目录。

## 数量口径与样本

本地 `/Users/qhua0119/Documents/ChatGPT/Ozstar/观测记录表格/` 中的表版本为
`20261009T011135023164Z`，所有观测行均标为 central。
全表有 60,643 条源—观测关联；60,612 是补齐四颗失败源之前的数量。
按当前 `eligible=True`（光谱型筛选）有 18,540 条处理关联，对应
18,041 个独立观测组。共享模型按独立组建立，DS/I/V 按目标关联生成。
这些是当前索引的总工作量，不是扣除已完成处理后的剩余工作量。

| 测试 | seed 范围 | 观测 | 应处理目标 | 目的 |
| --- | --- | --- | --- | --- |
| edge | 225–225 | SB67772_beam18 | UCS225 | central 新增、原 top 半径外的边缘目标 |
| shared3 | 689–689 | SB74372_beam28 | UCS689、UCS690、UCS693 | 一次建模，三目标处理；包含范围外伙伴 |
| hipass | 1872–1872 | SB83483_beam20 | UCS1872 | `HIPASSJ1404+08a_SB83483_F00_B20.ms.tar` 真实处理 |

edge 的中心距离约 0.5900°，top 半径约 0.5831°，central 半径约
0.6138°，适合检查扩大半径后新增目标的成像覆盖。
这三组共需三次建模、五套 DS/I/V（三个文件一套，共十五个文件）。
HIPASS 的归档约 15.86 GB（14.77 GiB），明显大于另两组。
请在运行前确认 /fred 的容量及 inode 配额；解压和独立 MS 副本会增加峰值占用，
`df` 显示的是文件系统容量，不能替代用户/项目配额检查。

三个样本顺序提交，每次一个观测组、8 核、40G 内存、10G 节点临时盘。
使用 `--parallel-slots 1` 是为了隔离诊断；不修改正式默认的 4 路/160G。
预计单目标作业申请 4.5 小时，三目标作业 5.5 小时，仍按程序估时，非交互式节点。
此前四路多轮测试已通过，本环节不再重复它。

## 1. Mac：部署到共享测试代码目录

不要在同一代码目录仍有作业运行时覆盖程序。确认此前测试已结束后执行：

```bash
cd /Users/qhua0119/Documents/ChatGPT/Ozstar/Ozstar_ASKAP_UCDs/ozstar_askap
# 没有密码文件时，脚本会提示隐藏输入 Ozstar 登录密码；输入后按回车。
OZSTAR_WORK=/fred/oz299/qhuang/ASKAP-UCDs-shared bash deploy_to_ozstar.sh
```

此部署不覆盖原 `/fred/oz299/qhuang/ASKAP-UCDs/ozstar_askap` 程序目录。
部署会同时上传根目录的 `Download_ASKAP_Visibility_Remote.py`，回归测试会
只读解析其中的数值逻辑，不执行登录/下载。仅上传程序，不上传观测表或运行状态。
交互输入的密码不保存到文件、不写入命令历史；已有环境变量/密码文件方式仍可使用。
后续本地观测表的下载、核对和对比均在 `观测记录表格/` 及其子目录进行，
不改动 Ozstar 上的索引路径。

## 2. Ozstar 登录节点：环境与本地回归

建议在 tmux 中执行。断开用 Ctrl-b、d；重新连接后重新设置下面的环境变量。
首次创建会话：`tmux new -s askap-gate8`。
下面的检查在函数中执行，失败用 `return`，不使用会关闭 tmux shell 的 `exit`。
只有看到 `Preflight: PASS` 后才能继续第3节；失败信息会说明具体检查项。

```bash
gate8_preflight() {
  local check_path gate8_test_rc gate8_module
  cd /fred/oz299/qhuang/ASKAP-UCDs-shared/ozstar_askap || {
    echo "FAIL: 无法进入共享程序目录"; return 1;
  }
  for gate8_module in gcc/13.3.0 python/3.12.3 apptainer; do
    module load "$gate8_module" || {
      echo "FAIL: module load $gate8_module"; return 1;
    }
  done

  export ASKAP_PYTHON_BIN=/fred/oz299/qhuang/ASKAP-UCDs/.venv/askap-python/bin/python
  export ASKAP_PYTHON_PARENT_MODULE=gcc/13.3.0
  export ASKAP_PYTHON_MODULE=python/3.12.3
  export ASKAP_CATALOGUE=/fred/oz299/qhuang/ASKAP-UCDs/catalogue/UltracoolSheet_Main_index_unbinaryUCD.csv
  export ASKAP_TIME_CSV=/fred/oz299/qhuang/ASKAP-UCDs/catalogue/All_combine_data_unrepetition.csv
  export DSTOOLS_CONTAINER=/fred/oz299/qhuang/dstools/dstools-260925.sif
  export ASKAP_CPU_PER_SLOT=8
  export ASKAP_CREATE_MODEL_THREADS=8
  export ASKAP_SBATCH_MEM=40G
  export ASKAP_SBATCH_TMP=10G
  export ASKAP_HOURS_PER_OBS=2.0
  export ASKAP_SHARED_TARGET_HOURS=0.5
  export ASKAP_SOURCE_OVERHEAD_HOURS=2.0
  export ASKAP_MAX_JOB_WALLTIME_HOURS=48
  unset ASKAP_CASDA_RADIUS_FREQUENCY_MODE ASKAP_GATE6_FAULT_CONFIG

  INDEX_WORK=/fred/oz299/qhuang/dstools/shared-validation/central-frequency-20261008
  PILOT_ROOT=/fred/oz299/qhuang/dstools/shared-validation/gate8-central-20261009
  set -o pipefail

  for check_path in "$INDEX_WORK/state/crosscatalogue.json" \
    "$ASKAP_CATALOGUE" "$ASKAP_TIME_CSV" "$DSTOOLS_CONTAINER" \
    /fred/oz299/qhuang/ASKAP-UCDs-shared/Download_ASKAP_Visibility_Remote.py \
    /fred/oz299/qhuang/dstools/dstools-260925.sif.sha256; do
    test -s "$check_path" || {
      echo "FAIL: 文件不存在或为空：$check_path"; return 1;
    }
  done
  test -x "$ASKAP_PYTHON_BIN" || {
    echo "FAIL: Python 不存在或不可执行：$ASKAP_PYTHON_BIN"; return 1;
  }
  sha256sum -c /fred/oz299/qhuang/dstools/dstools-260925.sif.sha256 || {
    echo "FAIL: SIF 校验未通过"; return 1;
  }
  "$ASKAP_PYTHON_BIN" -m py_compile config.py casda_query.py crosscatalogue.py \
    ozstar_main.py shared_controller.py shared_worker.py process_job.py || {
    echo "FAIL: Python 编译检查"; return 1;
  }

  mkdir -p "$PILOT_ROOT/logs" || {
    echo "FAIL: 无法创建预检查日志目录"; return 1;
  }
  "$ASKAP_PYTHON_BIN" -m unittest discover -s tests -v 2>&1 \
    | tee "$PILOT_ROOT/logs/preflight-unittest.log"
  gate8_test_rc=$?
  test "$gate8_test_rc" -eq 0 || {
    echo "FAIL: 回归测试或日志写入失败，请查看 $PILOT_ROOT/logs/preflight-unittest.log"
    return 1
  }
  "$ASKAP_PYTHON_BIN" -c \
    'import config; print("Default:", config.CASDA_RADIUS_FREQUENCY_MODE); assert config.CASDA_RADIUS_FREQUENCY_MODE == "central"' || {
    echo "FAIL: 部署的默认频率模式不是 central，或 config 导入失败"; return 1;
  }
  df -h /fred/oz299/qhuang/dstools || return 1
  df -i /fred/oz299/qhuang/dstools || return 1
  return 0
}

if gate8_preflight; then
  echo "Gate8 Preflight: PASS，可以继续第3节定义样本函数。"
else
  echo "Gate8 Preflight: FAIL，请保留上方输出，先不要下载或提交；当前 shell 不退出。"
fi
```

不要添加 `--update-crosscatalogue`、`--refresh-cache` 或 `--retry-existing`。
不设置频率 CLI/环境覆盖，正是为了验证新的 central 默认值。

## 3. 在同一 shell 中定义样本、准备和检查函数

复制整段。函数只是调用已有 controller 和只读检查，不增加新的调度机制。
`select_case` 必须在每个样本开始前调用；断开后重新定义函数、重新选择即可。

```bash
select_case() {
  CASE_NAME="$1"
  case "$CASE_NAME" in
    edge)
      SEED=225
      OBS_KEY=6952057a1ddba073e679bf07eacf92186e68bba1648f1fa30bff8e8c028f794e
      ;;
    shared3)
      SEED=689
      OBS_KEY=36d17f6f9b843c1d9e0c7aa0b9277258665fd02841a6c936f0728f79d844e2b7
      ;;
    hipass)
      SEED=1872
      OBS_KEY=8569cbdc4f44fcf9d6ae301609abc53ec6a34600400692acd7ae8e867ba3b013
      ;;
    *) echo "Unknown case: $CASE_NAME" >&2; return 1 ;;
  esac
  export ASKAP_WORK="$PILOT_ROOT/$CASE_NAME"
  printf 'Case=%s; seed=%s; work=%s\n' "$CASE_NAME" "$SEED" "$ASKAP_WORK"
}

prepare_case() {
  mkdir -p "$ASKAP_WORK/state" "$ASKAP_WORK/logs" || return 1
  # Only copy the published index. Do NOT copy processing DB/state or products.
  if ! test -e "$ASKAP_WORK/state/crosscatalogue.json"; then
    cp "$INDEX_WORK/state/crosscatalogue.json" "$ASKAP_WORK/state/crosscatalogue.json" || return 1
  fi
  "$ASKAP_PYTHON_BIN" - "$ASKAP_WORK" "$SEED" "$OBS_KEY" <<'PY'
import json
import sys
from pathlib import Path
import config
import crosscatalogue

work = Path(sys.argv[1])
seed, key = int(sys.argv[2]), sys.argv[3]
snapshot = json.loads((work / 'state/crosscatalogue.json').read_text())
assert config.CASDA_RADIUS_FREQUENCY_MODE == 'central', 'Unexpected frequency override'
assert snapshot['revision'] == '20261009T011135023164Z', 'Unexpected index revision'
assert snapshot.get('finished_at'), 'Index was not published completely'
assert snapshot['signature'] == crosscatalogue.selection_signature(), 'Index/config mismatch: STOP; do not re-query'
assert all(s['query_status'] in ('available', 'no_observations') for s in snapshot['sources'])
groups = crosscatalogue.groups_for_range(snapshot, seed, seed)
selected = [g for g in groups if g['observation']['key'] == key]
assert len(selected) == 1, 'Exact product was not selected'
group = selected[0]
expected = {225: {'UCS225'}, 689: {'UCS689', 'UCS690', 'UCS693'}, 1872: {'UCS1872'}}[seed]
assert {s['source_name'] for s in group['sources']} == expected
assert all(Path(s['query_cache']).is_file() for s in group['sources']), 'Frozen ECSV missing'
print('Frozen central index: PASS', group['observation']['sb_name'], sorted(expected))
PY
  test "$?" -eq 0 || return 1
  "$ASKAP_PYTHON_BIN" ozstar_main.py --begin "$SEED" --end "$SEED" \
    --shared-observation-key "$OBS_KEY" --parallel-slots 1 \
    --prepare-download --no-submit 2>&1 | tee -a "$ASKAP_WORK/logs/prepare.log"
  local rc=${PIPESTATUS[0]}
  test "$rc" -eq 0 || return "$rc"
  "$ASKAP_PYTHON_BIN" - "$ASKAP_WORK" "$OBS_KEY" <<'PY'
import json
import sys
from pathlib import Path
work = Path(sys.argv[1])
controller = json.loads((work / 'state/shared-controller.json').read_text())
job = json.loads(Path(controller['active_manifest']).read_text())
assert job['observation']['key'] == sys.argv[2]
assert job['status'] == 'prepared', job.get('results')
assert not job.get('results'), job.get('results')
script = Path(job['script_path']).read_text()
assert '#SBATCH --cpus-per-task=8' in script
assert '#SBATCH --mem=40G' in script
assert '#SBATCH --tmp=10G' in script
assert len(job['sources']) in (1, 3)
print('Prepared:', job['manifest_path'])
print('Targets:', [s['source_name'] for s in job['sources']])
print('Requested hours:', job['job_walltime_hours'])
print('Preparation/Slurm settings: PASS')
PY
}

run_case() {
  "$ASKAP_PYTHON_BIN" ozstar_main.py --begin "$SEED" --end "$SEED" \
    --shared-observation-key "$OBS_KEY" --parallel-slots 1 --submit \
    2>&1 | tee -a "$ASKAP_WORK/logs/controller.log"
  local rc=${PIPESTATUS[0]}
  echo "Controller exit code: $rc"
  return "$rc"
}

check_case() {
  "$ASKAP_PYTHON_BIN" - "$ASKAP_WORK" "$SEED" "$OBS_KEY" <<'PY'
import json
import sqlite3
import sys
from pathlib import Path

work = Path(sys.argv[1])
seed, key = int(sys.argv[2]), sys.argv[3]
expected = {225: {'UCS225'}, 689: {'UCS689', 'UCS690', 'UCS693'}, 1872: {'UCS1872'}}[seed]
controller = json.loads((work / 'state/shared-controller.json').read_text())
assert controller['status'] == 'complete', 'Controller is still active'
assert len(controller['jobs']) == 1, controller['jobs']
job = json.loads(Path(controller['jobs'][0]).read_text())
assert job['observation']['key'] == key
assert set(job['results']) == expected, job['results']
assert all(r['status'] == 'complete' for r in job['results'].values()), job['results']
assert job['shared_model_seconds']['preprocess'] > 0
assert job['shared_model_seconds']['create_model'] > 0

connection = sqlite3.connect((work / 'state/sb_processing.sqlite').resolve().as_uri() + '?mode=ro', uri=True)
connection.row_factory = sqlite3.Row
try:
    rows = list(connection.execute('SELECT * FROM sb_processing'))
finally:
    connection.close()
assert len(rows) == len(expected), [dict(r) for r in rows]
steps = ('extract_archive', 'preprocess', 'create_model', 'insert_model', 'subtract_model',
         'extract_ds', 'crop_i', 'crop_v', 'product_copy', 'cleanup')
origins = set()
for row in rows:
    assert row['source_name'] in expected and row['observation_key'] == key
    assert all(row[s] == 'Y' for s in steps), dict(row)
    assert row['error_step'] is None and row['retry_stage'] == 'complete', dict(row)
    assert row['model_reused'] == 1, dict(row)
    origins.add(row['model_origin'])
    number = row['ucs_number']
    start = (number - 1) // 50 * 50 + 1
    sb_name = row['sb_id'] + '_' + row['beam']
    product = work / 'products' / f'UCS{start}-{start + 49}' / row['source_name'] / 'LongObs' / sb_name
    for path in (product / (sb_name + '.ds'),
                 product / 'wsclean_model/wsclean-MFS-I-image.fits',
                 product / 'wsclean_model/wsclean-MFS-V-image.fits'):
        assert path.is_file(), path
        print('Y', path)
    provenance = json.loads((product / 'observation.json').read_text())
    assert provenance['observation_key'] == key
    assert provenance['index_revision'] == '20261009T011135023164Z'
assert len(origins) == 1 and next(iter(origins)), origins
assert not list((work / 'staging').rglob('*.ms')), 'Unexpected retained MS: inspect before deleting'
print('Shared model timings (seconds):', job['shared_model_seconds'])
print('Gate8 technical check: PASS', sorted(expected))
PY
}
```

## 4. 逐组运行，任意一步失败就停止，保留数据和日志

先 edge，再 shared3，最后 HIPASS。均在登录节点执行，controller 负责下载、
提交 sbatch 和等待；不需要申请交互式节点。

```bash
select_case edge && prepare_case && run_case && check_case
```

上述检查通过后继续：

```bash
select_case shared3 && prepare_case && run_case && check_case
```

再通过后，确认 HIPASS 所需持久盘空间和配额充足：

```bash
select_case hipass && prepare_case && run_case && check_case
```

若失败，不要删除 MS、修改 SQLite、加 `--retry-existing` 或重复手动 `sbatch`。
收集对应 `$ASKAP_WORK/logs/controller.log`、`logs/slurm-*.err`、
`logs/failures/` 后排查。controller 退出 0 也不能代替 `check_case` 验收。
成功的公共模型会正常清理；其耗时仍保存在任务 JSON 中。
`model_reused=1` 表示目标用公共模型副本，并不表示从旧测试缓存命中模型。

等待时可从另一个 SSH 会话查看队列和日志：

```bash
squeue -u qhuang
tail -n 60 /fred/oz299/qhuang/dstools/shared-validation/gate8-central-20261009/edge/logs/controller.log
```

## 5. Gate8 验收标准

三组 `check_case` 均 PASS、合计五套 DS/I/V 文件；shared3 的范围外两颗源
确实完成，同组三个目标记录的 `model_origin` 相同。
还需人工检查 edge 的裁剪图是否覆盖目标的自行修正位置、I/V 图与 DS 是否符合
预期；文件存在是程序完成判据，不等于科学质量保证，不对文件内容添加强制校验。

将三个检查输出及异常日志（如有）返回后，再安排环节2（共享模式重试/恢复）
与环节3（共享产物 Ada 拉取）。本环节不会提前执行这两项。
