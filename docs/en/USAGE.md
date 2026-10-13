# 2. Usage and adjustable parameters

[Index](../../README.md) · [中文](../zh-CN/USAGE.md) · [Installation](INSTALLATION.md) · [How it works](REFERENCE.md)

Commands use Bash. Run sections 1–5 on an **Ozstar login node**, section 6 on
an **Ada login node**. Stop on a failed prerequisite; do not paste the remaining
steps blindly. CLI options are appended to commands; environment variables
must be exported before starting Python. Explicit CLI values override their
environment equivalents. Read defaults below as code defaults, not a guarantee
that an already-exported value or a frozen job uses those defaults.

## 1. Session environment and common settings

Restore this environment after connecting, preferably inside a persistent
`tmux new -s askap-production` session. On reconnection attach on the original
login host; detach with Ctrl-b, d, not Ctrl-Z.

```bash
askap_session() {
  cd /fred/oz299/qhuang/ASKAP-UCDs-shared/ozstar_askap || return 1
  module load gcc/13.3.0 || return 1
  module load python/3.12.3 || return 1
  module load apptainer || return 1
  export ASKAP_WORK=/fred/oz299/qhuang/ASKAP-UCDs-production
  export ASKAP_CATALOGUE=/fred/oz299/qhuang/ASKAP-UCDs/catalogue/UltracoolSheet_Main_index_unbinaryUCD.csv
  export ASKAP_TIME_CSV=/fred/oz299/qhuang/ASKAP-UCDs/catalogue/All_combine_data_unrepetition.csv
  export ASKAP_PYTHON_BIN=/fred/oz299/qhuang/ASKAP-UCDs/.venv/askap-python/bin/python
  export ASKAP_PYTHON_PARENT_MODULE=gcc/13.3.0
  export ASKAP_PYTHON_MODULE=python/3.12.3
  export DSTOOLS_CONTAINER=/fred/oz299/qhuang/dstools/dstools-260925.sif
  export ASKAP_PARALLEL_SLOTS=4 ASKAP_CPU_PER_SLOT=8 ASKAP_CREATE_MODEL_THREADS=8
  export ASKAP_SBATCH_MEM=160G ASKAP_SBATCH_TMP=10G
  export ASKAP_MAX_JOB_WALLTIME_HOURS=48
  unset ASKAP_CASDA_RADIUS_FREQUENCY_MODE ASKAP_GATE6_FAULT_CONFIG ASKAP_GATE9_FAULT_CONFIG
  test -s "$ASKAP_CATALOGUE" && test -s "$ASKAP_TIME_CSV" || return 1
  "$ASKAP_PYTHON_BIN" -c 'import numpy,pandas,astropy,astroquery,keyring' || return 1
  sha256sum -c /fred/oz299/qhuang/dstools/dstools-260925.sif.sha256 || return 1
  bash prepare_layout.sh || return 1
  set -o pipefail
}
if askap_session; then
  echo "Session: READY"
else
  echo "Session: FAIL; stop and inspect the output."
fi
```

If needed, enter the CASDA password using [Installation §5](INSTALLATION.md#5-casda-credentials-on-ozstar).
This function deliberately selects the production directory and baseline
resources. Edit these exports for your deployment; never run with an empty
`ASKAP_WORK`. Check personal/project quotas before downloading.

| Environment variable | Code default | Purpose |
| --- | --- | --- |
| `ASKAP_WORK` | `/fred/oz299/qhuang/ASKAP-UCDs` | Persistent data/state root; this guide explicitly uses a new production root |
| `ASKAP_CATALOGUE` | `$ASKAP_WORK/catalogue/UltracoolSheet_Main_index_unbinaryUCD.csv` | Source catalogue |
| `ASKAP_TIME_CSV` | `$ASKAP_WORK/catalogue/All_combine_data_unrepetition.csv` | Legacy observation-epoch fallback |
| `ASKAP_PYTHON_BIN` | `python3` | Controller/worker host interpreter; use the absolute venv path |
| `ASKAP_PYTHON_PARENT_MODULE`, `ASKAP_PYTHON_MODULE` | `gcc/13.3.0`, `python/3.12.3` | Modules loaded by generated workers |
| `ASKAP_PYTHON_ENV` | `$ASKAP_WORK/.venv/askap-python` | Installation script only: environment to create |
| `DSTOOLS_CONTAINER` | `/fred/oz299/qhuang/dstools/dstools-260925.sif` | Accepted processing image |
| `APPTAINER_BIN`, `ASKAP_APPTAINER_BIND` | `apptainer`, `/fred/oz299/qhuang:/fred/oz299/qhuang` | Container executable and bind |
| `CASDA_USERNAME` | `qhua0119@uni.sydney.edu.au` | CASDA login identity |
| `CASDA_PASSWORD`, `CASDA_PASSWORD_FILE` | No env password; `$HOME/.config/askap/casda-password` | Runtime credentials; env value takes precedence |

Deployment wrapper options (not runtime processing settings):

| Environment variable | Default | Purpose |
| --- | --- | --- |
| `OZSTAR_WORK` | `/fred/oz299/qhuang/ASKAP-UCDs` | Remote parent for uploaded code |
| `OZSTAR_SSH_USER`, `OZSTAR_SSH_HOST` | `qhuang`, `ozstar.swin.edu.au` | Deployment SSH destination |
| `TRANSFER_PASSWORD` | Unset | Current-process deployment password; otherwise file/prompt |
| `OZSTAR_PASSWORD_FILE` | `TRANSFER_PASSWORD_FILE`, then `$HOME/.config/askap/transfer-password` | Mode-600 deployment password file |

## 2. Build or update the observation index

```bash
"$ASKAP_PYTHON_BIN" ozstar_main.py --crosscatalogue-only
```

Builds the full catalogue index if necessary, otherwise reuses a compatible
published index. It does not download MS or submit compute jobs. Inspect:

```bash
cat "$ASKAP_WORK/state/crosscatalogue-summary.json"
```

`unknown=0` means no source remains unattempted; `query_failed=0` is additionally
needed for a fully successful query. `no_observations` can be a valid result.
CSV files are human-readable and TOPCAT-compatible; counts are source-observation
associations, not necessarily independent model builds or remaining work.

| CLI option | Default | Purpose / constraint |
| --- | --- | --- |
| `--crosscatalogue-only` | Off | Index operations only; no MS download/submission |
| `--update-crosscatalogue` | Off | Explicitly re-query every source and publish an updated index |
| `--retry-crosscatalogue-failures` | Off | Retry failed/pending queries only; requires `--crosscatalogue-only`; cannot combine with full update, refresh or legacy mode |
| `--radius-frequency-mode central\|top` | `central` unless overridden by env | Frequency used for half-power radius; use another work directory for a comparison |
| `--refresh-cache` | Off | In shared mode, requests a full index update, not just one seed's query |

| Environment variable | Default | Purpose |
| --- | --- | --- |
| `ASKAP_CASDA_RADIUS_FREQUENCY_MODE` | `central` | CLI can override with `top` or `central` |
| `ASKAP_CASDA_INITIAL_QUERY_RADIUS_DEG` | `1.0` | Initial search radius, degrees |
| `ASKAP_CASDA_REFERENCE_FWHM_DEG`, `ASKAP_CASDA_REFERENCE_FREQUENCY_GHZ` | `1.2`, `1.4` | Beam-scaling reference; these are full width and GHz |
| `ASKAP_CASDA_QUERY_LIMIT` | `5000` | TAP TOP limit; a truncated query is not accepted as complete |
| `ASKAP_CROSSCATALOGUE_QUERY_DELAY_SECONDS` | `0.2` | Delay between sequential source queries |
| `ASKAP_SPTNUM_THRESHOLD` | `20` | Processing eligibility; lower spectral types still appear in the index |
| `ASKAP_CASDA_MAX_FILE_SIZE_KB` | `52428800` | Per-product selection limit in CASDA KB |

Examples, only when no processing plan is active:

```bash
"$ASKAP_PYTHON_BIN" ozstar_main.py --update-crosscatalogue --crosscatalogue-only
"$ASKAP_PYTHON_BIN" ozstar_main.py --retry-crosscatalogue-failures --crosscatalogue-only
```

A compatible central index can also be reused by copying only its published
`crosscatalogue.json` into a new work tree. Its referenced immutable ECSV files
must remain readable, and catalogue/selection signatures must match. Do not copy
the old processing database or controller plan. Do not delete the original index tree.
Local observation-table downloads/review belong under
`/Users/qhua0119/Documents/ChatGPT/Ozstar/观测记录表格/` and its subdirectories.

## 3. Process an inclusive source range

```bash
"$ASKAP_PYTHON_BIN" ozstar_main.py --begin 1501 --end 1550 --submit
```

The login controller prepares archives, submits sbatch, waits and advances
through batches. Keep it running in tmux for automatic progression. Seeds are
1501–1550 inclusive; qualifying partners outside this range are also processed
for those same observations. Partners' other observations are not expanded.
The 50-number output folder is a naming convention, not a fixed download batch.

| CLI option | Default | Purpose / constraint |
| --- | --- | --- |
| `--begin`, `--end` | `ASKAP_UCS_START=1`, `ASKAP_UCS_END=1000` | Inclusive seed bounds; specify explicitly |
| `--submit` | Off | Download as needed, submit, wait and progress |
| `--no-submit` | Off | Prevent submission; takes precedence over `--submit` |
| `--prepare-download` | Off | Prepare the current planned batch without submission when combined with `--no-submit` |
| `--parallel-slots N` | `4` | Concurrent independent observations; N × CPUs/slot ≤32 |
| `--shared-observation-key KEY` | Unset | Restrict to one indexed product; retains all its qualifying partners |
| `--max-resubmits N` | `20` | Bounded partial-job continuations, not scientific-failure retries |
| `--controller-poll-interval SECONDS` | `60` | Manifest/worker-marker polling interval |

Resource and download environment options:

| Environment variable | Default | Purpose |
| --- | --- | --- |
| `ASKAP_PARALLEL_SLOTS`, `ASKAP_CPU_PER_SLOT` | `4`, `8` | 32 CPUs with four slots; actual slots decrease for fewer observations |
| `ASKAP_CREATE_MODEL_THREADS` | CPUs/slot (`8`) | DStools create-model `-j`; must be 1…CPUs/slot |
| `ASKAP_SBATCH_MEM`, `ASKAP_SBATCH_TMP` | `160G`, `10G` | Whole-job RAM/scratch, not per-slot requests; RAM does not auto-scale down |
| `ASKAP_SBATCH_PARTITION` | Empty | Optional partition; empty leaves Slurm default |
| `ASKAP_MAX_MS_PER_JOB` | `40` | Independent observation groups per planned batch, also bounded by time |
| `ASKAP_MAX_JOB_WALLTIME_HOURS` | `48` | Maximum estimated request and packing cap; allowed >0 and ≤48, not fixed request time |
| `ASKAP_HOURS_PER_OBS` | `2.0` | Model-work estimate per staged MS, hours |
| `ASKAP_SHARED_TARGET_HOURS` | `0.5` | Additional work estimate per unfinished target, hours |
| `ASKAP_SOURCE_OVERHEAD_HOURS` | `2.0` | Whole-job time overhead added to the busiest lane |
| `ASKAP_WALLTIME_RESERVE_MINUTES` | `20` | Stop starting work near the shared job deadline |
| `ASKAP_MAX_AUTO_RESUBMITS` | `20` | Default for CLI `--max-resubmits` |
| `ASKAP_CONTROLLER_POLL_INTERVAL_SECONDS` | `60` | Default for CLI polling interval |
| `ASKAP_STAGING_BUDGET_BYTES` | `2199023255552` (2 TiB) | Program staging budget; not a filesystem/user quota override |
| `ASKAP_CASDA_SOURCE_DOWNLOAD_RETRIES`, `ASKAP_CASDA_SOURCE_RETRY_DELAY_SECONDS` | `3`, `30` | Download attempts and delay; distinct from processing retries |

DStools/output environment options (set before planning a new run):

| Environment variable | Default | Purpose |
| --- | --- | --- |
| `ASKAP_DSTOOLS_BAND` | `AK_low` | Model imaging preset |
| `ASKAP_CREATE_MODEL_ITERATIONS` | `100000` | create-model iterations |
| `ASKAP_MIN_UV_METRES` | `200` | Model imaging minimum baseline |
| `ASKAP_CREATE_MODEL_MULTISCALE` | `1` | Enable multiscale; `0/false/no/off` disable |
| `ASKAP_EXTRACT_MIN_UV_METRES` | `500` | Dynamic-spectrum extraction minimum baseline |
| `ASKAP_CROP_ARCMIN` | `10` | Square I/V cutout side length in arcminutes |
| `ASKAP_DEFAULT_T_MIN`, `ASKAP_MJD_J2000` | `61041.5`, `51545` | Final epoch fallback and proper-motion reference MJD |

Example: two slots with 40G RAM per slot, prepare before submission:

```bash
export ASKAP_SBATCH_MEM=80G
"$ASKAP_PYTHON_BIN" ozstar_main.py --begin 1501 --end 1550 --parallel-slots 2 --prepare-download --no-submit
"$ASKAP_PYTHON_BIN" ozstar_main.py --begin 1501 --end 1550 --parallel-slots 2 --submit
```

Do not edit frozen jobs or change slots, SIF or imaging configuration mid-plan.
New settings affect future work/model keys; already complete matching products
are not automatically rebuilt or overwritten when imaging settings change.
Legacy-only controls such as `ASKAP_JOB_WALLTIME_HOURS`, `--controller-initial-delay`
and `--allow-multiple` are not shared-mode tuning knobs. `--legacy-source-mode`
is a compatibility entry point, not the normal workflow; never mix both modes in one active work tree.
Compute downloads are unsupported in shared mode.

## 4. Inspect progress and failures

```bash
"$ASKAP_PYTHON_BIN" show_failures.py
```

Outputs `UCSxxxx_SBxxxxx —— failed_step`, read-only. A missing database before
any processing has started is not evidence of a scientific failure.

| Report CLI option | Default | Purpose |
| --- | --- | --- |
| `--details` | Off | Beam, retry stage/counts, error and retained failure log |
| `--retryable-only` | Off | Hide exhausted failures; does not initiate retries |
| `--source UCSxxxx` | All sources | Repeat to filter several sources |
| `--database PATH` | `$ASKAP_WORK/state/sb_processing.sqlite` | Read another processing database |
| `--json` | Off | Machine-readable report |

Additional read-only checks:

```bash
hostname
squeue -u qhuang
"$ASKAP_PYTHON_BIN" - "$ASKAP_WORK" <<'PY'
import json
import sys
from pathlib import Path
path = Path(sys.argv[1]) / 'state/shared-controller.json'
if path.is_file():
    state = json.loads(path.read_text())
    print({key: state.get(key) for key in ('status', 'begin', 'end', 'group_index', 'active_manifest')})
else:
    print('No shared plan has been created yet')
PY
ls -lt "$ASKAP_WORK/logs/"slurm-*.err
"$ASKAP_PYTHON_BIN" show_failures.py --details
```

Read the relevant `slurm-JOBID.err` and retained `logs/failures/` files.
Empty squeue / Slurm COMPLETED / controller exit 0 alone do not prove every
observation succeeded. A completed seed plan can leave a partner's other
observations pending. See [state and cleanup](REFERENCE.md#5-state-and-cleanup).

## 5. Resume or explicitly retry failures

Resume an interrupted controller using **the same work directory and original
range/retry mode/key/parallel slots**; the ordinary example is:

```bash
"$ASKAP_PYTHON_BIN" ozstar_main.py --begin 1501 --end 1550 --submit
```

If the original process is still running on another login host, do not start a
second controller. Never delete a busy lock or state files to force progress.
If submission acknowledgment is missing, inspect the queued/running job before
altering anything; recovery intentionally refuses a speculative second submission.

To retry recorded processing failures after the prior plan has ended:

```bash
"$ASKAP_PYTHON_BIN" ozstar_main.py --begin 1501 --end 1550 --retry-existing --submit
```

| CLI option | Default | Purpose |
| --- | --- | --- |
| `--retry-existing` | Off | Select existing failed observations only; excludes new/successful/exhausted targets |

This first resets/preprocesses and builds a private model. On another failure,
only that target's MS/checksum is cleared and one fresh download/processing
attempt follows. A further failure is `exhausted`; it retains the final failed
input and does not retry again. Ordinary processing skips failures without this
flag. Download/network attempt limits are separate from this processing policy.

## 6. Pull all complete products on Ada

After Installation, run on Ada:

```bash
cd /import/ada1/qhua0119/Combine_ImagePlot/Ozstar/bin
python3 pull_askap_products.py --all \
  --source-root /fred/oz299/qhuang/ASKAP-UCDs-production/products \
  --receipt-remote-root /fred/oz299/qhuang/ASKAP-UCDs-production/state/transfer_receipts
```

This inventories all complete DS/I/V triples, incrementally pulls missing or
hash-mismatched files, verifies SHA256 and uploads verified receipts. It never
deletes Ozstar products. **Specify both roots for the new production workspace**;
script defaults still point at the older `/fred/oz299/qhuang/ASKAP-UCDs` tree.

Before running, reuse the protected password file or privately enter one:

```bash
ADA_ROOT=/import/ada1/qhua0119/Combine_ImagePlot/Ozstar
if test -z "${TRANSFER_PASSWORD:-}" && ! test -s "$ADA_ROOT/state/ozstar-transfer-password"; then
  read -r -s -p "Ozstar password: " TRANSFER_PASSWORD
  printf '\n'
  export TRANSFER_PASSWORD
fi
```

SSH keys/interactive SSH prompts also work without a password source. Unset
`TRANSFER_PASSWORD` after the last pull if entered through the environment.

| Pull CLI option | Default | Purpose |
| --- | --- | --- |
| `--all` or repeated `--batch UCS1501-1550` | One is required | Select all complete observations or named 50-source folders |
| `--dry-run` | Off | Read/hash inventory and report needed files; no transfer or ledger/receipt write |
| `--source-root` | `/fred/oz299/qhuang/ASKAP-UCDs/products` | Ozstar product root; must match the actual processing workspace |
| `--ada-root` | `/import/ada1/qhua0119/Combine_ImagePlot/Ozstar` | All Ada outputs must stay under this permitted tree |
| `--database` | `<ada-root>/state/transfer_ledger.sqlite` | Ada transfer ledger |
| `--receipt-remote-root` | `/fred/oz299/qhuang/ASKAP-UCDs/state/transfer_receipts` | Ozstar receipt directory; choose the matching production tree |
| `--method rsync\|scp` | `rsync` | Transport method |
| `--remote-user`, `--remote-host` | `qhuang`, `ozstar.swin.edu.au` | Source SSH identity |
| `--remote-python` | `/usr/bin/python3` | Remote stdlib interpreter; not the module-dependent project venv |
| `--password-file` | `<fixed Ada root>/state/ozstar-transfer-password` | Protected Ozstar password file; `TRANSFER_PASSWORD` env takes precedence |
| `--connect-timeout` | `30` | SSH connection timeout in seconds, not an overall transfer timeout |

`--no-receipt-upload` is a diagnostic escape hatch, not the production workflow.
Do not assume `config.py` SSH variables change this standalone pull tool's defaults;
use its CLI options. A repeated healthy pull transfers zero product files but
still rechecks hashes and writes an audit attempt/receipt. See [transfer mechanism](REFERENCE.md#6-transfer-and-space-management).

## 7. Optional standalone position download

```bash
"$ASKAP_PYTHON_BIN" ../Download_ASKAP_Visibility_Remote.py
```

First manually edit the script's `RA`, `Dec`, `OPAL_USER`, `casda_filepath`
(use an absolute FRED output path), and optionally `RADIUS_FREQUENCY_MODE`
(`central` or `top`) / `QUERY_LIMIT`. It is not the production controller: its
exposure threshold and query limit differ, it does not submit jobs, and it uses
Astroquery's own interactive login/keyring behavior. Do not run it unchanged
as a replacement for the indexed workflow.
