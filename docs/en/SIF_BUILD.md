# 4. SIF build (optional)

[Index](../../README.md) · [中文](../zh-CN/SIF_BUILD.md) · [Installation](INSTALLATION.md)

Normal processing uses the accepted `dstools-260925.sif`; skip this page unless
building a replacement. Host-script updates do not require a rebuild. A new
image is a separate candidate, never an in-place change to the accepted image.

## 1. Build inputs and permissions

The recipe is in [`New_dstools_build/`](../../New_dstools_build/). It pins a
DStools **dev snapshot**, Python 3.12, WSClean 3.5, casacore/python-casacore 3.7.1
and NumPy 2.2.6. The precise Python/Boost solve and artifact hashes are frozen
during preparation. It does not automatically follow upstream's latest commit.

| Recipe input | Role |
| --- | --- |
| `versions.env` | Source URLs/commits, dev snapshot hashes, module names, dependencies and build root |
| `prep_sources.sh` | Online login-node preparation, locks and input manifests |
| `askap-dstools.def` | Offline container build definition |
| `build_sif.sbatch` | Offline Slurm compilation/build and ordinary-user checks |
| `patches/0001`–`0006` | Visibility weighting, integrations, Boost.System, offline Radler dependencies, ABI consistency and subprocess handling |

Upload the **entire** recipe, not just four top-level files. The repository does
not vendor the upstream dev snapshot; supply the exact snapshot matching
`versions.env`. Missing patches or mismatched source hashes stop preparation.
Do not invent replacement hashes simply to bypass a failure.

No host root/sudo installation is used. Apptainer uses the site's supported
`--fakeroot` facility; if it is unavailable, stop and ask the site for a supported
build method rather than requesting/assuming privileges you do not have.
Preparation runs online on a login node; compute compilation is completely
offline. Persistent inputs, caches, configuration, logs and images stay below
`/fred/oz299/qhuang/dstools/`; only Slurm's allocated temporary disk is ephemeral.

## 2. Select a new build root and upload

Use a new root for changed versions/source/solve. The example below uses
`/fred/oz299/qhuang/dstools/sif-build-20261013-offline-v1`; do not reuse it if it
already contains a different frozen build.

Before upload, edit the recipe locally:

```bash
cd /Users/qhua0119/Documents/ChatGPT/Ozstar/Ozstar_ASKAP_UCDs/New_dstools_build
nano versions.env
nano build_sif.sbatch
```

Set `BUILD_ROOT` in **both** files to the new root, and change the sbatch
`--output`/`--error` paths to its `logs/` directory. Check `CONTROLLER_PYTHON_BIN`
and module names; the existing host interpreter needs its GCC/Python modules.
Keep all source pins unless deliberately making a new, consistently pinned recipe.

On Mac; SSH/rsync will prompt for the Ozstar password:

```bash
BUILD_ROOT=/fred/oz299/qhuang/dstools/sif-build-20261013-offline-v1
ssh qhuang@ozstar.swin.edu.au \
  "mkdir -p '$BUILD_ROOT/recipe' '$BUILD_ROOT/src/dstools-dev' '$BUILD_ROOT/logs'"
rsync -av --exclude='.DS_Store' \
  /Users/qhua0119/Documents/ChatGPT/Ozstar/Ozstar_ASKAP_UCDs/New_dstools_build/ \
  "qhuang@ozstar.swin.edu.au:$BUILD_ROOT/recipe/"
rsync -av --exclude='__pycache__' --exclude='*.pyc' --exclude='.DS_Store' --exclude='.venv' \
  /Users/qhua0119/Documents/ChatGPT/DStools/dstools-dev/ \
  "qhuang@ozstar.swin.edu.au:$BUILD_ROOT/src/dstools-dev/"
```

This copies into a new build tree, not the host environment or accepted SIF.
The source directory above must contain the exact selected upstream snapshot.

## 3. Login-node preparation

On an Ozstar login node in tmux:

```bash
BUILD_ROOT=/fred/oz299/qhuang/dstools/sif-build-20261013-offline-v1
cd "$BUILD_ROOT/recipe"
set -o pipefail
bash prep_sources.sh 2>&1 | tee "$BUILD_ROOT/logs/preparation.log"
prep_rc=${PIPESTATUS[0]}
echo "Preparation exit code: $prep_rc"
```

Only continue on exit 0 and the explicit preparation-complete message.
Preparation freezes the bootstrap SIF, source/submodules and patches, the full
local APT repository, wheelhouse/combined Python lock, relocatable Python/Boost,
CASA/casacore measures data, Astropy sites registry, and WSClean's FetchContent
dependencies. All caches and the preparation container's home mapping are under
the build root. The recipe does not install packages into the real host system.

After any authorized recipe edit, re-run preparation before checking/submitting:

```bash
sha256sum -c --quiet "$BUILD_ROOT/manifests/build-input-files.sha256"
```

Do not ignore a mismatch or hand-edit the manifest to bless unknown inputs.
The sbatch script itself is frozen too; editing its resources changes its hash.

## 4. Offline compute build

From a login node after preparation succeeds:

```bash
cd "$BUILD_ROOT/recipe"
sbatch build_sif.sbatch
```

The checked-in resource request is 8 CPUs, 32G RAM, 32G tmp, 12 hours.
Compilation and image assembly belong on compute resources, not a login node.
It uses local inputs only: local `.deb` repository, offline wheels, source/data
archives and the packed Python environment. No compute-node network access is required.

Alternatively, for an interactive build request **exactly**:

```bash
sinteractive --time=02:00:00 --ntasks=1 --cpus-per-task=8 --mem=32G --tmp=32G
```

On the allocated node:

```bash
BUILD_ROOT=/fred/oz299/qhuang/dstools/sif-build-20261013-offline-v1
build_interactive() {
  test -n "${JOBFS:-}" || { echo "JOBFS is required"; return 1; }
  cd "$BUILD_ROOT/recipe" || return 1
  set -o pipefail
  SLURM_TMPDIR="$JOBFS" TMPDIR="$JOBFS" bash build_sif.sbatch 2>&1 \
    | tee "$BUILD_ROOT/logs/build-interactive-${SLURM_JOB_ID}.log"
  local rc=${PIPESTATUS[0]}
  echo "Build exit code: $rc"
  return "$rc"
}
if build_interactive; then
  echo "Build command: PASS"
else
  echo "Build command: FAIL; keep the logs and stop."
fi
```

When run through Bash, `#SBATCH` headers are comments; the allocation's two-hour
limit applies, not the script's 12-hour request. `JOBFS` bridges the site scratch
path into Apptainer's temp settings; MS/science products are not moved there.

## 5. Candidate and adoption

Images/logs live in `artifacts/` and `logs/`; `manifests/candidate-sif.sha256`
identifies the candidate. Its filename includes source/input fingerprints, and
the build refuses to overwrite an existing candidate. Ordinary-user imports
and subprocess checks are part of the build; their failure is not success.

```bash
module load apptainer
sha256sum -c "$BUILD_ROOT/manifests/candidate-sif.sha256"
CANDIDATE=$(awk 'NR==1 {print $2}' "$BUILD_ROOT/manifests/candidate-sif.sha256")
apptainer exec --cleanenv --no-home "$CANDIDATE" \
  /opt/askap/bin/python -c 'import casacore.tables; from dstools.utils import LOCATIONS; print(sorted(LOCATIONS))'
apptainer exec --cleanenv --no-home "$CANDIDATE" \
  /opt/askap/bin/python -c 'from casatasks import tclean,phaseshift,uvsub,mstransform; print("CASA OK")'
apptainer exec --cleanenv --no-home "$CANDIDATE" /opt/wsclean/bin/wsclean --version
```

Keep python-casacore and CASA imports in **separate processes**. OpenBLAS is
single-threaded; this does not disable WSClean's own threads. Common casacore and
consistent C++/OpenSSL libraries, readable offline runtime data and portable CPU
options are intentional correctness/compatibility settings.

A new build passing internal checks still needs an isolated real-MS read and
end-to-end processing check before production adoption. Existing accepted images
do not need those old test steps repeated merely to read this guide. Preserve
the accepted SIF; copy a verified replacement to a distinct versioned name,
verify its copy hash, then explicitly select it with `DSTOOLS_CONTAINER` for new
plans. Do not change an active plan's image.
