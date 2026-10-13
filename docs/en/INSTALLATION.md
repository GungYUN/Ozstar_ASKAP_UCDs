# 1. Installation

[Index](../../README.md) · [中文](../zh-CN/INSTALLATION.md) · [Next: Usage](USAGE.md)

## 1. Requirements

- Ozstar: Linux x86_64, Slurm, Apptainer, GNU tar and persistent FRED space.
  Login nodes query/download; compute workers must not require internet access.
- Host controller: GCC 13.3.0 / Python 3.12.3 modules, plus `numpy`, `pandas`,
  `astropy`, `astroquery`, `keyring`.
- Accepted SIF: `/fred/oz299/qhuang/dstools/dstools-260925.sif`, containing
  DStools dev, WSClean 3.5 and compatible casacore/CASA. Building another SIF
  is optional; see [SIF build](SIF_BUILD.md).
- Deployment workstation: SSH/scp and `sshpass` for the deployment wrapper.
- Ada: Python 3.7+, SSH, rsync, sshpass and sha256sum. The pull tool uses only
  the standard library; retrieval does not require DStools on Ada.

Examples use this account's paths; adjust them consistently for another account.
Code goes to `ASKAP-UCDs-shared/ozstar_askap`; **new production data** goes to
`ASKAP-UCDs-production`. This explicitly overrides the runtime code default
`/fred/oz299/qhuang/ASKAP-UCDs`, avoiding old/test state. No existing data is moved.

## 2. Deploy from Mac to Ozstar

Run from a complete checkout:

```bash
cd /Users/qhua0119/Documents/ChatGPT/Ozstar/Ozstar_ASKAP_UCDs/ozstar_askap
OZSTAR_WORK=/fred/oz299/qhuang/ASKAP-UCDs-shared bash deploy_to_ozstar.sh
```

Without a credential file/environment override, the wrapper prompts for a
hidden Ozstar password and does not save it. An existing password file must
have mode 600. It uploads this program directory and the parent standalone
downloader, not external catalogues/runtime data. Keep local data and secrets
outside the code directory. Do not overwrite code used by running jobs.
Deploy `wsclean_slot/` together with its executable permission.

Deployment options are in [Usage §1](USAGE.md#1-session-environment-and-common-settings).
`OZSTAR_WORK` is the deployment root; `ASKAP_WORK` is the runtime data root.

## 3. Inputs and the accepted SIF

Use the existing catalogues, or supply equivalent files and adjust the paths:

```text
/fred/oz299/qhuang/ASKAP-UCDs/catalogue/UltracoolSheet_Main_index_unbinaryUCD.csv
/fred/oz299/qhuang/ASKAP-UCDs/catalogue/All_combine_data_unrepetition.csv
```

The source catalogue requires `UCS`, `UCS_name`, `ra_j2000_formula`,
`dec_j2000_formula`, `pmra_formula`, `pmdec_formula`, `sptnumabs_formula`.
Do not silently substitute the repository's older catalogue snapshot.
The epoch CSV is a fallback when CASDA has no valid `t_min`.

On an Ozstar login node:

```bash
module load apptainer
sha256sum -c /fred/oz299/qhuang/dstools/dstools-260925.sif.sha256
apptainer exec --cleanenv --no-home \
  /fred/oz299/qhuang/dstools/dstools-260925.sif \
  /opt/wsclean/bin/wsclean --version
```

Expect a successful hash check and WSClean 3.5. Updating host scripts does not
require replacing the accepted SIF.

## 4. Host Python and runtime directories

Try the existing controller interpreter first, without upgrading it:

```bash
module load gcc/13.3.0
module load python/3.12.3
/fred/oz299/qhuang/ASKAP-UCDs/.venv/askap-python/bin/python -c \
  'import numpy,pandas,astropy,astroquery,keyring; print("Host Python OK")'
```

Only if a new host environment is needed:

```bash
cd /fred/oz299/qhuang/ASKAP-UCDs-shared/ozstar_askap
ASKAP_WORK=/fred/oz299/qhuang/ASKAP-UCDs-production bash setup_host_python.sh
```

This installs host dependencies, not DStools/casacore/WSClean, and loads modules
without purging/unloading them. For a new environment, use the printed
interpreter path in Usage instead of the existing interpreter shown there.

Create the persistent layout:

```bash
cd /fred/oz299/qhuang/ASKAP-UCDs-shared/ozstar_askap
ASKAP_WORK=/fred/oz299/qhuang/ASKAP-UCDs-production bash prepare_layout.sh
```

Check personal/project space and inode quotas, not just `df`. MS, model copies,
checkpoints and WSClean temporary data stay under FRED; Slurm tmp is small scratch.

## 5. CASDA credentials on Ozstar

In the controller shell, privately enter the CASDA password:

```bash
read -r -s -p "CASDA password: " CASDA_PASSWORD
printf '\n'
export CASDA_PASSWORD
```

The default username is `qhua0119@uni.sydney.edu.au`. Alternatively set
`CASDA_PASSWORD_FILE` to a protected, nonsymlink file containing only the
password; its default is `$HOME/.config/askap/casda-password`. Never put secrets
in source, arguments, Git or logs. Do not enable `set -x` around credentials.
Only the login node needs CASDA credentials. Re-enter an environment-only
password after reconnecting; another host's tmux does not restore your shell.

## 6. Install the pull script on Ada

On Ada, retrieve the deployed script; SSH prompts for the Ozstar password:

```bash
ADA_ROOT=/import/ada1/qhua0119/Combine_ImagePlot/Ozstar
mkdir -p "$ADA_ROOT/bin"
scp -4 \
  qhuang@ozstar.swin.edu.au:/fred/oz299/qhuang/ASKAP-UCDs-shared/ozstar_askap/pull_askap_products.py \
  "$ADA_ROOT/bin/"
cd "$ADA_ROOT/bin"
python3 -m py_compile pull_askap_products.py
python3 pull_askap_products.py --help
sha256sum pull_askap_products.py
```

Compare the script hash with the workstation/Ozstar copy. Its rsync command
must contain `--ignore-times` for the already hash-selected repair list.
Ada connects to Ozstar, not the reverse. Use an SSH key, an existing mode-600
`$ADA_ROOT/state/ozstar-transfer-password` file, or the private prompt in
[Usage §6](USAGE.md#6-pull-all-complete-products-on-ada).

## 7. Continue

Follow [Usage](USAGE.md) to restore the environment, build the index, submit a
small initial production range and retrieve outputs. Do not copy test databases,
fault configurations or controller plans into production.
