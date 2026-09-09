# Ozstar ASKAP/UCD pipeline

This directory is the Ozstar-side implementation described in
`../Ozstar_ASKAP_Migration_Plan.md`.

The persistent work root is:

```text
/fred/oz299/qhuang/ASKAP-UCDs/
```

The program directory, catalogues, staging data, final products and state
files are all below that root.  The pipeline does not use `$TMPDIR` for MS
storage.

## Files

| File | Purpose |
|---|---|
| `ozstar_main.py` | Query TAP, pack sources, write a manifest and submit Slurm |
| `process_job.py` | Compute-node worker: download, extract, DStools, crop and checkpoint |
| `casda_query.py` | CASDA filtering, staging and safe per-file download |
| `crop_fits.py` | 10 arcmin FITS crop with a valid celestial WCS |
| `transfer_products.py` | Password-assisted SCP/rsync in either direction |
| `transfer_to_ada.sh` | Convenience wrapper for Ozstar → ada |
| `transfer_to_ozstar.sh` | Convenience wrapper for ada → Ozstar |
| `deploy_to_ozstar.sh` | Copy this program directory to the Ozstar work root |
| `prepare_layout.sh` | Create the persistent Ozstar directory layout |
| `setup_host_python.sh` | Create a venv host Python environment on Fred |
| `config.py` | Central paths and resource settings |

## Initial layout

Copy the two CSV files to the default locations, or set `ASKAP_CATALOGUE` and
`ASKAP_TIME_CSV` in the environment:

```text
/fred/oz299/qhuang/ASKAP-UCDs/catalogue/
    UltracoolSheet_Main_index_unbinaryUCD.csv
    All_combine_data_unrepetition.csv
```

Create the directories before the first run:

```bash
./prepare_layout.sh
```

To deploy/update this program directory from ada or a local Linux/macOS host,
run `deploy_to_ozstar.sh`.  It uses the same mode-600 Ozstar password file and
does not put the password in the SCP command:

```bash
./deploy_to_ozstar.sh
```

The DStools container must exist at:

```text
/fred/oz299/qhuang/dstools/dstools-v2.0.0.sif
```

The host Python environment needs `numpy`, `pandas`, `astropy`, `h5py` and
`astroquery`.  DStools, casacore and WSClean are provided by the Apptainer
container.  Create a venv on Fred with:

```bash
./setup_host_python.sh
module load python/3.12.3
export ASKAP_PYTHON_MODULE=python/3.12.3
export ASKAP_PYTHON_BIN=/fred/oz299/qhuang/ASKAP-UCDs/.venv/askap-python/bin/python
```

The script only executes `module load`; it does not run `module purge`,
`module unload` or remove any existing environment.  If the current shell
already has the `mamba` module loaded and the Python module reports a conflict,
start a new login shell and run the script there without loading `mamba`.
Always use the absolute venv interpreter, or set `ASKAP_PYTHON_BIN` to another
prepared interpreter, before running `ozstar_main.py`.  The generated Slurm
script loads the configured Python module and checks the imports before starting
a job.

## Credentials

No password is stored in the program.  CASDA uses a mode-600 file containing
only the CASDA password, or the `CASDA_PASSWORD` environment variable:

```text
~/.config/askap/casda-password
```

For SSH transfer, `sshpass` is required.  The recommended setup is a
mode-600 password file on the machine initiating each direction:

```bash
mkdir -p ~/.config/askap
read -r -s TRANSFER_PASSWORD
printf '%s\n' "$TRANSFER_PASSWORD" > ~/.config/askap/transfer-password
unset TRANSFER_PASSWORD
chmod 600 ~/.config/askap/transfer-password
```

The file used for each direction can be changed with:

```bash
export ADA_PASSWORD_FILE="$HOME/.config/askap/ada-password"
export OZSTAR_PASSWORD_FILE="$HOME/.config/askap/ozstar-password"
```

`to-ada` uses the password for the ada account.  `to-ozstar` uses the
password for the Ozstar account.  As an alternative, set
`TRANSFER_PASSWORD` only for the lifetime of the transfer command.  The
password is passed to `sshpass` without appearing in the SCP command line or
the program log.

SSH host keys should be accepted and verified manually once before an
automated transfer.  The transfer tool intentionally does not disable host
key checking.

## Plan and submit a job

Run on the Ozstar login node.  First use a dry plan:

```bash
cd /fred/oz299/qhuang/ASKAP-UCDs/ozstar_askap
python3 ozstar_main.py --begin 1 --end 50 --no-submit
```

When the manifest is correct, submit it:

```bash
python3 ozstar_main.py --begin 1 --end 50 --submit
```

The same range can be set at the top of `ozstar_main.py` by changing
`UCS_START`, `UCS_END` and `SUBMIT`.  `--retry-existing` is required when a
previous `complete`, `queued` or `running` state should deliberately be
replanned.

The default packer uses 4 slots × 8 CPU, estimates 4 hours per observation,
targets 40 observations and requests 48 hours.  These values are all in
`config.py` and can be overridden with environment variables.

Each source is downloaded in waves of at most four observations.  A wave is
processed before the next wave is downloaded.  Therefore a source with many
observations does not leave all of its MS files in `staging/` at once.

## Transfer products to ada

Run on Ozstar after the Slurm job has produced products:

```bash
cd /fred/oz299/qhuang/ASKAP-UCDs/ozstar_askap
./transfer_to_ada.sh --batch UCS1-50
```

Multiple batches can be transferred in one invocation:

```bash
./transfer_to_ada.sh --batch UCS1-50 --batch UCS51-100
```

The default source is `products/`; the default ada destination is:

```text
/import/ada1/qhua0119/Visibility/
```

Only `.ds` files and the two WSClean MFS image names are transferred and
verified.  Verification compares every relative product path and SHA-256
digest; the source is retained unless `--delete-source` is supplied:

```bash
./transfer_to_ada.sh --batch UCS1-50 --delete-source
```

Deletion happens only after the remote product paths and SHA-256 digests match
the local batch.
For Ozstar → ada deletion, every source in the batch must also have a
`complete` state in `state/sources/`.  Reverse-transfer deletion is refused by
default because ada has no Ozstar worker state; only use
`--allow-delete-without-state` after manual verification.

## Reverse transfer

The same code can copy ada-compatible batch trees back to Ozstar.  Run the
program on ada, with this directory available there:

```bash
python3 transfer_products.py \
  --direction to-ozstar \
  --source-root /import/ada1/qhua0119/Visibility \
  --batch UCS1-50
```

The default remote destination is
`/fred/oz299/qhuang/ASKAP-UCDs/products/`.  Use `--remote-root` to select a
different Ozstar directory.

## ada plotting

After transfer, confirm the expected tree exists under ada's `Visibility/`
directory.  Run the existing ada main with download, decompression and
DStools disabled, and plotting/combining enabled:

```python
STEPS = {
    "download_visibility": False,
    "decompress_move": False,
    "dstool_process": False,
    "plot_images": True,
    "combine_images": True,
}
```

The 10 arcmin WCS-preserving images are sufficient for the existing 1 arcmin,
3 arcmin and 60 arcsec plotting products.

## State and recovery

State is stored below `state/`:

```text
state/tap_cache/       TAP ECSV results
state/sources/         per-source status and SB checkpoints
state/jobs/             manifests and generated sbatch scripts
```

An existing `.ds` plus Stokes-I FITS product is treated as complete for that
SB.  Failed SB directories remain in `staging/` for a later retry.  A job
that reaches its walltime reserve exits with `partial`; the next manifest can
resume the same source.
