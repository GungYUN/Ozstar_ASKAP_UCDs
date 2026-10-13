# 3. How it works

[Index](../../README.md) · [中文](../zh-CN/REFERENCE.md) · [Commands and parameters](USAGE.md)

This page explains the current **shared mode**. Adjustable defaults and commands
are centralized in Usage; this is not another runbook or a list of past tests.

## 1. Responsibilities

```text
Ozstar login controller
  full-catalogue index → product groups → archive/checksum download → sbatch
                                      ↑                              ↓
                         next batch / bounded continuation ← outcome/state

Offline compute job: independent observation lanes in parallel
  each observation: extract → master preprocess/create → serial target copies
  each target: insert → subtract → extract DS → crop I/V → copy products

Ada login tool: snapshot → incremental pull → SHA256 → ledger + Ozstar receipt
```

One shared compute job is active at a time for a work tree; its lanes process
independent observation groups, with multiple rounds per lane when appropriate.
The login controller must stay alive to submit the next batch. A submitted
compute job continues if the login shell disconnects or the controller stops.

## 2. Query, eligibility and product identity

Each catalogue source is queried independently, including sources outside the
seed range. A one-degree search is followed by public/quality, exposure, size,
excluded-SB and frequency-dependent radius filters. The nearest qualifying beam
is selected for each SB. Spectral-type eligibility controls processing, not
whether a source is queried or shown in the index.

ObsCore `em_min`/`em_max` are wavelength bounds, converted to metres. With
`c=299792458 m/s`:

```text
frequency_high = c / em_min
frequency_low  = c / em_max
frequency_central = (frequency_high + frequency_low) / 2
radius = (reference_FWHM / 2) × reference_frequency / chosen_frequency
```

The radius is **half** the full FWHM, with consistent frequency units. Central
uses the arithmetic frequency midpoint, not `c / mean(wavelength)`; top uses
the high-frequency bound. Invalid/truncated metadata cannot silently become a
successful empty query. Historical indexes without a frequency-mode marker are
interpreted as top, never relabeled central.

Shared identity includes SB, beam, full CASDA filename, publisher DID and band
bounds. RACS `SB...beam...` and HIPASS `SBxxxxx_Fxx_Bxx` names are recognized;
HIPASS B maps to beam, while F remains in the filename/identity. Nearby sky
positions alone are insufficient to justify sharing.

Seed bounds determine order and the observations to start. Matching eligible
partners from the entire index join those observations, even outside the range;
the partners' other observations do not recursively expand the run.

## 3. Shared master and target isolation

For an observation, the login node passes its **whole staged URL list** (archive
and checksum) to one `download_files()` call. Signed URL encoding is preserved.
After download, tar member names establish the staged MS count; each shared
data product must contain exactly one MS. This is not checksum/content validation.

The compute worker uses command-line tar, retains the checksum, and removes
the archive after successful extraction. Tar paths/links are checked for unsafe
targets before extraction. Public inputs are under `staging/_shared/<model_key>/`:

- `extracted/`: unmodified raw MS.
- `master/`: a real copy, preprocessed before create-model; full uncropped model.
- `wsclean_tmp/`: temporary imaging data for that observation.

The model key includes product identity, SIF SHA256 and imaging settings.
Each target receives an independent copy of the **preprocessed master MS** and
complete model files, not the raw MS and not hard links. Target insertion,
subtraction, corrected columns, masks and cutouts therefore cannot affect another
target or the master. The master is never passed to insert/subtract/crop.

Targets run serially within one observation. I and V are cropped at each target's
proper-motion-corrected coordinates, using CASDA `t_min`, then the legacy CSV,
then the configured final fallback. A reusable master avoids new imaging on a
continuation. Explicit failure retries instead rebuild a private model.

Preprocess retains the `FIELD_OLD` precheck where applicable; a nonzero DStools
exit fails that target/step rather than blindly continuing. File-existence
checkpoints skip already available named products. No `.subtracted.complete`
marker, `.ds.part` promotion or separate DS/FITS format-validation gate is added.
FITS cropping itself necessarily reads the image.

## 4. Time, parallelism and recovery

Estimate unfinished work after archive preparation, not only from CASDA row
counts. Each observation contributes model work for its staged MS plus work for
pending targets; a valid retained master removes the model-work term. Groups
are assigned to the least-loaded lane. Request time is the busiest lane's sum
plus whole-job overhead, capped by the configured limit. Packing may split
observations or targets across batches. It is not always the maximum request.

Each lane owns independent paths/process state. SQLite writes are short
transactions with locking; large input-copy space checks/copies are serialized
by `shared-copy.lock`, while imaging and target processing remain parallel.
Whole-job RAM is budgeted across actual lanes, not hard-isolated per lane.
The WSClean wrapper fills absent thread/memory options; create-model leaves
headroom for Python/MS operations. OpenBLAS stays single-threaded without
limiting WSClean's own `-j` threads. Temporary MS/model data stays on FRED.

All lanes use the same job deadline. Near the reserve, worker exit 75 records
partial work; the login controller makes bounded continuation submissions,
skipping finished/failed targets. Workers do not submit their own shared jobs.

Completion is based on manifests, `.worker_finished`/`.worker_failure` and
separate `.submission.json` acknowledgments. Shared mode does not use heartbeat
or resubmit-pending files. If a killed worker leaves no terminal outcome,
two consecutive explicit terminal `sacct` states can record abnormal failure;
an empty queue or failed accounting lookup cannot certify completion.

An exclusive controller lock prevents concurrent ownership of a work tree.
The existence of its file is not evidence of a held lock, and unlinking a busy
file can bypass protection without releasing the old lock. Resume the original
plan/settings, not a new speculative submission. An interruption between sbatch
and durable acknowledgment is deliberately treated as ambiguous and needs inspection.

Ordinary observational failures are recorded and do not stop other targets,
lanes or later seeds, even if a source's observations all fail. Reliable-state
failures (for example an unwritable database) stop progression; they are not
silently converted into successful or recoverable scientific outcomes.

## 5. State and cleanup

| Under `ASKAP_WORK` | Meaning |
| --- | --- |
| `state/crosscatalogue_sources.csv` | Every catalogue source, eligibility, query status/counts/note |
| `state/crosscatalogue_observations.csv` | Selected source-observation associations, frequency/radius and sharing information |
| `state/crosscatalogue-summary.json` | Query categories and association/group change counts |
| `state/crosscatalogue.json` | Published machine index; do not edit by hand |
| `state/crosscatalogue-update.json` | Update/resume checkpoint |
| `state/crosscatalogue_rows/<revision>/` | Immutable ECSV rows and per-source query checkpoints |
| `state/shared-controller.json`, `state/jobs/` | Frozen plan, manifests, generated sbatch and submission/outcome signals |
| `state/sb_processing.sqlite` | One row per source/SB/beam; steps, errors, timings, model origin and retry counters |
| `state/sources/`, `logs/` | Source summaries, Slurm logs and retained failure commands/output |

Query states are `available`, `no_observations`, `query_failed`, `unknown`.
Failed updates may retain old rows for inspection, but failed/unknown sources'
old results are not used for current scheduling. CSV notes are columns, not
free text prepended before the header. A query update can remove associations
or change the nearest beam; it is not append-only and never deletes science products.
Active plans freeze an index revision and reject updates until finished.

Step flags are `Y` / `N`, or NULL for an unrecorded step. Failure logs retain
the command and output; routine successful detailed step logs are discarded.
Shared preprocess/create flags may be Y with zero target time because the work
was performed once on the master; shared timings remain in its manifest.

Outputs remain:

```text
products/UCSxxxx-yyyy/UCSxxxx/LongObs/SBxxxxx_beamyy/
  SBxxxxx_beamyy.ds
  wsclean_model/wsclean-MFS-I-image.fits
  wsclean_model/wsclean-MFS-V-image.fits
  observation.json
```

Completion requires the DS, I and V **filenames** plus matching product identity.
It does not imply scientific quality. Old products without sidecars need
matching SQLite filename evidence; a conflicting product is not overwritten.
Controller completion means the planned work has been handled, not zero failures
or completion of every other observation belonging to out-of-range partners.

A target's MS is cleaned only after its triple is safely copied to products.
Public inputs/models are released only when all indexed eligible partners are
complete. Failed/unfinished targets keep inputs. Replaced partial target folders
may be preserved as `.previous-*`; those backups are not auto-deleted.
The explicit retry budget is one reset/preprocess/private rebuild, then one
fresh download/processing attempt, then `exhausted`; partner products are untouched.

## 6. Transfer and space management

Ada builds an Ozstar snapshot of complete triples, including SHA256/size for
each file. Missing/incomplete triples are not transferable. The scanner expects
exactly the four product directory levels shown above; do not supply a parent
that adds an extra test-case directory level. An empty inventory can return
PASS with zero transfers; it does not certify successful scientific processing.
Do not run simultaneous pulls into the same Ada product tree/ledger. Separate
source projects should use separate Ada subtrees/ledgers when isolation is needed.

Only missing/hash-mismatched files enter the rsync list. `--ignore-times` forces
those selected repairs even if size/mtime match; healthy files are excluded.
After pull, local hashes and source stability are checked. The Ada ledger marks
`complete` only with three Y statuses and a verified uploaded receipt in the
normal workflow. Repeated pulls update audit history, not duplicate observations.

Ada keeps `state/transfer_ledger.sqlite`, manifests and immutable receipts under
its configured root; verified receipts are uploaded to the selected Ozstar tree.
These are distinct from the processing database. Transfer copies only DS/I/V,
not the provenance sidecar, raw MS or full model. Keep Ozstar provenance/state
for processing recovery and scientific traceability.

No automatic post-transfer deletion is implemented. Before any manual cleanup,
identify the exact source/SB/beam, require all three verified files and a matching
receipt, and retain adequate scientific/state backups. A controller PASS alone
does not authorize clearing products or all of staging.
