# Ozstar ASKAP UCD Pipeline

ASKAP visibility processing on Ozstar, with product retrieval from HPC-ada.

## Repository layout

- `ozstar_askap/`: current processing pipeline, deployment scripts,
  validation programs, retry handling and product transfer tools.
- `New_dstools_build/`: DStools SIF build recipes, version configuration
  and compatibility patches.
- `Download_ASKAP_Visibility_Remote.py`: standalone downloader for
  manually testing a single sky position.

## Documentation

- [Pipeline instructions](ozstar_askap/README.md)
- [Shared-observation processing and isolated validation](ozstar_askap/SHARED_PROCESSING.md)
- [Gate8 central-frequency small-sample validation](ozstar_askap/GATE8_CENTRAL_VALIDATION.md)
- [Gate9 shared-retry and controller-resume validation](ozstar_askap/GATE9_SHARED_RETRY_VALIDATION.md)
- [Gate10 shared-product Ada pull validation](ozstar_askap/GATE10_SHARED_TRANSFER_VALIDATION.md)
- [SIF build instructions](New_dstools_build/README.md)

Run and deploy the pipeline from `ozstar_askap/`.
SIF construction is only needed when building or updating the container.

Local observation-table downloads, review and comparisons belong under
`/Users/qhua0119/Documents/ChatGPT/Ozstar/观测记录表格/` (including dated subdirectories).
This local review location does not change the Ozstar pipeline's runtime index paths.

## Baseline

The new default controller includes full-catalogue observation indexing,
frequency-dependent cross-matching and shared-model processing. The
default radius uses the arithmetic central frequency; retain
`--radius-frequency-mode top` or `central` for per-run overrides. Historical
top indexes stay identified as top and are not silently reused as central.
Concurrency defaults to four observation slots (eight CPUs each), with a configurable
`--parallel-slots` switch and a 48-hour walltime cap; requests use slot estimates.
Default Slurm memory is 160G total (40G per slot with four slots).
Its local regression suite uses simulated CASDA/Slurm/DStools calls; a real isolated
Ozstar validation is still required before production deployment.

The tag `pre-shared-model-20261002` preserves the original
pre-refactor backup. Older root-level pipeline files remain
recoverable from Git history.
