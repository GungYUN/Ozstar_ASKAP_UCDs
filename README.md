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
- [SIF build instructions](New_dstools_build/README.md)

Run and deploy the pipeline from `ozstar_askap/`.
SIF construction is only needed when building or updating the container.

## Baseline

This snapshot includes frequency-dependent ASKAP cross-matching.
Full-catalogue observation indexing and shared-model processing
have not yet been implemented.

The tag `pre-shared-model-20261002` preserves the original
pre-refactor backup. Older root-level pipeline files remain
recoverable from Git history.
