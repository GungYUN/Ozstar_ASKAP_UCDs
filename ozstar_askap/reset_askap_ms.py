"""Restore an ASKAP MeasurementSet to its pre-DStools-preprocess state.

This helper runs inside the validated DStools container.  It uses the backup
tables/columns written by FixMS 0.4.1, removes only preprocess-derived data,
and leaves the original DATA column untouched.  Any incomplete or unexpected
layout fails closed so the controller can request one fresh CASDA download.
"""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path

from casacore.tables import table, tableexists


def _restore_table_column(target: Path, backup: Path, column: str) -> None:
    if not tableexists(str(backup)):
        return
    with (
        table(str(target), readonly=False, ack=False) as target_table,
        table(str(backup), readonly=True, ack=False) as backup_table,
    ):
        target_table.putcol(column, backup_table.getcol(column))
        target_table.flush()


def reset_ms(ms: Path) -> None:
    """Undo FixMS direction/correlation outputs so preprocess can run again."""

    if not ms.is_dir() or not (ms / "table.dat").is_file():
        raise RuntimeError(f"Not a MeasurementSet directory: {ms}")

    field = ms / "FIELD"
    field_old = ms / "FIELD_OLD"
    feed = ms / "FEED"
    feed_old = ms / "FEED_OLD"

    if tableexists(str(field_old)):
        with (
            table(str(field), readonly=False, ack=False) as current,
            table(str(field_old), readonly=True, ack=False) as original,
        ):
            phase = original.getcol("PHASE_DIR")
            for column in ("PHASE_DIR", "DELAY_DIR", "REFERENCE_DIR"):
                current.putcol(column, phase)
            current.flush()

    _restore_table_column(feed, feed_old, "BEAM_OFFSET")

    with table(str(feed), readonly=False, ack=False) as feed_table:
        columns = set(feed_table.colnames())
        if "INSTRUMENT_RECEPTOR_ANGLE" in columns:
            feed_table.putcol(
                "RECEPTOR_ANGLE",
                feed_table.getcol("INSTRUMENT_RECEPTOR_ANGLE"),
            )
            feed_table.removecols(["INSTRUMENT_RECEPTOR_ANGLE"])
            feed_table.flush()

    with table(str(ms), readonly=False, ack=False) as main_table:
        if "CORRECTED_DATA" in set(main_table.colnames()):
            main_table.removecols(["CORRECTED_DATA"])
            main_table.flush()

    # DStools deliberately refuses preprocess while FIELD_OLD exists. Remove
    # the closed backup tables only after their contents have been restored.
    for backup in (field_old, feed_old):
        if backup.is_dir():
            shutil.rmtree(backup)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("ms", type=Path)
    args = parser.parse_args()
    reset_ms(args.ms)
    print(f"Restored preprocess input state: {args.ms}")


if __name__ == "__main__":
    main()
