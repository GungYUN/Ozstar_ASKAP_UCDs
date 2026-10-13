"""Full-catalogue selection on the login node; CSV exports for TOPCAT.

Each successful query writes a new immutable ECSV. A published JSON snapshot
indexes those files; workers receive their own frozen manifest. No downloads,
processing-state resets, or target-neighbour radius searches happen here.
"""
from __future__ import annotations

import csv
import hashlib
import json
import logging
import math
import os
import re
import time
from collections import Counter, defaultdict
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

try:
    from . import config, casda_query
    from .pipeline_utils import atomic_write_json, read_json, utc_now, sb_beam_from_name
except ImportError:
    import config, casda_query
    from pipeline_utils import atomic_write_json, read_json, utc_now, sb_beam_from_name

logger = logging.getLogger("askap.crosscatalogue")
SCHEMA = 1


@contextmanager
def exclusive_lock(path: Path, *, wait: bool = False):
    """Prevent concurrent publishers/controllers; OS releases lock on exit."""
    import fcntl
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | (0 if wait else fcntl.LOCK_NB))
        except BlockingIOError as exc:
            raise RuntimeError(f"Another process owns {path}") from exc
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def digest_file(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            value.update(chunk)
    return value.hexdigest()


def fingerprint(value) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, allow_nan=False).encode()).hexdigest()


def text(value) -> str:
    return value.decode() if isinstance(value, bytes) else str(value)


def number(value):
    try:
        result = float(value)
        return result if math.isfinite(result) else None
    except (ValueError, TypeError):
        return None


def observation(row) -> dict:
    """A key identifies the actual data product, not just the SB/beam."""
    from urllib.parse import unquote
    filename = unquote(text(row["filename"]))
    identity_tokens = sb_beam_from_name(filename)
    if not identity_tokens:
        raise ValueError(f"Cannot identify a safe SB/beam filename: {filename}")
    sb, beam = identity_tokens
    if text(row["obs_id"]).split("-")[-1] != sb[2:]:
        raise ValueError(f"obs_id/filename mismatch: {filename}")
    identity = {
        "sb_id": sb, "beam": beam, "filename": filename,
        "publisher_did": text(row["obs_publisher_did"]) if "obs_publisher_did" in row.colnames else "",
        "em_min_m": number(row["em_min"]), "em_max_m": number(row["em_max"]),
    }
    # ObsCore mandates metres; honour explicit column units in imported tables.
    for column in ("em_min", "em_max"):
        # Slicing the parent preserves column units; Table(rows=[row]) does not.
        identity[column + "_m"] = float(casda_query._wavelength_values_metres(
            row.table[row.index:row.index + 1], column)[0])
    return {
        **identity, "key": fingerprint(identity), "sb_name": f"{sb}_{beam}",
        "estimated_bytes": int(float(row["access_estsize"]) * 1024),
        "t_min": number(row["t_min"]) if "t_min" in row.colnames else None,
    }


def selection_signature() -> dict:
    return {
        "schema": SCHEMA, "catalogue_sha256": digest_file(config.CATALOGUE_PATH),
        "selection": casda_query._cache_selection_metadata(0, 0),
        "max_file_size_kb": config.CASDA_MAX_FILE_SIZE_KB,
        "query_limit": config.CASDA_QUERY_LIMIT,
        "spt_threshold": config.SPTNUM_THRESHOLD,
    }


def _write_csv(path: Path, columns: list[str], rows: list[dict]) -> None:
    temporary = path.with_suffix(".csv.tmp")
    with temporary.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    os.replace(temporary, path)


def export_tables(snapshot: dict) -> dict:
    """Both CSVs always include query status; failed queries are never empty successes."""
    sources = snapshot["sources"]
    counts = Counter(item["query_status"] for item in sources)
    summary = {key: counts[key] for key in ("available", "no_observations", "query_failed", "unknown")}
    mode = snapshot.get("signature", {}).get("selection", {}).get("askap_radius_frequency_mode", "top")
    summary.update(total_sources=len(sources), revision=snapshot["revision"],
                   radius_frequency_mode=mode,
                   previous_results_retained=sum(bool(s.get("using_previous_result")) for s in sources),
                   added_observations=sum(s.get("added_count", 0) for s in sources if s.get("query_status") == "available"),
                   removed_observations=sum(s.get("removed_count", 0) for s in sources if s.get("query_status") in {"available", "no_observations"}))
    shared = defaultdict(list)
    for item in sources:
        if item["query_status"] == "available" and item.get("eligible"):
            for obs in item.get("observations", []):
                shared[obs["key"]].append(item["source_name"])
    summary.update(observation_groups=len(shared),
        shared_observation_groups=sum(len(names) > 1 for names in shared.values()),
        sources_with_shared_observations=len({name for names in shared.values() if len(names) > 1 for name in names}),
        max_shared_sources=max((len(names) for names in shared.values()), default=0))
    source_rows, obs_rows = [], []
    for item in sources:
        source_rows.append({**item, "ra_deg": item.get("ra"), "dec_deg": item.get("dec"),
                            "index_revision": snapshot["revision"]})
        for obs in item.get("observations", []):
            # Old published indexes were top-only. Never label retained old
            # observations as central merely because this update used central.
            frequency_fields = casda_query.frequency_selection(
                obs["em_min_m"], obs["em_max_m"], obs.get("radius_frequency_mode", "top"))
            obs_rows.append({**frequency_fields, **obs, "source_name": item["source_name"],
                "ucs_number": item["ucs_number"], "query_status": item["query_status"],
                "eligible": item.get("eligible", False),
                "using_previous_result": item.get("using_previous_result", False),
                "index_revision": snapshot["revision"],
                "shared_source_count": len(shared[obs["key"]]),
                "shared_sources": ";".join(shared[obs["key"]])})
    root = config.STATE_ROOT
    _write_csv(root / "crosscatalogue_sources.csv", [
        "source_name", "ucs_number", "ra_deg", "dec_deg", "eligible", "query_status",
        "candidate_count", "selected_count", "added_count", "removed_count", "query_time_utc", "last_success_time_utc",
        "using_previous_result", "query_note", "index_revision"], source_rows)
    _write_csv(root / "crosscatalogue_observations.csv", [
        "source_name", "ucs_number", "sb_id", "beam", "filename", "publisher_did", "key",
        "em_min_m", "em_max_m", "frequency_min_ghz", "frequency_max_ghz", "separation_deg",
        "frequency_central_ghz", "radius_frequency_mode", "radius_frequency_ghz",
        "half_power_radius_deg", "t_min", "estimated_bytes", "shared_source_count", "shared_sources",
        "query_status", "eligible", "using_previous_result", "index_revision"], obs_rows)
    atomic_write_json(root / "crosscatalogue-summary.json", summary)
    logger.info("Crosscatalogue: total=%d available=%d no_observations=%d query_failed=%d unknown=%d; previous_results_retained=%d",
        summary["total_sources"], summary["available"], summary["no_observations"],
        summary["query_failed"], summary["unknown"], summary["previous_results_retained"])
    logger.info("Observation groups=%d; shared groups=%d; sources participating in sharing=%d; largest group=%d",
        summary["observation_groups"], summary["shared_observation_groups"],
        summary["sources_with_shared_observations"], summary["max_shared_sources"])
    return summary


def _restore_query_checkpoints(snapshot: dict) -> dict:
    """Hydrate the main snapshot from durable per-source query journals."""
    root = config.STATE_ROOT / "crosscatalogue_rows" / snapshot["revision"]
    for index, source in enumerate(snapshot["sources"]):
        if re.fullmatch(r"UCS\d+", source["source_name"]):
            checkpoint = read_json(root / (source["source_name"] + ".query.json"), {})
            if checkpoint.get("query_done"):
                snapshot["sources"][index] = checkpoint
    return snapshot


def ensure_catalogue(catalogue, source_from_row, update=False, retry_failed=False) -> dict:
    """First use queries every row; --update bypasses TAP caches and resumes interruptions."""
    config.ensure_directories()
    root = config.STATE_ROOT
    published = root / "crosscatalogue.json"
    progress = root / "crosscatalogue-update.json"
    with exclusive_lock(root / "crosscatalogue.lock"):
        signature = selection_signature()
        old = read_json(published, {})
        if update and retry_failed:
            raise ValueError("Choose a full update OR failed-query retry, not both")
        if old and old.get("signature") == signature and not update and not retry_failed:
            export_tables(old)
            return old
        previous = {s["source_name"]: s for s in old.get("sources", [])}
        snapshot = read_json(progress, {})
        if retry_failed:
            baseline = snapshot or old
            if not baseline:
                raise ValueError("No crosscatalogue checkpoints to retry; build the index first")
            if baseline.get("signature") != signature:
                raise ValueError("Catalogue/selection changed; use --update-crosscatalogue instead")
            # Resume the SAME targeted pass after interruption. A completed
            # pass requires another explicit invocation to retry any failures.
            if not (snapshot.get("retry_failed_only") and not snapshot.get("finished_at")):
                baseline = _restore_query_checkpoints(baseline)
                snapshot = {"schema": SCHEMA, "signature": signature,
                    "revision": datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ"),
                    "started_at": utc_now(), "retry_failed_only": True,
                    "retry_of_revision": baseline["revision"],
                    "sources": [dict(source) for source in baseline["sources"]]}
                failed = 0
                for source in snapshot["sources"]:
                    if source.get("query_status") == "query_failed":
                        failed += 1
                        source.update(query_done=False, query_status="unknown",
                            query_note="Pending explicit retry of failed query",
                            query_time_utc="", using_previous_result=bool(source.get("observations")))
                logger.info("Targeted crosscatalogue retry: %d failed source(s); successful rows/ECSV retained", failed)
                # Successful ECSV paths still point to the original frozen
                # revision. Only retried/unfinished sources create new files.
                atomic_write_json(progress, snapshot)
        elif not snapshot or snapshot.get("signature") != signature or snapshot.get("finished_at"):
            snapshot = {"schema": SCHEMA, "signature": signature,
                "revision": datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ"),
                "started_at": utc_now(), "sources": []}
            names = set()
            for _, row in catalogue.iterrows():
                # Name/UCS errors are catalogue-wide, not evidence of an empty sky.
                name = text(row["UCS_name"])
                if name in names:
                    raise ValueError(f"Duplicate catalogue source: {name}")
                names.add(name)
                item = {**previous.get(name, {}), "source_name": name, "ucs_number": int(row["UCS"]),
                    "query_status": "unknown", "query_note": "Not queried in this update",
                    "query_time_utc": "", "candidate_count": "", "selected_count": "",
                    "using_previous_result": name in previous, "query_done": False}
                try:
                    item.update(source_from_row(row))
                    if not re.fullmatch(r"UCS\d+", item["source_name"]):
                        raise ValueError("Shared processing requires UCS<number> source names")
                    item["eligible"] = item["sptnum"] >= config.SPTNUM_THRESHOLD
                except Exception as exc:
                    item.update(query_status="query_failed", query_note=f"Invalid catalogue row: {exc}",
                                eligible=False, query_done=True)
                snapshot["sources"].append(item)
            atomic_write_json(progress, snapshot)
        checkpoint_root = root / "crosscatalogue_rows" / snapshot["revision"]
        # Per-source journals make SIGKILL recovery cheap: do not rewrite the
        # whole potentially large catalogue after every single TAP request.
        _restore_query_checkpoints(snapshot)
        export_tables(snapshot)
        try:
            for index, source in enumerate(snapshot["sources"]):
                if source.get("query_done"):
                    continue
                logger.info("Crosscatalogue query %d/%d: %s", index + 1, len(snapshot["sources"]), source["source_name"])
                source["query_time_utc"] = utc_now()
                try:
                    rows = casda_query.query_source_table(source["ra"], source["dec"])
                    from astropy.coordinates import SkyCoord
                    import astropy.units as u
                    target = SkyCoord(source["ra"] * u.deg, source["dec"] * u.deg)
                    observations = []
                    sb_beams = set()
                    for row in rows:
                        obs = observation(row)
                        if obs["sb_name"] in sb_beams:
                            raise ValueError(f"Ambiguous selected products for {obs['sb_name']}")
                        sb_beams.add(obs["sb_name"])
                        obs.update(casda_query.frequency_selection(obs["em_min_m"], obs["em_max_m"]))
                        obs["separation_deg"] = float(target.separation(
                            SkyCoord(float(row["s_ra"])*u.deg, float(row["s_dec"])*u.deg)).deg)
                        observations.append(obs)
                    path = root / "crosscatalogue_rows" / snapshot["revision"] / (source["source_name"] + ".ecsv")
                    path.parent.mkdir(parents=True, exist_ok=True)
                    rows.write(path, format="ascii.ecsv", overwrite=True)
                    old_keys = {o["key"] for o in source.get("observations", [])}
                    new_keys = {o["key"] for o in observations}
                    source.update(observations=observations, query_cache=str(path),
                        added_count=len(new_keys - old_keys), removed_count=len(old_keys - new_keys),
                        query_status="available" if len(rows) else "no_observations",
                        candidate_count=rows.meta.get("askap_candidate_count", ""), selected_count=len(rows),
                        last_success_time_utc=utc_now(), using_previous_result=False,
                        query_note="Selected after frequency/quality/release/nearest-beam filtering" if len(rows)
                            else ("Initial spatial query returned zero rows" if rows.meta.get("askap_candidate_count") == 0
                                  else "All candidates removed by selection filters"))
                except Exception as exc:
                    source.update(query_status="query_failed", query_note=f"{type(exc).__name__}: {exc}")
                    logger.warning("Query failed for %s: %s", source["source_name"], exc)
                source["query_done"] = True
                atomic_write_json(checkpoint_root / (source["source_name"] + ".query.json"), source)
                if config.CROSSCATALOGUE_QUERY_DELAY_SECONDS:
                    time.sleep(config.CROSSCATALOGUE_QUERY_DELAY_SECONDS)
        finally:
            atomic_write_json(progress, snapshot)
            export_tables(snapshot)
        snapshot["finished_at"] = utc_now()
        atomic_write_json(published, snapshot)
        atomic_write_json(progress, snapshot)
        return snapshot


def groups_for_range(snapshot: dict, begin: int, end: int) -> list[dict]:
    """Only observations selected by seeds expand; companions' other SBs do not."""
    available = [s for s in snapshot["sources"] if s["query_status"] == "available" and s.get("eligible")]
    users = defaultdict(list)
    for source in available:
        for obs in source["observations"]:
            users[obs["key"]].append(source)
    groups, seen = [], set()
    for source in sorted(available, key=lambda s: s["ucs_number"]):
        if not begin <= source["ucs_number"] <= end:
            continue
        for obs in source["observations"]:
            if obs["key"] in seen:
                continue
            seen.add(obs["key"])
            groups.append({"observation": obs, "seed_source": source["source_name"],
                "sources": sorted(users[obs["key"]], key=lambda s: (s["source_name"] != source["source_name"], s["ucs_number"])),
                "crosscatalogue_revision": snapshot["revision"]})
    return groups
