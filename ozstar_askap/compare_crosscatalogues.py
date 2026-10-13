"""Read-only top/central index comparison; write TOPCAT-readable difference CSVs.

No queries, downloads, jobs, processing-state changes or product deletions.
Uses only Python's standard library. Input paths are ASKAP_WORK directories.
"""
import argparse
import csv
import json
from collections import Counter
from pathlib import Path


def load_index(work, expected_mode):
    path = Path(work) / "state/crosscatalogue.json"
    snapshot = json.loads(path.read_text(encoding="utf-8"))
    if not snapshot.get("finished_at"):
        raise ValueError("Index is not finished: %s" % path)
    mode = snapshot["signature"]["selection"].get("askap_radius_frequency_mode", "top")
    if mode != expected_mode:
        raise ValueError("Expected %s index, found %s: %s" % (expected_mode, mode, path))
    return snapshot


def compare(top, central):
    # Apart from frequency mode, the input catalogue and selection must match.
    signatures = []
    for snapshot in (top, central):
        signature = dict(snapshot["signature"])
        signature["selection"] = dict(signature["selection"])
        signature["selection"].pop("askap_radius_frequency_mode", None)
        signatures.append(signature)
    if signatures[0] != signatures[1]:
        raise ValueError("Catalogue/selection settings differ beyond frequency mode")
    tops = {s["source_name"]: s for s in top["sources"]}
    centrals = {s["source_name"]: s for s in central["sources"]}
    if tops.keys() != centrals.keys():
        raise ValueError("Source lists differ")
    source_rows, differences = [], []
    success = {"available", "no_observations"}
    totals = Counter()
    for name in sorted(tops, key=lambda name: int(name[3:])):
        a, b = tops[name], centrals[name]
        row = dict(source_name=name, eligible=a.get("eligible", False),
            top_query_status=a["query_status"], central_query_status=b["query_status"])
        if any(s["query_status"] not in success or s.get("using_previous_result") for s in (a, b)):
            row["comparison_status"] = "uncomparable_query"
            totals["uncomparable_sources"] += 1
            source_rows.append(row)
            continue
        ao = {o["key"]: o for o in a.get("observations", [])}
        bo = {o["key"]: o for o in b.get("observations", [])}
        removed, added = ao.keys() - bo.keys(), bo.keys() - ao.keys()
        old_by_sb = {o["sb_id"]: o for o in ao.values()}
        new_by_sb = {o["sb_id"]: o for o in bo.values()}
        replaced = {sb for sb in old_by_sb.keys() & new_by_sb.keys()
                    if old_by_sb[sb]["key"] != new_by_sb[sb]["key"]}
        row.update(comparison_status="changed" if removed or added else "unchanged",
            top_count=len(ao), central_count=len(bo), common_count=len(ao.keys() & bo.keys()),
            only_top_count=len(removed), only_central_count=len(added),
            replaced_sb_count=len(replaced))
        source_rows.append(row)
        totals["comparable_sources"] += 1
        totals["changed_sources"] += bool(removed or added)
        for field in ("top_count", "central_count", "common_count", "only_top_count",
                      "only_central_count", "replaced_sb_count"):
            totals[field] += row[field]
        for side, keys, observations, opposite in (
                ("only_top", removed, ao, new_by_sb), ("only_central", added, bo, old_by_sb)):
            for key in sorted(keys):
                obs = observations[key]
                other = opposite.get(obs["sb_id"])
                kind = ("beam_changed" if other and obs["beam"] != other["beam"]
                        else "product_changed" if other else "observation_added_or_removed")
                differences.append(dict(source_name=name, side=side, change_kind=kind,
                    sb_id=obs["sb_id"], beam=obs["beam"], counterpart_beam=other["beam"] if other else "",
                    filename=obs["filename"], key=key, separation_deg=obs.get("separation_deg", ""),
                    half_power_radius_deg=obs.get("half_power_radius_deg", "")))
    summary = {field: totals[field] for field in ("comparable_sources", "uncomparable_sources",
        "changed_sources", "top_count", "central_count", "common_count", "only_top_count",
        "only_central_count", "replaced_sb_count")}
    summary.update(total_sources=len(tops), top_revision=top["revision"],
        central_revision=central["revision"],
        warning="Queries at different times may include CASDA changes; differences are not necessarily caused only by radius.")
    return summary, source_rows, differences


def write_csv(path, columns, rows):
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--top-work", required=True)
    parser.add_argument("--central-work", required=True)
    parser.add_argument("--output-dir", required=True, help="New directory; never overwrites previous reports")
    args = parser.parse_args()
    top = load_index(args.top_work, "top")
    central = load_index(args.central_work, "central")
    summary, sources, differences = compare(top, central)
    output = Path(args.output_dir).resolve()
    output.mkdir(parents=True, exist_ok=False)
    write_csv(output / "source_comparison.csv", ["source_name", "eligible", "top_query_status",
        "central_query_status", "comparison_status", "top_count", "central_count", "common_count",
        "only_top_count", "only_central_count", "replaced_sb_count"], sources)
    write_csv(output / "observation_differences.csv", ["source_name", "side", "change_kind",
        "sb_id", "beam", "counterpart_beam", "filename", "key", "separation_deg",
        "half_power_radius_deg"], differences)
    (output / "comparison-summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    print("Comparison files: %s" % output)


if __name__ == "__main__":
    main()
