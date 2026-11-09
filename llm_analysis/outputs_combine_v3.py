"""Combine one independently-run v3 analysis with its original OpenAlex metadata."""

import argparse
import csv
import json
import sys
from pathlib import Path

from llm_output_json import extract_json_objects


SCRIPT_DIR = Path(__file__).resolve().parent
SEARCH_RESULTS_DIR = SCRIPT_DIR.parent / "search_results"
OUTPUT_ROOT = SEARCH_RESULTS_DIR / "llm_outputs_v3"

ATTITUDE_CATEGORIES = [
    "national_movement",
    "state_as_a_whole",
    "specific_state_practices_or_organs",
    "state_affiliated_individuals_or_groups",
    "empathetic",
]


def _as_dict(value):
    return value if isinstance(value, dict) else {}


def _as_list(value):
    return value if isinstance(value, list) else []


def _file_metadata(path):
    """Recover works_<country> and OpenAlex work ID from <source>_<id>_<model>.txt."""
    source_and_id, _, model = path.stem.rpartition("_")
    source_key, _, source_id = source_and_id.rpartition("_")
    return source_key, source_id, model


def _load_metadata():
    metadata = {}
    fieldnames = []
    for csv_path in sorted(SEARCH_RESULTS_DIR.glob("works_*.csv")):
        with csv_path.open(encoding="utf-8", newline="") as f:
            rows = list(csv.DictReader(f))
        if rows and not fieldnames:
            fieldnames = list(rows[0])
        by_id = {}
        for row in rows:
            if row_id := (row.get("id") or "").strip():
                by_id.setdefault(row_id, []).append(row)
        metadata[csv_path.stem] = {
            "focal_country": csv_path.stem.removeprefix("works_"),
            "rows": by_id,
        }
    return metadata, fieldnames


def _common_fields(obj, source_file, model, focal_country):
    return {
        "source_file": source_file,
        "model": model,
        "record_id": obj.get("record_id", ""),
        "focal_country": obj.get("focal_country", focal_country),
    }


def _sentiment_fields(obj):
    relevance = _as_dict(obj.get("relevance"))
    sentiment = _as_dict(obj.get("sentiment"))
    categories = _as_dict(obj.get("attitude_categories"))
    confidence = _as_dict(obj.get("confidence_and_ambiguity"))
    fields = {
        "relevance_is_relevant": relevance.get("is_relevant", ""),
        "relevance_basis": relevance.get("basis", ""),
        "relevance_explanation": relevance.get("explanation", ""),
        "sentiment_code": sentiment.get("code", ""),
        "sentiment_explanation": sentiment.get("explanation", ""),
        "sentiment_evidence_json": json.dumps(_as_list(sentiment.get("evidence")), ensure_ascii=False),
        "verdict_rule_applied": sentiment.get("verdict_rule_applied", ""),
        "no_category_fits": obj.get("no_category_fits", ""),
        "confidence_level": confidence.get("confidence_level", ""),
        "uncertainty_explanation": confidence.get("uncertainty_explanation", ""),
    }
    for category in ATTITUDE_CATEGORIES:
        item = _as_dict(categories.get(category))
        fields[f"{category}_applies"] = item.get("applies", "")
        fields[f"{category}_code"] = item.get("code", "")
        fields[f"{category}_explanation"] = item.get("explanation", "")
    return fields


def _theme_fields(obj):
    themes = [theme for theme in _as_list(obj.get("themes")) if isinstance(theme, dict)]
    confidence = _as_dict(obj.get("confidence_and_ambiguity"))
    return {
        "theme_screening": obj.get("theme_screening", ""),
        "themes": "; ".join(theme.get("theme_name", "") for theme in themes if theme.get("theme_name")),
        "themes_json": json.dumps(themes, ensure_ascii=False),
        "confidence_level": confidence.get("confidence_level", ""),
        "uncertainty_explanation": confidence.get("uncertainty_explanation", ""),
    }


def _analysis_fieldnames(analysis):
    common = ["source_file", "model", "record_id", "focal_country"]
    if analysis == "themes":
        return common + ["theme_screening", "themes", "themes_json", "confidence_level", "uncertainty_explanation"]
    fields = [
        "relevance_is_relevant", "relevance_basis", "relevance_explanation",
        "sentiment_code", "sentiment_explanation", "sentiment_evidence_json",
        "verdict_rule_applied", "no_category_fits",
    ]
    for category in ATTITUDE_CATEGORIES:
        fields.extend([f"{category}_applies", f"{category}_code", f"{category}_explanation"])
    return common + fields + ["confidence_level", "uncertainty_explanation"]


def combine(analysis, input_dir, output_csv, output_json):
    metadata, metadata_fields = _load_metadata()
    output_rows = []
    errors = 0
    for path in sorted(input_dir.glob("*.txt")):
        source_key, source_id, model = _file_metadata(path)
        source = metadata.get(source_key)
        if not source or source_id not in source["rows"]:
            print(f"ERROR: Cannot match {path.name} to a source row", file=sys.stderr)
            errors += 1
            continue
        objects = extract_json_objects(path.read_text(encoding="utf-8", errors="replace"))
        if len(objects) != 1 or not isinstance(objects[0], dict):
            print(f"ERROR: Expected one JSON object in {path.name}", file=sys.stderr)
            errors += 1
            continue
        obj = objects[0]
        llm_fields = _common_fields(obj, path.name, model, source["focal_country"])
        llm_fields.update(_sentiment_fields(obj) if analysis == "sentiment" else _theme_fields(obj))
        for metadata_row in source["rows"][source_id]:
            row = dict(metadata_row)
            # `keyword` was the old country column name.  Metadata keyword tags stay `keywords`.
            row["focal_country"] = source["focal_country"]
            row.update(llm_fields)
            output_rows.append(row)

    if not output_rows:
        print(f"ERROR: No valid v3 {analysis} outputs found in {input_dir}", file=sys.stderr)
        return 2
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = metadata_fields + ["focal_country"] + _analysis_fieldnames(analysis)
    with output_csv.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(output_rows)
    with output_json.open("w", encoding="utf-8") as f:
        json.dump(output_rows, f, ensure_ascii=False, indent=2)
    print(f"Wrote {len(output_rows)} rows to {output_csv}")
    print(f"Wrote {len(output_rows)} JSON entries to {output_json}")
    return 1 if errors else 0


def main():
    parser = argparse.ArgumentParser(description="Combine a v3 sentiment or themes run with OpenAlex metadata.")
    parser.add_argument("analysis", choices=("sentiment", "themes"))
    parser.add_argument("--input-dir", type=Path)
    parser.add_argument("--output-csv", type=Path)
    parser.add_argument("--output-json", type=Path)
    args = parser.parse_args()
    input_dir = args.input_dir or OUTPUT_ROOT / args.analysis
    output_csv = args.output_csv or SEARCH_RESULTS_DIR / f"llm_outputs_v3_{args.analysis}.csv"
    output_json = args.output_json or SEARCH_RESULTS_DIR / f"llm_outputs_v3_{args.analysis}.json"
    return combine(args.analysis, input_dir, output_csv, output_json)


if __name__ == "__main__":
    raise SystemExit(main())
