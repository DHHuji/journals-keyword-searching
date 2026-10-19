import csv
import json
import sys
from pathlib import Path

from llm_output_json import extract_json_objects


SEARCH_RESULTS_INPUT_DIR = Path("search_results/llm_outputs")
SEARCH_RESULTS_OUTPUT_CSV = Path("search_results/llm_outputs.csv")
SEARCH_RESULTS_OUTPUT_JSON = Path("search_results/llm_outputs.json")

ISR_STUDIES_SOURCE_ID = "S76733833"
ISR_STUDIES_SUFFIX = "_isr_studies"

PROGRESS_EVERY = 250

ATTITUDE_CATEGORIES = [
    "national_movement",
    "state_as_a_whole",
    "specific_state_practices_or_organs",
    "empathetic",
    "state_affiliated_individuals_or_groups",
]


def _extract_file_metadata(path):
    stem = path.stem
    if "_" in stem:
        source_with_id, model = stem.rsplit("_", 1)
    else:
        source_with_id, model = stem, ""
    if "_" in source_with_id:
        source_key, source_id = source_with_id.rsplit("_", 1)
    else:
        source_key, source_id = source_with_id, ""
    return {
        "source_key": source_key,
        "source_id": source_id,
        "model": model,
    }


def _search_keyword_from_source_key(source_key):
    return source_key.removeprefix("works_")


def _csv_rows(path):
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def _as_dict(value):
    return value if isinstance(value, dict) else {}


def _as_list(value):
    return value if isinstance(value, list) else []


def _dict_items(values):
    return [value for value in values if isinstance(value, dict)]


def _json_safe(value):
    if value is Ellipsis:
        return None
    if isinstance(value, dict):
        return {key: _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [_json_safe(item) for item in value]
    if isinstance(value, set):
        return [_json_safe(item) for item in sorted(value, key=repr)]
    return value


def _load_metadata_tables(search_results_dir):
    print(f"Loading metadata tables for {search_results_dir}...")
    search_results_by_source = {}
    search_results_fieldnames = []
    if search_results_dir.exists():
        for csv_path in sorted(search_results_dir.glob("works_*.csv")):
            rows = _csv_rows(csv_path)
            if rows and not search_results_fieldnames:
                search_results_fieldnames = list(rows[0].keys())
            rows_by_id = {}
            for row in rows:
                row_id = (row.get("id") or "").strip()
                if row_id:
                    rows_by_id.setdefault(row_id, []).append(row)
            search_results_by_source[csv_path.stem] = {
                "keyword": _search_keyword_from_source_key(csv_path.stem),
                "rows_by_id": rows_by_id,
            }

    return {
        "search_results_by_source": search_results_by_source,
        "search_results_fieldnames": search_results_fieldnames,
    }


def _llm_fieldnames():
    fieldnames = [
        "source_file",
        "model",
        "record_id",
        "target_country",
        "country_salience",
    ]
    for category in ATTITUDE_CATEGORIES:
        fieldnames.extend([
            f"{category}_code",
            f"{category}_explanation",
            f"{category}_evidence_json",
        ])
    fieldnames.extend([
        "themes",
        "themes_with_details",
        "themes_json",
        "confidence_level",
        "uncertainty_explanation",
    ])
    return fieldnames


def _rows_from_json(obj, source_path, tables):
    file_meta = _extract_file_metadata(source_path)
    search_data = tables["search_results_by_source"].get(file_meta["source_key"])
    if not search_data:
        return []
    search_rows = search_data["rows_by_id"].get(file_meta["source_id"], [])
    if not search_rows:
        return []

    attitudes = _as_dict(obj.get("attitudes", {}))
    confidence = _as_dict(obj.get("confidence_and_ambiguity", {}))
    themes = _dict_items(_as_list(obj.get("themes", [])))

    themes_with_details = []
    for theme in themes:
        theme_name = theme.get("theme_name", "")
        if not theme_name:
            continue
        themes_with_details.append(f"{theme.get('rank', '')}. {theme_name}: {theme.get('explanation', '')}")

    llm_fields = {
        "source_file": source_path.name,
        "model": file_meta["model"],
        "record_id": obj.get("record_id", ""),
        "target_country": obj.get("target_country", ""),
        "country_salience": obj.get("country_salience", ""),
    }
    for category in ATTITUDE_CATEGORIES:
        attitude = _as_dict(attitudes.get(category, {}))
        evidence = _dict_items(_as_list(attitude.get("evidence", [])))
        llm_fields[f"{category}_code"] = attitude.get("code", "")
        llm_fields[f"{category}_explanation"] = attitude.get("explanation", "")
        llm_fields[f"{category}_evidence_json"] = json.dumps(evidence, ensure_ascii=True)
    llm_fields.update({
        "themes": ",".join(
            [t.get("theme_name", "") for t in themes if t.get("theme_name")]
        ),
        "themes_with_details": "\n".join(themes_with_details),
        "themes_json": json.dumps(themes, ensure_ascii=True),
        "confidence_level": confidence.get("confidence_level", ""),
        "uncertainty_explanation": confidence.get("uncertainty_explanation", ""),
    })

    rows = []
    for search_row in search_rows:
        row = dict(search_row)
        row["keyword"] = search_data["keyword"]
        row.update(llm_fields)
        rows.append(row)
    return rows


def _combine_outputs(input_dir, output_csv, output_json):
    print(f"Starting combine for {input_dir}...")
    if not input_dir.exists():
        print(f"ERROR: Input directory not found: {input_dir}", file=sys.stderr)
        return 2
    tables = _load_metadata_tables(input_dir.parent)
    print(
        "Loaded metadata: "
        f"{len(tables['search_results_by_source'])} search_results variants"
    )

    txt_files = sorted(input_dir.glob("*.txt"))
    if not txt_files:
        print(f"ERROR: No .txt files found in {input_dir}", file=sys.stderr)
        return 2
    print(f"Found {len(txt_files)} output text files in {input_dir}")

    rows = []
    json_rows = []
    errors = 0

    for index, path in enumerate(txt_files, start=1):
        if index == 1 or index % PROGRESS_EVERY == 0 or index == len(txt_files):
            print(f"Processed {index}/{len(txt_files)} files...")
        text = path.read_text(encoding="utf-8", errors="replace")
        json_objects = extract_json_objects(text)
        if not json_objects:
            print(f"ERROR: No JSON found in {path}", file=sys.stderr)
            errors += 1
            continue

        for obj in json_objects:
            if not isinstance(obj, dict):
                print(f"ERROR: JSON root is not an object in {path}", file=sys.stderr)
                errors += 1
                continue
            for row in _rows_from_json(obj, path, tables):
                rows.append(row)
                enriched = dict(row)
                for key, value in obj.items():
                    if key in enriched:
                        continue
                    enriched[key] = _json_safe(value)
                json_rows.append(enriched)

    if not rows:
        print("ERROR: No valid JSON objects were parsed.", file=sys.stderr)
        return 2
    print(f"Parsed {len(rows)} total row(s) from {len(txt_files)} file(s)")

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        *tables["search_results_fieldnames"],
        "keyword",
        *_llm_fieldnames(),
    ]
    isr_studies_pairs = [(row, json_row) for row, json_row in zip(rows, json_rows) if row.get("source_id") == ISR_STUDIES_SOURCE_ID]
    other_pairs = [(row, json_row) for row, json_row in zip(rows, json_rows) if row.get("source_id") != ISR_STUDIES_SOURCE_ID]
    targets = [(output_csv, output_json, other_pairs)]
    if isr_studies_pairs:
        targets.append((
            output_csv.with_name(f"{output_csv.stem}{ISR_STUDIES_SUFFIX}{output_csv.suffix}"),
            output_json.with_name(f"{output_json.stem}{ISR_STUDIES_SUFFIX}{output_json.suffix}"),
            isr_studies_pairs,
        ))
    for target_csv, target_json, pairs in targets:
        with target_csv.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(row for row, _ in pairs)
        print(f"Wrote {len(pairs)} rows to {target_csv}")
        target_json.parent.mkdir(parents=True, exist_ok=True)
        with target_json.open("w", encoding="utf-8") as f:
            json.dump([json_row for _, json_row in pairs], f, ensure_ascii=True, indent=2)
        print(f"Wrote {len(pairs)} JSON entries to {target_json}")
    if errors:
        print(f"Completed with {errors} error(s).", file=sys.stderr)
    print(f"Finished combine for {input_dir}")
    return 0


def main():
    print("Starting outputs combine...")
    exit_code = _combine_outputs(SEARCH_RESULTS_INPUT_DIR, SEARCH_RESULTS_OUTPUT_CSV, SEARCH_RESULTS_OUTPUT_JSON)
    print(f"Outputs combine finished with exit code {exit_code}")
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
