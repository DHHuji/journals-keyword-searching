import csv
import json
import sys
from pathlib import Path

from llm_output_json import extract_json_objects


PDFS_INPUT_DIR = Path("pdfs/llm_outputs")
PDFS_OUTPUT_CSV = Path("pdfs/llm_outputs.csv")
PDFS_OUTPUT_JSON = Path("pdfs/llm_outputs.json")

SEARCH_RESULTS_INPUT_DIR = Path("search_results/llm_outputs")
SEARCH_RESULTS_OUTPUT_CSV = Path("search_results/llm_outputs.csv")
SEARCH_RESULTS_OUTPUT_JSON = Path("search_results/llm_outputs.json")

PROGRESS_EVERY = 250


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
    is_works = source_key == "works"
    return {
        "source_with_id": source_with_id,
        "source_key": source_key,
        "source_id": source_id,
        "work_id": source_id if is_works else "",
        "model": model,
        "is_works": is_works,
    }


def _search_keyword_from_source_key(source_key):
    return source_key.removeprefix("works_")


def _extract_file_id(row):
    if "work_id" in row and row["work_id"]:
        return row["work_id"]
    if "ID" in row and row["ID"]:
        return row["ID"]
    url = row.get("url", "")
    if url and "jstor.org/stable/" in url:
        file_id = url.split("jstor.org/stable/")[-1]
        if "/" in file_id:
            file_id = file_id.split("/")[-1]
        if file_id:
            return file_id
    return row.get("citation_key", "")


def _csv_rows_by_id(path):
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))
    out = {}
    for row in rows:
        file_id = _extract_file_id(row)
        if file_id:
            out[file_id] = row
    return out


def _csv_rows(path):
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def _year_from_text(text):
    if not text:
        return ""
    text = str(text)
    for i in range(len(text) - 3):
        chunk = text[i:i + 4]
        if chunk.isdigit():
            return chunk
    return ""


def _canonical_source_key(name):
    return name.replace(" ", "_")


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


def _load_metadata_tables(input_dir):
    repo_dir = input_dir.parent.parent
    pdfs_dir = repo_dir / "pdfs"
    search_results_dir = repo_dir / "search_results"
    print(f"Loading metadata tables for {input_dir}...")

    works_csv_path = repo_dir / "works.csv"
    works_index_path = pdfs_dir / "works" / "index.csv"

    works_rows = _csv_rows(works_csv_path)
    works_by_id = {}
    for row in works_rows:
        work_id = row.get("id", "")
        if not work_id:
            continue
        works_by_id[work_id] = {
            "title": row.get("title", ""),
            "author": row.get("authors", ""),
            "year": _year_from_text(row.get("publication_date", "")),
            "journal": row.get("journal_name", ""),
            "url": row.get("doi_follow", "") or row.get("oa_url", "") or row.get("doi", ""),
        }

    works_counts = _csv_rows_by_id(works_index_path)

    per_source = {}
    if pdfs_dir.exists():
        for child in pdfs_dir.iterdir():
            if not child.is_dir():
                continue
            if child.name in {"llm_outputs", "misc"}:
                continue
            source_key = _canonical_source_key(child.name)
            items_by_id = _csv_rows_by_id(child / "items.csv")
            index_by_id = _csv_rows_by_id(child / "index.csv")
            per_source[source_key] = {
                "dir_name": child.name,
                "items_by_id": items_by_id,
                "index_by_id": index_by_id,
            }

    search_results_by_source = {}
    search_results_fieldnames = []
    if search_results_dir.exists():
        for csv_path in sorted(search_results_dir.glob("works_*.csv")):
            rows = _csv_rows(csv_path)
            if rows and not search_results_fieldnames:
                search_results_fieldnames = list(rows[0].keys())
            search_results_by_source[csv_path.stem] = {
                "keyword": _search_keyword_from_source_key(csv_path.stem),
                "rows_by_id": {
                    (row.get("id") or "").strip(): row
                    for row in rows
                    if (row.get("id") or "").strip()
                },
            }

    return {
        "works_by_id": works_by_id,
        "works_counts": works_counts,
        "per_source": per_source,
        "search_results_by_source": search_results_by_source,
        "search_results_fieldnames": search_results_fieldnames,
    }


def _academic_metadata(file_meta, tables):
    if file_meta["is_works"]:
        work_id = file_meta["work_id"]
        work_details = tables["works_by_id"].get(work_id, {})
        counts = tables["works_counts"].get(work_id, {})
        return {
            "record_type": "pdf_work",
            "work_id": work_id,
            "title": work_details.get("title", ""),
            "author": work_details.get("author", ""),
            "year": work_details.get("year", ""),
            "journal": work_details.get("journal", ""),
            "url": work_details.get("url", ""),
            "israel_count": counts.get("israel_count", ""),
            "lines_count": counts.get("lines_count", ""),
            "word_count": counts.get("word_count", ""),
            "israel_count_center": counts.get("israel_count_center", ""),
        }

    search_data = tables["search_results_by_source"].get(file_meta["source_key"])
    if search_data:
        search_row = dict(search_data["rows_by_id"].get(file_meta["source_id"], {}))
        return {
            "record_type": "search_result",
            "keyword": search_data["keyword"],
            "search_row": search_row,
        }

    source_data = tables["per_source"].get(file_meta["source_key"], {})
    items_row = source_data.get("items_by_id", {}).get(file_meta["source_id"], {})
    index_row = source_data.get("index_by_id", {}).get(file_meta["source_id"], {})
    title = items_row.get("title", "") or items_row.get("Article title", "") or index_row.get("title", "") or index_row.get("Article title", "")
    author = items_row.get("author", "") or items_row.get("Authors", "") or index_row.get("author", "") or index_row.get("Authors", "")
    year = items_row.get("year", "") or items_row.get("Volume year", "") or index_row.get("year", "") or index_row.get("Volume year", "")
    if not year:
        year = _year_from_text(items_row.get("publication_date", "") or index_row.get("publication_date", ""))

    return {
        "record_type": "journal_pdf",
        "work_id": "",
        "title": title,
        "author": author,
        "year": year,
        "journal": source_data.get("dir_name", file_meta["source_key"].replace("_", " ")),
        "url": items_row.get("url", "") or items_row.get("URL", "") or index_row.get("url", "") or index_row.get("URL", ""),
        "israel_count": index_row.get("israel_count", ""),
        "lines_count": index_row.get("lines_count", ""),
        "word_count": index_row.get("word_count", ""),
        "israel_count_center": index_row.get("israel_count_center", ""),
    }


def _row_from_json(obj, source_path, tables):
    file_meta = _extract_file_metadata(source_path)
    model = file_meta["model"]
    academic = _academic_metadata(file_meta, tables)

    raw_sentiment = {}
    raw_confidence = {}
    raw_themes = []
    if isinstance(obj, dict):
        raw_sentiment = obj.get("sentiment_toward_israel", obj.get("sentiment", {}))
        raw_confidence = obj.get("confidence_and_ambiguity", {})
        raw_themes = obj.get("themes", [])

    sentiment = _as_dict(raw_sentiment)
    confidence = _as_dict(raw_confidence)
    themes = _dict_items(_as_list(raw_themes))
    quotes = _dict_items(_as_list(sentiment.get("evidence_quotes", [])))

    themes_with_details = []
    for theme in themes:
        theme_name = theme.get("theme_name", "")
        explanation = theme.get("explanation", "")
        page_ref = (
            theme.get("page_ref")
            or theme.get("page_reference")
            or theme.get("page")
            or theme.get("page_number")
            or ""
        )
        if not theme_name:
            continue
        themes_with_details.append(f"{theme_name}: {explanation} ({page_ref})")

    llm_fields = {
        "source_file": source_path.name,
        "model": model,
        "sentiment_classification": sentiment.get("classification", ""),
        "sentiment_explanation": sentiment.get("explanation", ""),
        "sentiment_notes": sentiment.get("notes", ""),
        "quote_example": quotes[0].get("quote", "") if quotes else "",
        "evidence_quotes_json": json.dumps(quotes, ensure_ascii=True),
        "themes": ",".join(
            [t.get("theme_name", "") for t in themes if t.get("theme_name")]
        ),
        "themes_with_details": "\n".join(themes_with_details),
        "themes_json": json.dumps(themes, ensure_ascii=True),
        "confidence_level": confidence.get("confidence_level", ""),
        "uncertainty_explanation": confidence.get("uncertainty_explanation", ""),
    }

    if academic["record_type"] == "search_result":
        row = dict(academic["search_row"])
        row["keyword"] = academic["keyword"]
        row.update(llm_fields)
        return row

    return {
        **llm_fields,
        "work_id": academic["work_id"],
        "title": academic["title"],
        "author": academic["author"],
        "year": academic["year"],
        "journal": academic["journal"],
        "url": academic["url"],
        "israel_count": academic["israel_count"],
        "lines_count": academic["lines_count"],
        "word_count": academic["word_count"],
        "israel_count_center": academic["israel_count_center"],
    }


def _combine_outputs(input_dir, output_csv, output_json):
    print(f"Starting combine for {input_dir}...")
    if not input_dir.exists():
        print(f"ERROR: Input directory not found: {input_dir}", file=sys.stderr)
        return 2
    tables = _load_metadata_tables(input_dir)
    print(
        "Loaded metadata: "
        f"{len(tables['works_by_id'])} works, "
        f"{len(tables['per_source'])} pdf sources, "
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
            row = _row_from_json(obj, path, tables)
            rows.append(row)
            file_meta = _extract_file_metadata(path)
            if "keyword" in row:
                enriched = dict(row)
            else:
                enriched = {
                    "source_file": path.name,
                    "work_id": row["work_id"],
                    "title": row["title"],
                    "author": row["author"],
                    "year": row["year"],
                    "journal": row["journal"],
                    "url": row["url"],
                    "israel_count": row["israel_count"],
                    "lines_count": row["lines_count"],
                    "word_count": row["word_count"],
                    "israel_count_center": row["israel_count_center"],
                    "model": file_meta["model"],
                }
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
    llm_fieldnames = [
        "source_file",
        "model",
        "sentiment_classification",
        "sentiment_explanation",
        "sentiment_notes",
        "quote_example",
        "evidence_quotes_json",
        "themes",
        "themes_with_details",
        "themes_json",
        "confidence_level",
        "uncertainty_explanation",
    ]
    search_results_fieldnames = [
        *tables["search_results_fieldnames"],
        "keyword",
    ]
    default_fieldnames = [
        "source_file",
        "work_id",
        "title",
        "author",
        "year",
        "journal",
        "url",
        "israel_count",
        "lines_count",
        "word_count",
        "israel_count_center",
        "model",
        "sentiment_classification",
        "sentiment_explanation",
        "sentiment_notes",
        "quote_example",
        "evidence_quotes_json",
        "themes",
        "themes_with_details",
        "themes_json",
        "confidence_level",
        "uncertainty_explanation",
    ]
    if all("keyword" in row for row in rows):
        fieldnames = search_results_fieldnames + llm_fieldnames
    else:
        fieldnames = default_fieldnames
    with output_csv.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Wrote {len(rows)} rows to {output_csv}")
    output_json.parent.mkdir(parents=True, exist_ok=True)
    with output_json.open("w", encoding="utf-8") as f:
        json.dump(json_rows, f, ensure_ascii=True, indent=2)
    print(f"Wrote {len(json_rows)} JSON entries to {output_json}")
    if errors:
        print(f"Completed with {errors} error(s).", file=sys.stderr)
    print(f"Finished combine for {input_dir}")
    return 0


def main():
    exit_code = 0
    print("Starting outputs combine...")
    for input_dir, output_csv, output_json in (
        (PDFS_INPUT_DIR, PDFS_OUTPUT_CSV, PDFS_OUTPUT_JSON),
        (SEARCH_RESULTS_INPUT_DIR, SEARCH_RESULTS_OUTPUT_CSV, SEARCH_RESULTS_OUTPUT_JSON),
    ):
        result = _combine_outputs(input_dir, output_csv, output_json)
        if result != 0:
            exit_code = result
    print(f"Outputs combine finished with exit code {exit_code}")
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
