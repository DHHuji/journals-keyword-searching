import argparse
import csv
import json
import sys
from pathlib import Path

from llm_output_json import extract_json_objects


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


def _load_metadata_tables(input_dir):
    pdfs_dir = input_dir.parent
    repo_dir = pdfs_dir.parent

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

    return {
        "works_by_id": works_by_id,
        "works_counts": works_counts,
        "per_source": per_source,
    }


def _academic_metadata(file_meta, tables):
    if file_meta["is_works"]:
        work_id = file_meta["work_id"]
        work_details = tables["works_by_id"].get(work_id, {})
        counts = tables["works_counts"].get(work_id, {})
        return {
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

    source_data = tables["per_source"].get(file_meta["source_key"], {})
    items_row = source_data.get("items_by_id", {}).get(file_meta["source_id"], {})
    index_row = source_data.get("index_by_id", {}).get(file_meta["source_id"], {})
    title = items_row.get("title", "") or items_row.get("Article title", "") or index_row.get("title", "") or index_row.get("Article title", "")
    author = items_row.get("author", "") or items_row.get("Authors", "") or index_row.get("author", "") or index_row.get("Authors", "")
    year = items_row.get("year", "") or items_row.get("Volume year", "") or index_row.get("year", "") or index_row.get("Volume year", "")
    if not year:
        year = _year_from_text(items_row.get("publication_date", "") or index_row.get("publication_date", ""))

    return {
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

    sentiment = obj.get("sentiment_toward_israel", obj.get("sentiment", {})) if isinstance(obj, dict) else {}
    confidence = obj.get("confidence_and_ambiguity", {}) if isinstance(obj, dict) else {}
    themes = obj.get("themes", []) if isinstance(obj, dict) else []
    quotes = sentiment.get("evidence_quotes", []) if isinstance(sentiment, dict) else []

    themes_with_details = []
    for theme in themes:
        if not isinstance(theme, dict):
            continue
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

    return {
        "source_file": source_path.name,
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
        "model": model,
        "sentiment_classification": sentiment.get("classification", ""),
        "sentiment_explanation": sentiment.get("explanation", ""),
        "sentiment_notes": sentiment.get("notes", ""),
        "quote_example": quotes[0].get("quote", "") if quotes else "",
        "evidence_quotes_json": json.dumps(quotes, ensure_ascii=True),
        "themes": ",".join(
            [t.get("theme_name", "") for t in themes if isinstance(t, dict) and t.get("theme_name")]
        ),
        "themes_with_details": "\n".join(themes_with_details),
        "themes_json": json.dumps(themes, ensure_ascii=True),
        "confidence_level": confidence.get("confidence_level", ""),
        "uncertainty_explanation": confidence.get("uncertainty_explanation", ""),
    }


def main():
    parser = argparse.ArgumentParser(
        description="Extract JSON from LLM output TXT files and write a unified CSV."
    )
    parser.add_argument(
        "--input-dir",
        default="pdfs/llm_outputs",
        help="Directory containing LLM output .txt files.",
    )
    parser.add_argument(
        "--output-csv",
        default="pdfs/llm_outputs/llm_outputs.csv",
        help="Path for the unified CSV output.",
    )
    parser.add_argument(
        "--output-json",
        default="pdfs/llm_outputs/llm_outputs.json",
        help="Path for the unified JSON output.",
    )
    args = parser.parse_args()

    input_dir = Path(args.input_dir)
    output_csv = Path(args.output_csv)
    output_json = Path(args.output_json)

    if not input_dir.exists():
        print(f"ERROR: Input directory not found: {input_dir}", file=sys.stderr)
        return 2
    tables = _load_metadata_tables(input_dir)

    txt_files = sorted(input_dir.glob("*.txt"))
    if not txt_files:
        print(f"ERROR: No .txt files found in {input_dir}", file=sys.stderr)
        return 2

    rows = []
    json_rows = []
    errors = 0

    for path in txt_files:
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
                enriched[key] = value
            json_rows.append(enriched)

    if not rows:
        print("ERROR: No valid JSON objects were parsed.", file=sys.stderr)
        return 2

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
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
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
