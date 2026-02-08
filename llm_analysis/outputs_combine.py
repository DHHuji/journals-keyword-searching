import argparse
import ast
import csv
import json
import re
import sys
from pathlib import Path


CODE_BLOCK_RE = re.compile(r"```(?:json)?\s*(.*?)```", re.DOTALL | re.IGNORECASE)
TRAILING_COMMA_RE = re.compile(r",\s*([}\]])")


def _strip_fences(text):
    blocks = CODE_BLOCK_RE.findall(text)
    if blocks:
        return blocks
    return []


def _balanced_json_candidates(text):
    candidates = []
    start = None
    depth = 0
    for idx, ch in enumerate(text):
        if ch == "{":
            if depth == 0:
                start = idx
            depth += 1
        elif ch == "}":
            if depth > 0:
                depth -= 1
                if depth == 0 and start is not None:
                    candidates.append(text[start:idx + 1])
                    start = None
    return candidates


def _sanitize_json_text(text):
    cleaned = text.strip()
    cleaned = cleaned.replace("\u201c", "\"").replace("\u201d", "\"")
    cleaned = cleaned.replace("\u2018", "'").replace("\u2019", "'")
    cleaned = TRAILING_COMMA_RE.sub(r"\1", cleaned)
    return cleaned


def _try_parse_json(text):
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    sanitized = _sanitize_json_text(text)
    try:
        return json.loads(sanitized)
    except json.JSONDecodeError:
        pass

    try:
        parsed = ast.literal_eval(sanitized)
    except (ValueError, SyntaxError):
        return None

    if isinstance(parsed, (dict, list)):
        return parsed
    return None


def extract_json_objects(text):
    candidates = []
    candidates.extend(_strip_fences(text))
    candidates.extend(_balanced_json_candidates(text))

    seen = set()
    unique_candidates = []
    for cand in candidates:
        key = cand.strip()
        if key and key not in seen:
            seen.add(key)
            unique_candidates.append(cand)

    parsed_objects = []
    for cand in unique_candidates:
        parsed = _try_parse_json(cand)
        if parsed is not None:
            parsed_objects.append(parsed)

    return parsed_objects


def _extract_file_metadata(path):
    stem = path.stem
    if "_" in stem:
        work_id, model = stem.rsplit("_", 1)
    else:
        work_id, model = stem, ""
    return work_id, model


def _row_from_json(obj, source_path):
    work_id, model = _extract_file_metadata(source_path)

    sentiment = obj.get("sentiment_toward_israel", {}) if isinstance(obj, dict) else {}
    confidence = obj.get("confidence_and_ambiguity", {}) if isinstance(obj, dict) else {}
    themes = obj.get("themes", []) if isinstance(obj, dict) else []
    quotes = sentiment.get("evidence_quotes", []) if isinstance(sentiment, dict) else []

    return {
        "source_file": source_path.name,
        "work_id": work_id,
        "model": model,
        "sentiment_classification": sentiment.get("classification", ""),
        "sentiment_explanation": sentiment.get("explanation", ""),
        "sentiment_notes": sentiment.get("notes", ""),
        "evidence_quotes_json": json.dumps(quotes, ensure_ascii=True),
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
            rows.append(_row_from_json(obj, path))
            work_id, model = _extract_file_metadata(path)
            enriched = {
                "source_file": path.name,
                "work_id": work_id,
                "model": model,
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
        "model",
        "sentiment_classification",
        "sentiment_explanation",
        "sentiment_notes",
        "evidence_quotes_json",
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
