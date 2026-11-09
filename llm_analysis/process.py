import argparse
import csv
import os
import time
import traceback
from pathlib import Path
from llm_output_json import extract_json_objects

MODEL = ""

BATCH_SIZE = 32
DRY_RUN = False

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

SEARCH_RESULTS_DIR = os.path.join(SCRIPT_DIR, "../search_results")

SEARCH_RESULTS_CSV_PATTERN = "works_*.csv"

ANALYSES = {
    "sentiment": {
        "prompt_file": f"{SCRIPT_DIR}/prompt-v3/sentiment_prompt_v3.txt",
        "output_dir": f"{SEARCH_RESULTS_DIR}/llm_outputs_v3/sentiment",
    },
    "themes": {
        "prompt_file": f"{SCRIPT_DIR}/prompt-v3/themes_prompt_v3.txt",
        "output_dir": f"{SEARCH_RESULTS_DIR}/llm_outputs_v3/themes",
    },
}


def _print_exception(task_label, exc):
    print(f"❌ Error processing {task_label}: {exc}")
    traceback.print_exception(type(exc), exc, exc.__traceback__)


def _print_invalid_json_output(task_label, output_text):
    print(f"❌ Error processing {task_label}: Invalid JSON model output after retry")
    print("Failed output:")
    print(output_text)


def _load_llm_gen(model_name, gpu_count):
    if model_name == "deepseek-r1":
        from vllm_deepseek_r1 import build_llm_gen as _build_llm_gen
        return _build_llm_gen(gpu_count)
    if model_name == "llama3":
        from vllm_llama3 import build_llm_gen as _build_llm_gen
        return _build_llm_gen(gpu_count)
    if model_name == "llama4":
        from vllm_llama4 import build_llm_gen as _build_llm_gen
        return _build_llm_gen(gpu_count)
    if model_name == "deepseek3":
        from vllm_deepseek_3_2 import build_llm_gen as _build_llm_gen
        return _build_llm_gen(gpu_count)
    if model_name == "gptoss":
        from vllm_gptoss import build_llm_gen as _build_llm_gen
        return _build_llm_gen(gpu_count)
    raise ValueError(
        f"Unknown model '{model_name}'."
    )


def _is_valid_v3_output(output_text, analysis):
    """Reject valid JSON that belongs to the other v3 task or an old prompt."""
    objects = extract_json_objects(output_text)
    if len(objects) != 1 or not isinstance(objects[0], dict):
        return False
    obj = objects[0]
    if analysis == "themes":
        required = {
            "record_id", "focal_country", "theme_screening", "themes",
            "confidence_and_ambiguity",
        }
        return required <= obj.keys() and isinstance(obj["themes"], list)

    required = {
        "record_id", "focal_country", "relevance", "sentiment",
        "attitude_categories", "no_category_fits", "confidence_and_ambiguity",
    }
    if not required <= obj.keys():
        return False
    sentiment = obj["sentiment"]
    categories = obj["attitude_categories"]
    return (
        isinstance(sentiment, dict)
        and sentiment.get("code") in {1, 0, -1, 99}
        and isinstance(categories, dict)
        and all(category in categories for category in (
            "national_movement", "state_as_a_whole",
            "specific_state_practices_or_organs",
            "state_affiliated_individuals_or_groups", "empathetic",
        ))
    )


def build_task_prompt(task, prompt):
    return f"""{prompt}

Input:
{task["record_text"]}"""


def _sanitize_output_name(name):
    return str(name).replace(" ", "_").replace("/", "_")


def _output_path(task):
    output_dir = task["output_dir"]
    task_key = task["task_key"]
    return os.path.join(output_dir, f"{_sanitize_output_name(task_key)}_{MODEL}.txt")


def _write_output(task_label, task, output_text):
    output_path = _output_path(task)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(output_text)

    print(f"✅ Completed {task_label} -> {output_path}")
    return output_path


def _output_exists(task):
    return os.path.exists(_output_path(task))


def _print_dry_run_task(task, prompt_text, batch_index, task_index):
    output_path = _output_path(task)
    print(f"[DRY RUN] Task {task_index + 1} in batch {batch_index + 1}")
    print(f"[DRY RUN] Model: {MODEL}")
    print(f"[DRY RUN] Label: {task['task_label']}")
    print(f"[DRY RUN] Output path: {output_path}")
    print(f"[DRY RUN] Output exists: {_output_exists(task)}")
    print(f"[DRY RUN] Prompt length: {len(prompt_text)} chars")
    print("[DRY RUN] Prompt:")
    print(prompt_text)
    print("[DRY RUN] End prompt\n")


def _keyword_from_search_results_csv(csv_path):
    return csv_path.stem.removeprefix("works_")


def _format_record_text(row, focal_country):
    work_id = (row.get("id") or "").strip()
    title = (row.get("title") or "").strip()
    publication_date = (row.get("publication_date") or "").strip()
    keywords = (row.get("keywords") or "").strip()
    abstract = (row.get("abstract") or "").strip() or "NA"
    return "\n".join(
        [
            f"- id: {work_id}",
            f"- title: {title}",
            f"- publication_date: {publication_date}",
            f"- keywords: {keywords}",
            f"- abstract: {abstract}",
            f"- focal_country: {focal_country}",
        ]
    )


def _load_work_selection(path):
    """Read a CSV allowlist of exact (OpenAlex work ID, focal country) pairs."""
    if path is None:
        return None
    try:
        with path.open(encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames or "id" not in reader.fieldnames:
                raise ValueError("must contain an 'id' column")
            country_column = "focal_country" if "focal_country" in reader.fieldnames else "keyword"
            if country_column not in reader.fieldnames:
                raise ValueError("must contain a 'focal_country' column (or legacy 'keyword' column)")
            selection = {
                ((row.get("id") or "").strip(), (row.get(country_column) or "").strip())
                for row in reader
                if (row.get("id") or "").strip() and (row.get(country_column) or "").strip()
            }
    except OSError as exc:
        raise ValueError(f"Cannot read work selection CSV '{path}': {exc}") from exc
    if not selection:
        raise ValueError(f"Work selection CSV '{path}' contains no ID/country pairs")
    return selection


def collect_search_result_works(output_dir, work_selection=None):
    search_dir = Path(SEARCH_RESULTS_DIR).resolve()
    seen = set()
    for csv_path in search_dir.glob(SEARCH_RESULTS_CSV_PATTERN):
        focal_country = _keyword_from_search_results_csv(csv_path)
        with open(csv_path, "r", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                work_id = (row.get("id") or "").strip()
                title = (row.get("title") or "").strip()
                if not work_id or not title:
                    continue
                if work_selection is not None and (work_id, focal_country) not in work_selection:
                    continue
                task_key = f"{csv_path.stem}_{work_id}"
                if task_key in seen:
                    continue
                seen.add(task_key)
                yield {
                    "task_key": task_key,
                    "task_label": f"{csv_path.name}:{work_id}",
                    "record_text": _format_record_text(row, focal_country),
                    "output_dir": output_dir,
                }


def run_tasks(prompt, llm_gen, output_dir, analysis, work_selection=None):
    print("Starting batched processing...\n")

    start_time = time.perf_counter()
    task_count = 0
    successful = 0
    failed = 0
    found_any = False
    failures = []
    matched_selection = set()

    def iter_batches(tasks):
        nonlocal task_count, successful, found_any
        batch_tasks = []
        for task in tasks:
            found_any = True
            task_count += 1
            if work_selection is not None:
                work_id = task["task_key"].rsplit("_", 1)[-1]
                focal_country = task["record_text"].rsplit(": ", 1)[-1]
                matched_selection.add((work_id, focal_country))
            if _output_exists(task):
                successful += 1
                print(f"⏭️  Skipping {task['task_label']} (output exists)")
                continue
            batch_tasks.append((task_count - 1, task))
            if len(batch_tasks) >= BATCH_SIZE:
                yield batch_tasks
                batch_tasks = []
        if batch_tasks:
            yield batch_tasks

    processed_batch_count = 0
    for batch_items in iter_batches(collect_search_result_works(output_dir, work_selection)):
        processed_batch_count += 1
        batch = []
        batch_map = []
        for original_idx, task in batch_items:
            try:
                prompt_text = build_task_prompt(task, prompt)
                batch.append(prompt_text)
                batch_map.append((original_idx, task))
                if DRY_RUN:
                    _print_dry_run_task(task, prompt_text, processed_batch_count - 1, original_idx)
            except Exception as e:
                failed += 1
                failures.append((task, e))
                _print_exception(task["task_label"], e)

        if not batch:
            continue

        if DRY_RUN:
            successful += len(batch_map)
            continue

        try:
            outputs = llm_gen(batch)
            if len(outputs) != len(batch):
                raise RuntimeError(
                    f"Model returned {len(outputs)} outputs for {len(batch)} prompts"
                )
            retry_batch = []
            retry_map = []
            for batch_idx, output_text in enumerate(outputs):
                original_idx, task = batch_map[batch_idx]
                final_text = output_text
                if not final_text or not final_text.strip() or not _is_valid_v3_output(final_text, analysis):
                    retry_batch.append(batch[batch_idx])
                    retry_map.append((original_idx, task))
                    continue

                _write_output(
                    task["task_label"],
                    task,
                    final_text,
                )
                successful += 1

            if retry_batch:
                retry_outputs = llm_gen(retry_batch)
                if len(retry_outputs) != len(retry_batch):
                    raise RuntimeError(
                        f"Model returned {len(retry_outputs)} outputs for {len(retry_batch)} retry prompts"
                    )
                for retry_idx, retry_text in enumerate(retry_outputs):
                    original_idx, task = retry_map[retry_idx]
                    if not retry_text or not retry_text.strip():
                        error = ValueError("Empty model output after retry")
                        failed += 1
                        failures.append((task, error))
                        print(f"❌ Error processing {task['task_label']}: Empty model output after retry")
                        continue
                    if not _is_valid_v3_output(retry_text, analysis):
                        error = ValueError("Invalid or wrong-schema v3 JSON model output after retry")
                        failed += 1
                        failures.append((task, error))
                        _print_invalid_json_output(task["task_label"], retry_text)
                        continue
                    _write_output(
                        task["task_label"],
                        task,
                        retry_text,
                    )
                    successful += 1
        except Exception as e:
            task_labels = [task["task_label"] for _, task in batch_map]
            joined_task_labels = ", ".join(task_labels)
            _print_exception(joined_task_labels, e)
            failed += len(batch_map)
            for _, task in batch_map:
                failures.append((task, e))

    if work_selection is not None:
        unmatched_selection = work_selection - matched_selection
        if unmatched_selection:
            preview = ", ".join(
                f"{work_id}/{country}"
                for work_id, country in sorted(unmatched_selection)[:10]
            )
            suffix = " ..." if len(unmatched_selection) > 10 else ""
            print(
                f"⚠️  {len(unmatched_selection)} selected work/country pair(s) were not found "
                f"in {SEARCH_RESULTS_DIR}: {preview}{suffix}"
            )

    if not found_any:
        print(f"No matching works found in {SEARCH_RESULTS_DIR} from {SEARCH_RESULTS_CSV_PATTERN}")
        return

    end_time = time.perf_counter()
    total_elapsed = end_time - start_time

    print("\n" + "=" * 60)
    print(f"✨ Processing complete!")
    print(f"   Successful: {successful}/{task_count}")
    print(f"   Failed: {failed}/{task_count}")
    print(f"   Total tasks: {task_count}")
    print(f"   Total time: {total_elapsed:.2f}s")
    print("=" * 60)

    for task, result in failures:
        if result.__traceback__ is None:
            print(f"❌ Error processing {task['task_label']}: {result}")


def main():
    global MODEL, llm_gen, DRY_RUN, BATCH_SIZE
    parser = argparse.ArgumentParser(
        description="Run one independent v3 classification over OpenAlex metadata."
    )
    parser.add_argument("model", type=str)
    parser.add_argument(
        "analysis", choices=ANALYSES,
        help="Independent v3 task to run: sentiment or themes.",
    )
    parser.add_argument("--gpu-count", type=int, required=True)
    parser.add_argument("--dry-run", action="store_true", help="Print prompts without loading a model.")
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE)
    parser.add_argument("--prompt-file", type=Path, help="Override the prompt file for an experiment.")
    parser.add_argument("--output-dir", type=Path, help="Override the analysis output directory.")
    parser.add_argument(
        "--work-selection-csv", type=Path,
        help="Optional CSV with exact 'id' and 'focal_country' pairs (legacy 'keyword' is accepted).",
    )
    args = parser.parse_args()

    MODEL = args.model
    DRY_RUN = args.dry_run
    BATCH_SIZE = args.batch_size
    analysis_config = ANALYSES[args.analysis]
    prompt_file = args.prompt_file or Path(analysis_config["prompt_file"])
    output_dir = str(args.output_dir or Path(analysis_config["output_dir"]))
    if BATCH_SIZE < 1:
        parser.error("--batch-size must be at least 1")
    try:
        work_selection = _load_work_selection(args.work_selection_csv)
    except ValueError as exc:
        parser.error(str(exc))
    if work_selection is not None:
        print(f"Filtering to {len(work_selection)} work/country pair(s) from {args.work_selection_csv}")

    if DRY_RUN:
        print(f"DRY_RUN enabled. Skipping model load for '{MODEL}'.")
        llm_gen = None
    else:
        print(f"Loading model '{MODEL}'...")
        load_start = time.perf_counter()
        llm_gen = _load_llm_gen(MODEL, args.gpu_count)
        load_elapsed = time.perf_counter() - load_start
        print(f"Model '{MODEL}' loaded in {load_elapsed:.2f}s")

    try:
        with open(prompt_file, 'r', encoding='utf-8') as f:
            prompt = f.read().strip()
    except FileNotFoundError as e:
        print(f"❌ Error: {e}")
        raise

    run_tasks(prompt, llm_gen, output_dir, args.analysis, work_selection)


if __name__ == "__main__":
    main()

