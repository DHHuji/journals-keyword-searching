import argparse
import csv
import os
import time
import traceback
from pathlib import Path
from llm_output_json import has_valid_json_output

MODEL = ""

BATCH_SIZE = 8
DRY_RUN = False

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

WORKS_BASE_DIR = os.path.join(SCRIPT_DIR, "../pdfs")
SEARCH_RESULTS_DIR = os.path.join(SCRIPT_DIR, "../search_results")

PROMPT_FILE = f"{SCRIPT_DIR}/prompt.txt"
THEMES_FILE = f"{SCRIPT_DIR}/themes.txt"
PDFS_OUTPUT_DIR = f"{WORKS_BASE_DIR}/llm_outputs"
SEARCH_RESULTS_OUTPUT_DIR = f"{SEARCH_RESULTS_DIR}/llm_outputs"
SEARCH_RESULTS_CSV_PATTERN = "works_*.csv"

TASK_INPUT_DESCRIPTION_PLACEHOLDER = "{{TASK_INPUT_DESCRIPTION}}"
TASK_TYPE_PDFS = "pdfs"
TASK_TYPE_SEARCH_RESULTS = "search_results"
DEFAULT_TARGET_COUNTRY = "Israel"


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
    raise ValueError(
        f"Unknown model '{model_name}'."
    )


def _build_full_prompt(work_text, prompt, themes, task_input_description):
    prompt_text = prompt.replace(TASK_INPUT_DESCRIPTION_PLACEHOLDER, task_input_description)
    return f"""{prompt_text}

THEMES:
{themes}

WORK TO ANALYZE:
{work_text}"""


def load_work_prompt(work_path, prompt, themes, task_input_description):
    print(f"🔄 Loading {work_path}...")
    try:
        with open(work_path, 'r', encoding='utf-8') as f:
            work = f.read().strip()
    except FileNotFoundError:
        print(f"❌ File not found: {work_path}")
        raise

    return _build_full_prompt(work, prompt, themes, task_input_description)


def build_task_prompt(task, prompt, themes):
    prompt = prompt.replace(DEFAULT_TARGET_COUNTRY, task["target_country"])
    work_path = task.get("work_path")
    if work_path:
        return load_work_prompt(work_path, prompt, themes, task["task_input_description"])
    return _build_full_prompt(task["raw_text"], prompt, themes, task["task_input_description"])


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


def _extract_country_from_search_results_csv(csv_path):
    name = csv_path.stem.removeprefix("works_")
    name = name.split("_or_")[0]
    return name.replace("_", " ").title()


def collect_work_files():
    works_dir = Path(WORKS_BASE_DIR).resolve()
    seen = set()
    pdfs_output_dir = Path(PDFS_OUTPUT_DIR).resolve()
    for txt_path in works_dir.rglob("*.txt"):
        if txt_path.parent == works_dir:
            continue
        txt_path = txt_path.resolve()
        if txt_path.is_relative_to(pdfs_output_dir):
            continue
        if txt_path in seen:
            continue
        seen.add(txt_path)
        relative_path = txt_path.relative_to(works_dir)
        task_key = "_".join(relative_path.with_suffix("").parts)
        txt_path_str = str(txt_path)
        yield {
            "task_key": task_key,
            "task_label": txt_path_str,
            "work_path": txt_path_str,
            "output_dir": PDFS_OUTPUT_DIR,
            "task_input_description": "One academic article in plain text (with page numbers preserved where available).",
            "target_country": DEFAULT_TARGET_COUNTRY,
        }


def _format_search_result_text(row):
    title = (row.get("title") or "").strip()
    abstract = (row.get("abstract") or "").strip() or "N/A"
    keywords = (row.get("keywords") or "").strip()
    authors = (row.get("authors") or "").strip() or "N/A"
    return "\n".join(
        [
            f"Title: {title}",
            f"Abstract: {abstract}",
            f"Keywords: {keywords}",
            f"Authors: {authors}",
        ]
    )


def collect_search_result_works():
    search_dir = Path(SEARCH_RESULTS_DIR).resolve()
    seen = set()
    for csv_path in search_dir.glob(SEARCH_RESULTS_CSV_PATTERN):
        target_country = _extract_country_from_search_results_csv(csv_path)
        with open(csv_path, "r", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                work_id = (row.get("id") or "").strip()
                title = (row.get("title") or "").strip()
                if not work_id or not title:
                    continue
                task_key = f"{csv_path.stem}_{work_id}"
                if task_key in seen:
                    continue
                seen.add(task_key)
                yield {
                    "task_key": task_key,
                    "task_label": f"{csv_path.name}:{work_id}",
                    "raw_text": _format_search_result_text(row),
                    "output_dir": SEARCH_RESULTS_OUTPUT_DIR,
                    "task_input_description": "One academic work summary with Title, Abstract, Keywords, and Authors metadata.",
                    "target_country": target_country,
                }


def collect_tasks(task_type):
    if task_type == TASK_TYPE_PDFS:
        return collect_work_files()
    if task_type == TASK_TYPE_SEARCH_RESULTS:
        return collect_search_result_works()
    raise ValueError(f"Unknown task type '{task_type}'")


def run_tasks(task_type, prompt, themes, llm_gen):
    print("Starting batched processing...\n")

    start_time = time.perf_counter()
    task_count = 0
    successful = 0
    failed = 0
    found_any = False
    failures = []

    def iter_batches(tasks):
        nonlocal task_count, successful, found_any
        batch_tasks = []
        for task in tasks:
            found_any = True
            task_count += 1
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
    for batch_items in iter_batches(collect_tasks(task_type)):
        processed_batch_count += 1
        batch = []
        batch_map = []
        for original_idx, task in batch_items:
            try:
                prompt_text = build_task_prompt(task, prompt, themes)
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
                if not final_text or not final_text.strip() or not has_valid_json_output(final_text):
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
                    if not has_valid_json_output(retry_text):
                        error = ValueError("Invalid JSON model output after retry")
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

    if not found_any:
        if task_type == TASK_TYPE_PDFS:
            print(f"No matching .txt files found in subdirectories of {WORKS_BASE_DIR}")
        else:
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
    global MODEL, llm_gen
    parser = argparse.ArgumentParser(description="Run vLLM processing.")
    parser.add_argument("model", type=str)
    parser.add_argument("--gpu-count", type=int, required=True)
    args = parser.parse_args()

    MODEL = args.model
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
        with open(PROMPT_FILE, 'r', encoding='utf-8') as f:
            prompt = f.read().strip()
        with open(THEMES_FILE, 'r', encoding='utf-8') as f:
            themes = f.read().strip()
    except FileNotFoundError as e:
        print(f"❌ Error: {e}")
        raise

    for task_type in (TASK_TYPE_PDFS, TASK_TYPE_SEARCH_RESULTS):
        run_tasks(task_type, prompt, themes, llm_gen)


if __name__ == "__main__":
    main()
