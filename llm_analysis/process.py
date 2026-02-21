import argparse
import csv
import os
import time
from pathlib import Path

MODEL = ""

BATCH_SIZE = 8

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
WORKS_BASE_DIR = os.path.join(SCRIPT_DIR, "../pdfs")

PROMPT_FILE = f"{SCRIPT_DIR}/prompt.txt"
THEMES_FILE = f"{SCRIPT_DIR}/themes.txt"
OUTPUT_DIR = f"{WORKS_BASE_DIR}/llm_outputs"
CSV_FILENAME = "index.csv"
LINES_COUNT_COLUMN = "lines_count"
ISRAEL_COUNT_CENTER_COLUMN = "israel_count_center"
LINES_COUNT_MIN = 300
ISRAEL_COUNT_CENTER_MIN = 5


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


def load_work_prompt(work_path, prompt, themes):
    print(f"🔄 Loading {work_path}...")
    try:
        with open(work_path, 'r', encoding='utf-8') as f:
            work = f.read().strip()
    except FileNotFoundError:
        print(f"❌ File not found: {work_path}")
        raise

    full_prompt = f"""{prompt}

THEMES:
{themes}

WORK TO ANALYZE:
{work}"""
    return full_prompt


def _output_path(work_path):
    source_dir = Path(work_path).parent.name
    work_name = Path(work_path).stem
    return os.path.join(OUTPUT_DIR, f"{source_dir}_{work_name}_{MODEL}.txt".replace(" ", "_"))


def _write_output(work_path, output_text):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_path = _output_path(work_path)

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(output_text)

    print(f"✅ Completed {work_path} -> {output_path}")
    return output_path


def _output_exists(work_path):
    return os.path.exists(_output_path(work_path))


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


def _to_int(value):
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(float(text))
    except ValueError:
        return None


def collect_work_files():
    works_dir = Path(WORKS_BASE_DIR).resolve()
    work_files = []
    seen = set()
    for csv_path in works_dir.rglob(CSV_FILENAME):
        base_dir = csv_path.parent
        with open(csv_path, "r", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                lines_count = _to_int(row.get(LINES_COUNT_COLUMN))
                israel_count_center = _to_int(row.get(ISRAEL_COUNT_CENTER_COLUMN))
                if lines_count is None or israel_count_center is None:
                    continue
                if lines_count < LINES_COUNT_MIN or israel_count_center < ISRAEL_COUNT_CENTER_MIN:
                    continue

                file_id = _extract_file_id(row)
                if not file_id:
                    continue
                txt_path = (base_dir / f"{file_id}.txt").resolve()
                if not txt_path.exists():
                    continue
                if txt_path not in seen:
                    seen.add(txt_path)
                    work_files.append(str(txt_path))
    return work_files


def main():
    global MODEL, llm_gen
    parser = argparse.ArgumentParser(description="Run vLLM processing.")
    parser.add_argument("model", type=str)
    parser.add_argument("--gpu-count", type=int, required=True)
    args = parser.parse_args()

    work_files = collect_work_files()
    if not work_files:
        print(
            f"No matching .txt files found in {WORKS_BASE_DIR} "
            f"from {CSV_FILENAME} where {LINES_COUNT_COLUMN}>={LINES_COUNT_MIN} "
            f"and {ISRAEL_COUNT_CENTER_COLUMN}>={ISRAEL_COUNT_CENTER_MIN}"
        )
        return
    print(f"Discovered {len(work_files)} work files from CSV filters.")

    MODEL = args.model
    print(f"Loading model '{MODEL}'...")
    load_start = time.perf_counter()
    llm_gen = _load_llm_gen(MODEL, args.gpu_count)
    load_elapsed = time.perf_counter() - load_start
    print(f"Model '{MODEL}' loaded in {load_elapsed:.2f}s")

    print("Starting batched processing...\n")

    try:
        with open(PROMPT_FILE, 'r', encoding='utf-8') as f:
            prompt = f.read().strip()
        with open(THEMES_FILE, 'r', encoding='utf-8') as f:
            themes = f.read().strip()
    except FileNotFoundError as e:
        print(f"❌ Error: {e}")
        raise

    task_count = len(work_files)
    start_time = time.perf_counter()
    results = [None] * len(work_files)

    def chunks(items, size):
        for i in range(0, len(items), size):
            yield i, items[i:i + size]

    def iter_batches():
        batch_paths = []
        batch_indices = []
        for idx, work_path in enumerate(work_files):
            if _output_exists(work_path):
                results[idx] = "skipped"
                print(f"⏭️  Skipping {work_path} (output exists)")
                continue
            batch_paths.append(work_path)
            batch_indices.append(idx)
            if len(batch_paths) >= BATCH_SIZE:
                yield batch_paths, batch_indices
                batch_paths = []
                batch_indices = []
        if batch_paths:
            yield batch_paths, batch_indices

    for batch_paths, batch_indices in iter_batches():
        batch = []
        batch_map = []
        for work_path, original_idx in zip(batch_paths, batch_indices):
            try:
                batch.append(load_work_prompt(work_path, prompt, themes))
                batch_map.append((original_idx, work_path))
            except Exception as e:
                results[original_idx] = e

        if not batch:
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
                original_idx, work_path = batch_map[batch_idx]
                final_text = output_text
                if not final_text or not final_text.strip():
                    retry_batch.append(batch[batch_idx])
                    retry_map.append((original_idx, work_path))
                    continue

                results[original_idx] = _write_output(
                    work_path,
                    final_text,
                )

            if retry_batch:
                retry_outputs = llm_gen(retry_batch)
                if len(retry_outputs) != len(retry_batch):
                    raise RuntimeError(
                        f"Model returned {len(retry_outputs)} outputs for {len(retry_batch)} retry prompts"
                    )
                for retry_idx, retry_text in enumerate(retry_outputs):
                    original_idx, work_path = retry_map[retry_idx]
                    if not retry_text or not retry_text.strip():
                        results[original_idx] = ValueError("Empty model output after retry")
                        print(f"❌ Error processing {work_path}: Empty model output after retry")
                        continue
                    results[original_idx] = _write_output(
                        work_path,
                        retry_text,
                    )
        except Exception as e:
            for original_idx, _ in batch_map:
                results[original_idx] = e

    end_time = time.perf_counter()
    total_elapsed = end_time - start_time

    print("\n" + "=" * 60)
    successful = sum(1 for r in results if r and not isinstance(r, Exception))
    failed = len(results) - successful
    print(f"✨ Processing complete!")
    print(f"   Successful: {successful}/{len(results)}")
    print(f"   Failed: {failed}/{len(results)}")
    print(f"   Total tasks: {task_count}")
    print(f"   Total time: {total_elapsed:.2f}s")
    print("=" * 60)

    for work_path, result in zip(work_files, results):
        if isinstance(result, Exception):
            print(f"❌ Error processing {work_path}: {result}")


if __name__ == "__main__":
    main()
