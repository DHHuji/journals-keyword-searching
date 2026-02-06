import argparse
import os
import time
from pathlib import Path

MODEL = ""

BATCH_SIZE = 8

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
WORKS_BASE_DIR = os.path.join(SCRIPT_DIR, "../pdfs")
WORK_FILES = [
    f"{WORKS_BASE_DIR}/Critical Inquiry/26547794.txt",
    f"{WORKS_BASE_DIR}/Critical Inquiry/653411.txt",
    f"{WORKS_BASE_DIR}/International Journal of Middle East Studies/30069608.txt",
    f"{WORKS_BASE_DIR}/Journal of Genocide Research/52.txt",
    f"{WORKS_BASE_DIR}/International Journal of Middle East Studies/26852709.txt",
    f"{WORKS_BASE_DIR}/works/W2592572157.txt",
    f"{WORKS_BASE_DIR}/International Journal of Middle East Studies/30069652.txt",
    f"{WORKS_BASE_DIR}/American Sociological Review/2095571.txt",
    f"{WORKS_BASE_DIR}/American Journal of International Law/2200012.txt",
    f"{WORKS_BASE_DIR}/Journal of Genocide Research/31.txt",
    f"{WORKS_BASE_DIR}/works/W2124598117.txt",
    f"{WORKS_BASE_DIR}/International Journal of Middle East Studies/164535.txt",
    f"{WORKS_BASE_DIR}/Critical Inquiry/662741.txt",
    f"{WORKS_BASE_DIR}/Journal of Genocide Research/45.txt",
    f"{WORKS_BASE_DIR}/American Journal of International Law/2195023.txt",
    f"{WORKS_BASE_DIR}/Critical Inquiry/664554.txt",
    f"{WORKS_BASE_DIR}/misc/not-all-who-ascend-remain-afro-asian-jewish-returnees-from-israel.txt",
    f"{WORKS_BASE_DIR}/works/W4406178509.txt",
    f"{WORKS_BASE_DIR}/Journal of Genocide Research/89.txt",
]

PROMPT_FILE = f"{WORKS_BASE_DIR}/prompt.txt"
THEMES_FILE = f"{WORKS_BASE_DIR}/themes.txt"
OUTPUT_DIR = f"{WORKS_BASE_DIR}/llm_outputs"


def _load_llm_gen(model_name, gpu_count):
    if model_name == "deepseek-r1":
        from vllm_deepseek import build_llm_gen as _build_llm_gen
        return _build_llm_gen(gpu_count)
    if model_name == "llama3":
        from vllm_llama3 import build_llm_gen as _build_llm_gen
        return _build_llm_gen(gpu_count)
    raise ValueError(
        f"Unknown model '{model_name}'. Expected 'deepseek-r1' or 'llama3'."
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


def _write_output(work_path, output_text):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    work_name = Path(work_path).stem
    output_path = os.path.join(OUTPUT_DIR, f"{work_name}_{MODEL}.txt")

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(output_text)

    print(f"✅ Completed {work_path} -> {output_path}")
    return output_path


def _output_exists(work_path):
    work_name = Path(work_path).stem
    output_path = os.path.join(OUTPUT_DIR, f"{work_name}_{MODEL}.txt")
    return os.path.exists(output_path)


def main():
    global MODEL, llm_gen
    parser = argparse.ArgumentParser(description="Run vLLM processing.")
    parser.add_argument("model", choices=["deepseek-r1", "llama3"])
    parser.add_argument("--gpu-count", type=int, required=True)
    args = parser.parse_args()

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

    task_count = len(WORK_FILES)
    start_time = time.perf_counter()
    results = [None] * len(WORK_FILES)

    def chunks(items, size):
        for i in range(0, len(items), size):
            yield i, items[i:i + size]

    for start_idx, batch_paths in chunks(WORK_FILES, BATCH_SIZE):
        batch = []
        batch_map = []
        for offset, work_path in enumerate(batch_paths):
            if _output_exists(work_path):
                results[start_idx + offset] = "skipped"
                print(f"⏭️  Skipping {work_path} (output exists)")
                continue
            try:
                batch.append(load_work_prompt(work_path, prompt, themes))
                batch_map.append((offset, work_path))
            except Exception as e:
                results[start_idx + offset] = e

        if not batch:
            continue

        if len(batch) != len(batch_paths):
            # Some prompts failed to load or were skipped; still process what we have.
            pass

        try:
            outputs = llm_gen(batch)
            if len(outputs) != len(batch):
                raise RuntimeError(
                    f"Model returned {len(outputs)} outputs for {len(batch)} prompts"
                )
            retry_batch = []
            retry_map = []
            for batch_idx, output_text in enumerate(outputs):
                original_offset, work_path = batch_map[batch_idx]
                final_text = output_text
                if not final_text or not final_text.strip():
                    retry_batch.append(batch[batch_idx])
                    retry_map.append((original_offset, work_path))
                    continue

                results[start_idx + original_offset] = _write_output(
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
                    original_offset, work_path = retry_map[retry_idx]
                    if not retry_text or not retry_text.strip():
                        results[start_idx + original_offset] = ValueError("Empty model output after retry")
                        print(f"❌ Error processing {work_path}: Empty model output after retry")
                        continue
                    results[start_idx + original_offset] = _write_output(
                        work_path,
                        retry_text,
                    )
        except Exception as e:
            for original_offset, _ in batch_map:
                results[start_idx + original_offset] = e

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

    for work_path, result in zip(WORK_FILES, results):
        if isinstance(result, Exception):
            print(f"❌ Error processing {work_path}: {result}")


if __name__ == "__main__":
    main()
