import asyncio
import os
from pathlib import Path
from vllm_deepseek import llm_gen

MODEL = "deepseek-r1"

CONCURRENCY = 8

WORKS_BASE_DIR = "../pdfs"
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


async def process_work_file(work_path, prompt, themes, semaphore):
    async with semaphore:
        print(f"🔄 Processing {work_path}...")

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

        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(None, llm_gen, full_prompt)

        os.makedirs(OUTPUT_DIR, exist_ok=True)
        work_name = Path(work_path).stem
        output_path = os.path.join(OUTPUT_DIR, f"{work_name}_{MODEL}.txt")

        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(result[0])

        print(f"✅ Completed {work_path} -> {output_path}")
        return output_path


async def main():
    print("Starting parallel processing...\n")

    try:
        with open(PROMPT_FILE, 'r', encoding='utf-8') as f:
            prompt = f.read().strip()
        with open(THEMES_FILE, 'r', encoding='utf-8') as f:
            themes = f.read().strip()
    except FileNotFoundError as e:
        print(f"❌ Error: {e}")
        raise

    semaphore = asyncio.Semaphore(CONCURRENCY)

    tasks = [
        process_work_file(work_path, prompt, themes, semaphore)
        for work_path in WORK_FILES
    ]

    start_time = asyncio.get_event_loop().time()
    results = await asyncio.gather(*tasks, return_exceptions=True)
    end_time = asyncio.get_event_loop().time()

    print("\n" + "="*60)
    successful = sum(1 for r in results if r and not isinstance(r, Exception))
    failed = len(results) - successful
    print(f"✨ Processing complete!")
    print(f"   Successful: {successful}/{len(results)}")
    print(f"   Failed: {failed}/{len(results)}")
    print(f"   Total time: {end_time - start_time:.2f}s")
    print("="*60)

    for work_path, result in zip(WORK_FILES, results):
        if isinstance(result, Exception):
            print(f"❌ Error processing {work_path}: {result}")


if __name__ == "__main__":
    asyncio.run(main())
