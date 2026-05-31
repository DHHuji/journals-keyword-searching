import asyncio
import csv
import datetime
import email.utils
import json
import sys
from pathlib import Path

import aiohttp
from tqdm.asyncio import tqdm

JOURNALS_INPUT = "journals.csv"
CONCURRENCY = 1
RATE_LIMIT = 10  # per second
OUTPUT_BASE_DIR = Path("search_results")
KEYWORDS = [
    #"israel",
    #"australia",
    #"france",
    #"belgium",
    #"syria",
    #"lebanon",
    #"egypt",
    #"iraq",
    #"(\"south africa\")",
    #"germany",
    #"japan",
    #"italy",
    #"(\"united states\" OR usa)",
    #"spain",
    #"jordan",
    #"algeria",
    #"switzerland",
    "russia",
    "turkey"
]

OUTPUT_BASE_DIR.mkdir(exist_ok=True)


def _normalize_keyword(keyword):
    normalized = keyword.strip().lower()
    normalized = normalized.replace('"', '')
    normalized = normalized.replace("(", "").replace(")", "")
    normalized = normalized.replace(" OR ", "_")
    normalized = normalized.replace(" ", "_")
    while "__" in normalized:
        normalized = normalized.replace("__", "_")
    return normalized.strip("_")


def _parse_retry_after_seconds(retry_after_value):
    if not retry_after_value:
        return None

    try:
        return max(0.0, float(retry_after_value))
    except ValueError:
        pass

    try:
        retry_at = email.utils.parsedate_to_datetime(retry_after_value)
    except (TypeError, ValueError, IndexError, OverflowError):
        return None

    if retry_at.tzinfo is None:
        retry_at = retry_at.replace(tzinfo=datetime.timezone.utc)

    now = datetime.datetime.now(datetime.timezone.utc)
    return max(0.0, (retry_at - now).total_seconds())


async def fetch_page(session, source_id, keyword, page, rate_limiter):
    while True:
        async with rate_limiter:
            url = f"https://api.openalex.org/works"
            params = {
                'page': page,
                'filter': f'title_and_abstract.search:{keyword},primary_location.source.id:{source_id}',
                'per_page': 100,
                'mailto': 'reallyliri@gmail.com'
            }

            async with session.get(url, params=params) as response:
                if response.status == 429:
                    retry_after_seconds = _parse_retry_after_seconds(
                        response.headers.get("Retry-After")
                    )
                    if retry_after_seconds is None:
                        raise Exception(
                            f"HTTP 429 for source {source_id}, page {page} without a valid Retry-After header"
                        )

                    retry_after_hours = retry_after_seconds / 3600
                    print(
                        f"HTTP 429 for source {source_id}, page {page}. "
                        f"Retry-After: {retry_after_hours:.2f} hours. Waiting..."
                    )
                    await asyncio.sleep(retry_after_seconds)
                    continue

                if response.status != 200:
                    raise Exception(f"HTTP {response.status} for source {source_id}, page {page}")
                data = await response.json()
                return data


async def process_source_id(session, source_id, keyword, output_dir, semaphore, rate_limiter):
    async with semaphore:
        all_results = []
        page = 1

        while True:
            try:
                data = await fetch_page(session, source_id, keyword, page, rate_limiter)

                results = data.get('results', [])
                if not results:
                    break

                all_results.extend(results)

                meta = data.get('meta', {})
                if page >= meta.get('count', 0) / 100:
                    break

                page += 1

            except Exception as e:
                raise Exception(f"Error processing source {source_id}, page {page}: {str(e)}")

        output_file = f"{output_dir}/{source_id}.json"
        with open(output_file, 'w') as f:
            json.dump(all_results, f, indent=2)

        return len(all_results)


async def main():
    try:
        source_ids = []

        with open(JOURNALS_INPUT, 'r', newline='') as csvfile:
            reader = csv.DictReader(csvfile)

            if 'OpenAlexSourceId' not in reader.fieldnames:
                raise Exception("CSV file must contain 'OpenAlexSourceId' column")

            for row in reader:
                source_id = row['OpenAlexSourceId']
                if source_id and source_id.strip():
                    source_id = source_id.strip()
                    if source_id not in source_ids:
                        source_ids.append(source_id)

        if not source_ids:
            raise Exception("No valid OpenAlexSourceId values found in CSV")

        semaphore = asyncio.Semaphore(CONCURRENCY)
        rate_limiter = asyncio.Semaphore(RATE_LIMIT)

        async with aiohttp.ClientSession() as session:
            total_results = 0

            for keyword in KEYWORDS:
                normalized_keyword = _normalize_keyword(keyword)
                output_dir = OUTPUT_BASE_DIR / normalized_keyword
                output_dir.mkdir(exist_ok=True)

                tasks = [
                    process_source_id(session, source_id, keyword, output_dir, semaphore, rate_limiter)
                    for source_id in source_ids
                ]
                results = await tqdm.gather(*tasks, desc=f"Processing sources [{normalized_keyword}]")
                total_results += sum(results)

        print(f"Total results fetched: {total_results}")

    except Exception as e:
        print(f"Error: {str(e)}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
