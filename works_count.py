import asyncio
import csv
import sys

import aiohttp
from tqdm.asyncio import tqdm


JOURNALS_FILE = "journals.csv"
OUTPUT_FILE = "works_counts_by_year.csv"
CONCURRENCY = 5
MAX_RETRIES = 5
OPENALEX_API_URL = "https://api.openalex.org/works"
OPENALEX_MAILTO = "reallyliri@gmail.com"


def _load_journals():
    journals = []
    seen_source_ids = set()

    with open(JOURNALS_FILE, "r", newline="", encoding="utf-8-sig") as csvfile:
        first_line = csvfile.readline()
        if first_line.startswith("version https://git-lfs.github.com/spec/v1"):
            raise RuntimeError(
                f"{JOURNALS_FILE} is a Git LFS pointer, not the downloaded CSV. "
                f"Run: git lfs pull --include='{JOURNALS_FILE}'"
            )
        csvfile.seek(0)

        reader = csv.DictReader(csvfile)
        required_columns = {"OpenAlexSourceId", "Journal Name"}
        if not reader.fieldnames or not required_columns.issubset(reader.fieldnames):
            raise ValueError(
                "CSV file must contain 'OpenAlexSourceId' and 'Journal Name' columns"
            )

        for row in reader:
            source_id = row["OpenAlexSourceId"].strip()
            if not source_id:
                continue

            source_id = source_id.removeprefix("https://openalex.org/")
            if source_id in seen_source_ids:
                continue

            seen_source_ids.add(source_id)
            journals.append((source_id, row["Journal Name"].strip()))

    if not journals:
        raise ValueError("No valid journal entries found in CSV")

    return journals


async def _fetch_year_counts(session, source_id):
    params = {
        # This is the only filter: no keyword, date, author, or work-type filters.
        "filter": f"primary_location.source.id:{source_id}",
        "group_by": "publication_year",
        "mailto": OPENALEX_MAILTO,
    }

    for attempt in range(MAX_RETRIES):
        async with session.get(OPENALEX_API_URL, params=params) as response:
            if response.status == 200:
                return await response.json()

            if response.status == 429 or response.status >= 500:
                retry_after = response.headers.get("Retry-After")
                try:
                    delay = float(retry_after) if retry_after else 2**attempt
                except ValueError:
                    delay = 2**attempt

                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(delay)
                    continue

            response_text = await response.text()
            raise RuntimeError(
                f"OpenAlex returned HTTP {response.status} for {source_id}: "
                f"{response_text[:200]}"
            )

    raise RuntimeError(
        f"OpenAlex request failed after {MAX_RETRIES} attempts for {source_id}"
    )


async def _process_journal(session, source_id, journal_name, semaphore):
    async with semaphore:
        data = await _fetch_year_counts(session, source_id)

    rows = []
    for group in data.get("group_by", []):
        year = group.get("key")
        if year in (None, ""):
            continue

        rows.append(
            {
                "source_id": source_id,
                "journal_name": journal_name,
                "year": int(year),
                "works_count": group.get("count", 0),
            }
        )

    return sorted(rows, key=lambda row: row["year"])


async def main():
    try:
        journals = _load_journals()
        semaphore = asyncio.Semaphore(CONCURRENCY)

        timeout = aiohttp.ClientTimeout(total=60)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            tasks = [
                _process_journal(session, source_id, journal_name, semaphore)
                for source_id, journal_name in journals
            ]
            journal_results = await tqdm.gather(
                *tasks, desc="Counting works by journal"
            )

        rows = [row for journal_rows in journal_results for row in journal_rows]
        with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(
                csvfile,
                fieldnames=["source_id", "journal_name", "year", "works_count"],
                quoting=csv.QUOTE_ALL,
            )
            writer.writeheader()
            writer.writerows(rows)

        print(f"Successfully created {OUTPUT_FILE} with {len(rows)} records")

    except Exception as error:
        print(f"Error: {error}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
