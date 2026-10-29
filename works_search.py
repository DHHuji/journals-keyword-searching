import argparse
import asyncio
import csv
import datetime
import email.utils
import json
import os
import sys
from pathlib import Path

import aiohttp
from tqdm.asyncio import tqdm

JOURNALS_INPUT = "journals.csv"
CONCURRENCY = 1
RATE_LIMIT = 10  # per second
OUTPUT_BASE_DIR = Path("search_results")
OPENALEX_API_KEY = os.environ.get("OPENALEX_API_KEY")
KEYWORDS = [
    "israel",
    "australia",
    "france",
    "belgium",
    "syria",
    "lebanon",
    "egypt",
    "iraq",
    "(\"south africa\")",
    "germany",
    "japan",
    "italy",
    "(\"united states\" OR usa)",
    "spain",
    "jordan",
    "algeria",
    "switzerland",
    "russia",
    "turkey",
    "iran",
    #"palestine",
    #"(\"england\" OR \"united kingdom\" OR \"britain\")",
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


RETRY_AFTER_CHECK_INTERVAL_SECONDS = 5 * 60


def _parse_retry_after_deadline(retry_after_value):
    if not retry_after_value:
        return None

    try:
        delay_seconds = max(0.0, float(retry_after_value))
        return datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(
            seconds=delay_seconds
        )
    except ValueError:
        pass

    try:
        retry_at = email.utils.parsedate_to_datetime(retry_after_value)
    except (TypeError, ValueError, IndexError, OverflowError):
        return None

    if retry_at.tzinfo is None:
        retry_at = retry_at.replace(tzinfo=datetime.timezone.utc)

    return retry_at


async def _wait_until(deadline):
    """Wait for a UTC wall-clock deadline, checking it at least every five minutes."""
    while True:
        remaining_seconds = (
            deadline - datetime.datetime.now(datetime.timezone.utc)
        ).total_seconds()
        if remaining_seconds <= 0:
            return
        await asyncio.sleep(min(remaining_seconds, RETRY_AFTER_CHECK_INTERVAL_SECONDS))


async def fetch_page(
    session, source_id, keyword, page_or_cursor, rate_limiter,
    year_range=None, use_cursor=False, api_key=None,
):
    loc = f"cursor {page_or_cursor}" if use_cursor else f"page {page_or_cursor}"
    while True:
        async with rate_limiter:
            url = "https://api.openalex.org/works"
            params = {
                'filter': f'title_and_abstract.search:{keyword},primary_location.source.id:{source_id}',
                'per_page': 100,
                'mailto': 'reallyliri@gmail.com',
            }
            if use_cursor:
                params['cursor'] = page_or_cursor
            else:
                params['page'] = page_or_cursor

            if api_key:
                params['api_key'] = api_key

            if year_range:
                start_year, end_year = year_range
                params['filter'] += (
                    f',from_publication_date:{start_year}-01-01'
                    f',to_publication_date:{end_year}-12-31'
                )

            async with session.get(url, params=params) as response:
                if response.status == 429:
                    retry_after_deadline = _parse_retry_after_deadline(
                        response.headers.get("Retry-After")
                    )
                    if retry_after_deadline is None:
                        raise Exception(
                            f"HTTP 429 for source {source_id}, {loc} without a valid Retry-After header"
                        )

                    retry_after_seconds = max(
                        0.0,
                        (retry_after_deadline - datetime.datetime.now(datetime.timezone.utc)).total_seconds(),
                    )
                    retry_after_hours = retry_after_seconds / 3600
                    print(
                        f"HTTP 429 for source {source_id}, {loc}. "
                        f"Retry-After: {retry_after_hours:.2f} hours. Waiting..."
                    )
                    await _wait_until(retry_after_deadline)
                    continue

                if response.status != 200:
                    error_body = await response.text()
                    raise Exception(f"HTTP {response.status} for source {source_id}, {loc}: {error_body}")
                data = await response.json()
                return data


def _load_existing_results(output_file):
    if not output_file.exists():
        return []

    with output_file.open('r', encoding='utf-8') as f:
        results = json.load(f)

    if not isinstance(results, list):
        raise ValueError(f"Existing output file is not a JSON array: {output_file}")
    return results


def _upsert_results(existing_results, new_results):
    """Replace existing works by OpenAlex ID and append newly discovered works."""
    new_by_id = {
        result['id']: result
        for result in new_results
        if isinstance(result, dict) and result.get('id')
    }
    merged_results = []
    seen_ids = set()

    for result in existing_results:
        work_id = result.get('id') if isinstance(result, dict) else None
        if work_id:
            if work_id in seen_ids:
                continue
            seen_ids.add(work_id)
            merged_results.append(new_by_id.pop(work_id, result))
        else:
            merged_results.append(result)

    # OpenAlex work IDs are stable; the remaining IDs are new results.
    merged_results.extend(new_by_id.values())
    # A result without an ID cannot be safely upserted, but should not be discarded.
    merged_results.extend(
        result for result in new_results
        if not isinstance(result, dict) or not result.get('id')
    )
    return merged_results


MAX_BASIC_PAGES = 100  # OpenAlex basic pagination limit: page * per_page <= 10,000


def _load_partial_state(state_file, year_range):
    if not state_file.exists():
        return [], None

    with state_file.open('r', encoding='utf-8') as f:
        lines = [line for line in f if line.strip()]

    if not lines:
        return [], None

    header = json.loads(lines[0])
    expected_year_range = list(year_range) if year_range else None
    if header.get('year_range') != expected_year_range:
        return [], None

    entries = []
    last_cursor = None
    for line in lines[1:]:
        try:
            item = json.loads(line)
            if isinstance(item, dict) and 'results' in item:
                cursor = item.get('next_cursor')
                entries.append((item['results'], cursor))
                last_cursor = cursor
            elif isinstance(item, list):
                entries.append((item, None))
            else:
                break
        except json.JSONDecodeError:
            break
    return entries, last_cursor


def _start_partial_state(state_file, year_range):
    with state_file.open('w', encoding='utf-8') as f:
        f.write(json.dumps({'year_range': list(year_range) if year_range else None}) + "\n")


def _append_partial_state(state_file, results, next_cursor=None):
    with state_file.open('a', encoding='utf-8') as f:
        if next_cursor is not None:
            f.write(json.dumps({'results': results, 'next_cursor': next_cursor}) + "\n")
        else:
            f.write(json.dumps(results) + "\n")
        f.flush()


def _write_results(output_file, results):
    temporary_file = output_file.with_suffix('.json.tmp')
    with temporary_file.open('w', encoding='utf-8') as f:
        json.dump(results, f, indent=2)
    temporary_file.replace(output_file)


async def process_source_id(
    session, source_id, keyword, output_dir, semaphore, rate_limiter,
    year_range=None, upsert=False, use_cursor=False, api_key=None,
):
    async with semaphore:
        state_file = output_dir / f"{source_id}.partial.jsonl"
        completed_entries, last_cursor = _load_partial_state(state_file, year_range)

        if use_cursor and completed_entries and last_cursor is None:
            print(
                f"Notice: Source {source_id} partial state has {len(completed_entries)} pages without cursor tokens. "
                f"Restarting with cursor pagination."
            )
            completed_entries = []
            all_results = []
            cursor = '*'
            _start_partial_state(state_file, year_range)
        else:
            _start_partial_state(state_file, year_range)
            for page_results, page_cursor in completed_entries:
                _append_partial_state(state_file, page_results, next_cursor=page_cursor)
            all_results = [result for page_results, _ in completed_entries for result in page_results]
            cursor = last_cursor if last_cursor else '*'

        if use_cursor:
            if completed_entries:
                print(f"Resuming source {source_id} with cursor after {len(completed_entries)} pages")
            while True:
                try:
                    data = await fetch_page(
                        session, source_id, keyword, cursor, rate_limiter,
                        year_range=year_range, use_cursor=True, api_key=api_key,
                    )
                    results = data.get('results', [])
                    if not results:
                        break

                    meta = data.get('meta', {})
                    next_cursor = meta.get('next_cursor')

                    all_results.extend(results)
                    _append_partial_state(state_file, results, next_cursor=next_cursor)

                    if not next_cursor or next_cursor == cursor:
                        break
                    cursor = next_cursor
                except Exception as e:
                    raise Exception(f"Error processing source {source_id}, cursor {cursor}: {str(e)}")
        else:
            page = len(completed_entries) + 1
            if completed_entries:
                print(f"Resuming source {source_id} from page {page}")

            while True:
                if page > MAX_BASIC_PAGES:
                    print(
                        f"Warning: Source {source_id} reached OpenAlex basic pagination limit of "
                        f"{MAX_BASIC_PAGES * 100} records. Finishing with {len(all_results)} results."
                    )
                    break

                try:
                    data = await fetch_page(
                        session, source_id, keyword, page, rate_limiter,
                        year_range=year_range, use_cursor=False, api_key=api_key,
                    )

                    results = data.get('results', [])
                    if not results:
                        break

                    all_results.extend(results)
                    _append_partial_state(state_file, results)

                    meta = data.get('meta', {})
                    total_count = meta.get('count', 0)

                    if page >= MAX_BASIC_PAGES:
                        if total_count > MAX_BASIC_PAGES * 100:
                            print(
                                f"Warning: Source {source_id} has {total_count} total results, "
                                f"but OpenAlex basic pagination is limited to {MAX_BASIC_PAGES * 100} records. "
                                f"Capping at {len(all_results)} results. "
                                f"(Provide an OpenAlex API key and --cursor to fetch >10,000 records)."
                            )
                        break

                    if page >= total_count / 100:
                        break

                    page += 1

                except Exception as e:
                    raise Exception(f"Error processing source {source_id}, page {page}: {str(e)}")

        fetched_count = len(all_results)
        output_file = output_dir / f"{source_id}.json"
        if upsert:
            existing_results = _load_existing_results(output_file)
            all_results = _upsert_results(existing_results, all_results)
        _write_results(output_file, all_results)
        state_file.unlink(missing_ok=True)

        return fetched_count


async def main():
    # Usage:
    #   python3 works_search.py --upsert START_YEAR END_YEAR
    # Re-fetches only the inclusive publication-year range and merges it into
    # existing JSON output files by OpenAlex work ID. Existing works outside
    # that range are retained; matching IDs are replaced with fresh data.
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true")
    parser.add_argument(
        "--upsert",
        nargs=2,
        type=int,
        metavar=("START_YEAR", "END_YEAR"),
        help=(
            "Fetch only this inclusive publication-year range and merge the results "
            "into existing source JSON files by OpenAlex work ID."
        ),
    )
    args = parser.parse_args()

    api_key = OPENALEX_API_KEY
    use_cursor = bool(api_key)

    if args.upsert:
        start_year, end_year = args.upsert
        if not (1 <= start_year <= end_year <= 9999):
            parser.error("--upsert requires years in ascending order between 1 and 9999")

    try:
        source_ids = []

        with open(JOURNALS_INPUT, 'r', newline='') as csvfile:
            reader = csv.DictReader(csvfile)

            if 'OpenAlexSourceId' not in reader.fieldnames:
                raise Exception("CSV file must contain 'OpenAlexSourceId' column")

            for row in reader:
                if row.get('excluded', '').strip() == '1':
                    continue
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
                    process_source_id(
                        session,
                        source_id,
                        keyword,
                        output_dir,
                        semaphore,
                        rate_limiter,
                        year_range=tuple(args.upsert) if args.upsert else None,
                        upsert=bool(args.upsert),
                        use_cursor=use_cursor,
                        api_key=api_key,
                    )
                    for source_id in source_ids
                    if args.upsert or args.force or not (output_dir / f"{source_id}.json").exists()
                ]
                results = await tqdm.gather(*tasks, desc=f"Processing sources [{normalized_keyword}]")
                total_results += sum(results)

        print(f"Total results fetched: {total_results}")

    except Exception as e:
        print(f"Error: {str(e)}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
