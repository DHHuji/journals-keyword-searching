import asyncio
import csv
import json
from pathlib import Path
from collections import defaultdict

import aiohttp
from tqdm.asyncio import tqdm

RESULTS_DIR = 'search_results'
OUTPUT_AUTHORS_FILE = 'authors_works.csv'
STATE_FILE = 'authors_works_state.json'
STATE_PATH = Path(__file__).resolve().with_name(STATE_FILE)

CONCURRENCY = 5
RATE_LIMIT = 10
REQUEST_TIMEOUT_SECONDS = 60
MAX_RETRIES = 3


class RateLimitError(Exception):
    pass


def _load_state():
    if not STATE_PATH.exists():
        return {
            'author_ids': [],
            'author_works': {},
            'search_work_ids': [],
            'failed_author_ids': [],
            'finalized': False,
        }

    with open(STATE_PATH, 'r', encoding='utf-8') as f:
        state = json.load(f)

    if not isinstance(state, dict):
        return {
            'author_ids': [],
            'author_works': {},
            'search_work_ids': [],
            'failed_author_ids': [],
            'finalized': False,
        }

    author_ids = state.get('author_ids', [])
    author_works = state.get('author_works', {})
    search_work_ids = state.get('search_work_ids', [])
    failed_author_ids = state.get('failed_author_ids', [])
    finalized = state.get('finalized', False)

    return {
        'author_ids': author_ids if isinstance(author_ids, list) else [],
        'author_works': author_works if isinstance(author_works, dict) else {},
        'search_work_ids': search_work_ids if isinstance(search_work_ids, list) else [],
        'failed_author_ids': failed_author_ids if isinstance(failed_author_ids, list) else [],
        'finalized': bool(finalized),
    }


def _save_state(author_ids, author_works, search_work_ids, failed_author_ids, finalized=False):
    with open(STATE_PATH, 'w', encoding='utf-8') as f:
        json.dump(
            {
                'author_ids': author_ids,
                'author_works': author_works,
                'search_work_ids': search_work_ids,
                'failed_author_ids': failed_author_ids,
                'pending_author_ids': [author_id for author_id in author_ids if author_id not in author_works],
                'completed_authors_count': len(author_works),
                'total_authors_count': len(author_ids),
                'finalized': finalized,
            },
            f,
            ensure_ascii=False,
            indent=2,
        )


def _load_search_results_work_ids():
    work_ids = set()
    results_dir = Path(RESULTS_DIR)
    for json_file in results_dir.glob('**/*.json'):
        if json_file.name == 'llm_outputs.json':
            continue
        with open(json_file, 'r', encoding='utf-8') as f:
            json_data = json.load(f)
            for item in json_data:
                work_id = item.get('id', '').replace('https://openalex.org/', '')
                if work_id:
                    work_ids.add(work_id)
    return work_ids


def _extract_author_works(json_data):
    author_works = defaultdict(list)

    for item in json_data:
        for authorship in item.get('authorships', []):
            author = authorship.get('author', {})
            if not author:
                continue

            author_id = author.get('id', '')
            if author_id:
                author_id = author_id.replace('https://openalex.org/', '')
                if author_id:
                    author_works[author_id].append(item)

    return author_works


def _clean_author_name(name):
    return name.strip("'\"ʻʼ'ʽ`´") if name else ''


def _extract_work_data_for_author(item, authorship, authorship_index, search_work_ids):
    row = {}

    openalex_prefix = 'https://openalex.org/'
    row['id'] = item.get('id', '').replace(openalex_prefix, '')
    row['doi'] = item.get('doi', '')
    row['title'] = item.get('title', '')
    row['publication_date'] = item.get('publication_date', '')

    primary_location = item.get('primary_location', {})
    source = primary_location.get('source', {}) if primary_location else {}
    row['source_id'] = source.get('id', '').replace(openalex_prefix, '') if source else ''
    row['journal_name'] = source.get('display_name', '') if source else ''

    raw_name = authorship.get('raw_author_name', '')
    row['author_name'] = _clean_author_name(raw_name)

    author_id = ''
    author = authorship.get('author', {})
    if author:
        author_id = (author.get('id', '') or '').replace(openalex_prefix, '')
    row['author_id'] = author_id

    other_authors = []
    for other_index, other_authorship in enumerate(item.get('authorships', [])):
        if other_index == authorship_index:
            continue
        other_name = _clean_author_name(other_authorship.get('raw_author_name', ''))
        if other_name:
            other_authors.append(other_name)
    row['others'] = ';'.join(other_authors)

    institutions = authorship.get('institutions', [])
    all_institutions = []
    for inst in institutions:
        if inst and inst.get('display_name'):
            all_institutions.append(inst.get('display_name'))

    countries = authorship.get('countries', [])
    all_countries = [country for country in countries if country]

    all_affiliations = []
    if not all_institutions:
        raw_affiliation_strings = authorship.get('raw_affiliation_strings', []) or []
        for affiliation in raw_affiliation_strings:
            if affiliation and affiliation != "View further author information":
                all_affiliations.append(affiliation)

    row['institutions'] = ';'.join(sorted(all_institutions))
    row['countries'] = ';'.join(sorted(all_countries))
    row['affiliations_comment'] = ';'.join(sorted(all_affiliations)) if not all_institutions else ''
    row['cited_by_count'] = item.get('cited_by_count', 0)

    keywords = item.get('keywords', [])
    keyword_names = [kw.get('display_name', '') for kw in keywords if kw.get('display_name')]
    row['keywords'] = ';'.join(keyword_names)

    row['references_israel'] = 'Yes' if row['id'] in search_work_ids else 'No'

    return row


async def fetch_author_works(session, author_id, rate_limiter):
    url = "https://api.openalex.org/works"
    params = {
        'filter': f'author.id:{author_id}',
        'per-page': 200,
        'mailto': 'reallyliri@gmail.com'
    }

    all_works = []
    cursor = '*'

    while cursor:
        params['cursor'] = cursor
        attempt = 0

        while True:
            attempt += 1
            try:
                async with rate_limiter:
                    async with session.get(url, params=params) as response:
                        if response.status == 429:
                            retry_after = response.headers.get('Retry-After', '')
                            retry_after_hours = ''
                            if retry_after:
                                try:
                                    retry_after_hours = str(float(retry_after) / 3600)
                                except ValueError:
                                    retry_after_hours = retry_after
                            raise RateLimitError(
                                f"OpenAlex rate limit hit for author {author_id}. "
                                f"Retry-After hours: {retry_after_hours or 'unknown'}"
                            )
                        if response.status != 200:
                            print(f"Warning: HTTP {response.status} for author {author_id}")
                            return all_works

                        data = await response.json()
                        results = data.get('results', [])
                        all_works.extend(results)

                        meta = data.get('meta', {})
                        next_cursor = meta.get('next_cursor')
                        cursor = next_cursor if next_cursor else None

                        if len(results) < 200:
                            cursor = None

                        break
            except RateLimitError:
                raise
            except (asyncio.TimeoutError, aiohttp.ClientError) as exc:
                if attempt >= MAX_RETRIES:
                    raise RuntimeError(
                        f"Failed to fetch author {author_id} after {MAX_RETRIES} attempts: {exc}"
                    ) from exc
                print(f"Warning: retrying author {author_id} after error: {exc}")
                await asyncio.sleep(attempt)

    return all_works


async def enrich_authors_with_all_works(author_ids, search_work_ids):
    semaphore = asyncio.Semaphore(CONCURRENCY)
    rate_limiter = asyncio.Semaphore(RATE_LIMIT)
    state = _load_state()
    author_works_dict = state['author_works']
    failed_author_ids = set(state['failed_author_ids'])
    _save_state(author_ids, author_works_dict, search_work_ids, sorted(failed_author_ids), finalized=False)
    pending_author_ids = [author_id for author_id in author_ids if author_id not in author_works_dict]

    async def fetch_single_author_works(author_id):
        async with semaphore:
            try:
                works = await fetch_author_works(session, author_id, rate_limiter)
                return author_id, works, None
            except Exception as exc:
                return author_id, None, str(exc)

    if not pending_author_ids:
        return author_works_dict, failed_author_ids

    timeout = aiohttp.ClientTimeout(total=None, connect=REQUEST_TIMEOUT_SECONDS, sock_connect=REQUEST_TIMEOUT_SECONDS)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        tasks = [
            asyncio.create_task(fetch_single_author_works(author_id))
            for author_id in pending_author_ids
        ]

        with tqdm(total=len(tasks), desc="Fetching all works for authors") as progress:
            for task in asyncio.as_completed(tasks):
                author_id, works, error = await task
                if error is None:
                    author_works_dict[author_id] = works
                    failed_author_ids.discard(author_id)
                else:
                    failed_author_ids.add(author_id)
                    print(f"Warning: skipping author {author_id}: {error}")
                _save_state(author_ids, author_works_dict, search_work_ids, sorted(failed_author_ids), finalized=False)
                progress.update(1)

    return author_works_dict, failed_author_ids


def _build_output_rows(unique_author_ids, all_author_works, search_work_ids):
    seen_work_author_pairs = set()
    output_rows = []

    for author_id in unique_author_ids:
        if author_id not in all_author_works:
            continue

        works = all_author_works[author_id]
        for work in works:
            work_id = (work.get('id', '') or '').replace('https://openalex.org/', '')
            if not work_id:
                continue

            for authorship_index, authorship in enumerate(work.get('authorships', [])):
                author = authorship.get('author', {})
                current_author_id = (author.get('id', '') or '').replace('https://openalex.org/', '')
                pair_key = (work_id, current_author_id, authorship_index)

                if pair_key in seen_work_author_pairs:
                    continue

                seen_work_author_pairs.add(pair_key)
                work_data = _extract_work_data_for_author(work, authorship, authorship_index, search_work_ids)
                if work_data['id'] and work_data['author_name']:
                    output_rows.append(work_data)

    return output_rows


def _write_output_rows(output_rows):
    with open(OUTPUT_AUTHORS_FILE, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = [
            'id', 'doi', 'title', 'publication_date', 'source_id',
            'journal_name', 'author_name', 'author_id', 'others', 'institutions', 'countries',
            'affiliations_comment', 'cited_by_count', 'keywords', 'references_israel'
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        writer.writerows(output_rows)


def main():
    state = _load_state()
    unique_author_ids = state['author_ids']
    search_work_ids = set(state['search_work_ids'])

    if unique_author_ids and search_work_ids:
        print(f"Loaded {len(search_work_ids)} search result work IDs from {STATE_FILE}")
        print(f"Loaded {len(unique_author_ids)} author tasks from {STATE_FILE}")
    else:
        results_dir = Path(RESULTS_DIR)
        all_json_data = []

        print("Loading search results work IDs...")
        search_work_ids = _load_search_results_work_ids()
        print(f"Found {len(search_work_ids)} unique work IDs in search results")

        for json_file in results_dir.glob('**/*.json'):
            if json_file.name == 'llm_outputs.json':
                continue
            print(f"Processing {json_file.relative_to(results_dir)}...")
            with open(json_file, 'r', encoding='utf-8') as f:
                json_data = json.load(f)
                all_json_data.extend(json_data)

        author_works_from_search = _extract_author_works(all_json_data)
        unique_author_ids = list(author_works_from_search.keys())

        print(f"Found {len(unique_author_ids)} unique authors in search results")
        _save_state(
            unique_author_ids,
            state['author_works'],
            sorted(search_work_ids),
            state['failed_author_ids'],
            finalized=False,
        )

    if unique_author_ids:
        print(f"Fetching all works for {len(unique_author_ids)} authors from OpenAlex API...")
        all_author_works, failed_author_ids = asyncio.run(
            enrich_authors_with_all_works(unique_author_ids, sorted(search_work_ids))
        )
        pending_author_ids = [author_id for author_id in unique_author_ids if author_id not in all_author_works]

        if pending_author_ids:
            _save_state(unique_author_ids, all_author_works, sorted(search_work_ids), sorted(failed_author_ids), finalized=False)
            print(
                f"Saved partial state to {STATE_FILE}: "
                f"{len(all_author_works)}/{len(unique_author_ids)} authors collected"
            )
            return

        output_rows = _build_output_rows(unique_author_ids, all_author_works, search_work_ids)
        if output_rows:
            _write_output_rows(output_rows)
            _save_state(unique_author_ids, all_author_works, sorted(search_work_ids), sorted(failed_author_ids), finalized=True)
            print(f"Successfully created {OUTPUT_AUTHORS_FILE} with {len(output_rows)} author-work pairs")
        else:
            _save_state(unique_author_ids, all_author_works, sorted(search_work_ids), sorted(failed_author_ids), finalized=True)
            print("No works data found to process")
    else:
        print("No authors data found to process")


if __name__ == "__main__":
    main()
