import csv
import re
import unicodedata
from collections import defaultdict

INPUT_FILE = 'authors_works.csv'
OUTPUT_FILE = 'authors_works_aggregated.csv'


def normalize_name(name):
    if not name:
        return ""

    name = unicodedata.normalize('NFKD', name)
    name = ''.join(c for c in name if not unicodedata.combining(c))

    name = re.sub(r'[^\w\s-]', '', name)

    name = re.sub(r'[-\s]+', ' ', name)

    name = name.lower().strip()

    return name


def aggregate_authors():
    normalized_to_group_id = {}
    name_to_group_id = {}
    group_id_counter = 0
    group_normalized_names = {}

    author_data = defaultdict(lambda: {
        'author_ids': set(),
        'work_ids': set(),
        'specific_work_ids': set(),
        'cited_by_per_work': {},
        'years': set(),
        'source_ids': set(),
        'institutions': set(),
        'countries': set(),
        'affiliations': set()
    })

    print("Reading and processing data...")
    row_count = 0

    with open(INPUT_FILE, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)

        for row in reader:
            row_count += 1

            author_name = row.get('author_name', '').strip()
            if not author_name:
                continue

            work_id = row.get('id', '').strip()
            if not work_id:
                continue

            if author_name in name_to_group_id:
                group_id = name_to_group_id[author_name]
            else:
                normalized_name = normalize_name(author_name)

                if normalized_name in normalized_to_group_id:
                    group_id = normalized_to_group_id[normalized_name]
                else:
                    group_id = group_id_counter
                    group_id_counter += 1
                    normalized_to_group_id[normalized_name] = group_id
                    group_normalized_names[group_id] = normalized_name

                name_to_group_id[author_name] = group_id

            data = author_data[group_id]

            author_id = row.get('author_id', '').strip()
            if author_id:
                data['author_ids'].add(author_id)

            data['work_ids'].add(work_id)

            source_id = row.get('source_id', '').strip()
            if source_id:
                data['source_ids'].add(source_id)

            if row.get('references_israel', '').lower() in ['yes', 'true', '1']:
                data['specific_work_ids'].add(work_id)

            try:
                cited_by = int(row.get('cited_by_count', 0))
                if work_id not in data['cited_by_per_work']:
                    data['cited_by_per_work'][work_id] = cited_by
            except (ValueError, TypeError):
                pass

            pub_date = row.get('publication_date', '')
            if pub_date and len(pub_date) >= 4:
                try:
                    year = int(pub_date[:4])
                    if 1900 <= year <= 2100:
                        data['years'].add(year)
                except (ValueError, IndexError):
                    pass

            institutions = row.get('institutions', '')
            if institutions:
                for inst in institutions.split(';'):
                    inst = inst.strip()
                    if inst:
                        data['institutions'].add(inst)

            countries = row.get('countries', '')
            if countries:
                for country in countries.split(';'):
                    country = country.strip()
                    if country:
                        data['countries'].add(country)

            affiliations = row.get('affiliations_comment', '')
            if affiliations:
                for aff in affiliations.split(';'):
                    aff = aff.strip()
                    if aff:
                        data['affiliations'].add(aff)

    print(f"Total rows processed: {row_count:,}")
    print("Creating output...")

    output_rows = []
    for group_id, data in author_data.items():
        years_list = sorted(data['years']) if data['years'] else []
        normalized_name = group_normalized_names.get(group_id, '')
        title_case_name = ' '.join(word.capitalize() for word in normalized_name.split()) if normalized_name else ''

        row = {
            'author_name': title_case_name,
            'author_ids': ';'.join(sorted(data['author_ids'])),
            'works_count': len(data['work_ids']),
            'specific_works_count': len(data['specific_work_ids']),
            'journals_count': len(data['source_ids']),
            'cited_by_count': sum(data['cited_by_per_work'].values()),
            'min_year': years_list[0] if years_list else '',
            'max_year': years_list[-1] if years_list else '',
            'institutions': ';'.join(sorted(data['institutions'])),
            'countries': ';'.join(sorted(data['countries'])),
            'affiliations_comment': ';'.join(sorted(data['affiliations']))
        }
        output_rows.append(row)

    output_rows.sort(key=lambda x: x['works_count'], reverse=True)

    with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
        fieldnames = [
            'author_name', 'author_ids', 'works_count', 'specific_works_count',
            'journals_count', 'cited_by_count', 'min_year', 'max_year', 'institutions',
            'countries', 'affiliations_comment'
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(output_rows)

    print(f"Aggregated {len(author_data):,} unique author groups")
    print(f"Output saved to {OUTPUT_FILE}")

    return output_rows


def main():
    print("Aggregating author data...")
    print("Note: This version groups authors by EXACT normalized name match only")
    print("(spaces/dashes/special chars are normalized, but no fuzzy matching)")
    aggregate_authors()


if __name__ == "__main__":
    main()
