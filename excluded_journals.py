import csv

JOURNALS_FILE = 'journals.csv'


def load_excluded_source_ids():
    with open(JOURNALS_FILE, 'r', newline='', encoding='utf-8-sig') as f:
        return {
            row['OpenAlexSourceId'].strip()
            for row in csv.DictReader(f)
            if row.get('excluded', '').strip() == '1'
        }
