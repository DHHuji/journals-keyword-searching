#!/usr/bin/env python3

import sys
import os
import csv
import re
from pathlib import Path

PDFS_DIR = "pdfs"
CITATIONS_FILENAME = "citations.txt"
OUTPUT_FILENAME = "index.csv"
STANDARD_FIELDS = [
    'entry_type', 'citation_key', 'title', 'author', 'year',
    'journal', 'volume', 'number', 'pages', 'publisher',
    'issn', 'url', 'urldate'
]


def parse_bibtex_entry(entry):
    """Parse a single BibTeX entry and return a dictionary of fields."""
    fields = {}

    entry_match = re.match(r'@(\w+)\{([^,]+),', entry.strip())
    if entry_match:
        fields['entry_type'] = entry_match.group(1)
        fields['citation_key'] = entry_match.group(2)

    field_pattern = r'^\s*(\w+)\s*=\s*\{([^}]*)\}(?:,)?$'

    lines = entry.split('\n')

    for i, line in enumerate(lines[1:]):
        if line.strip() == '}':
            break

        match = re.match(field_pattern, line)
        if match:
            field_name = match.group(1).lower()
            field_value = match.group(2)

            j = i + 2
            while j < len(lines) and not re.match(r'^\s*(\w+)\s*=\s*\{', lines[j]) and lines[j].strip() != '}':
                if lines[j].strip() and not lines[j].strip().endswith(','):
                    field_value += ' ' + lines[j].strip()
                elif lines[j].strip().endswith(','):
                    field_value += ' ' + lines[j].strip()[:-1]
                    break
                j += 1

            fields[field_name] = field_value.strip()

    return fields


def process_file(file_path):
    """Process a single BibTeX file and return list of parsed entries."""
    entries = []

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()

        entry_pattern = r'@\w+\{[^@]+?\n\}'
        matches = re.findall(entry_pattern, content, re.DOTALL)

        for match in matches:
            parsed = parse_bibtex_entry(match)
            if parsed:
                entries.append(parsed)

    except Exception as e:
        print(f"Error processing {file_path}: {e}", file=sys.stderr)

    return entries


def collect_all_fields(all_entries):
    """Collect all unique field names from all entries."""
    fields = set()
    for entry in all_entries:
        fields.update(entry.keys())

    ordered_fields = []
    for field in STANDARD_FIELDS:
        if field in fields:
            ordered_fields.append(field)
            fields.discard(field)

    ordered_fields.extend(sorted(fields))

    return ordered_fields


def process_directory(dir_path):
    """Process all .txt files in a directory and create a CSV."""
    txt_files = list(dir_path.glob("*.txt"))

    if not txt_files:
        return 0

    print(f"  Processing {len(txt_files)} files in {dir_path.name}...")

    all_entries = []
    seen_urls = set()

    for txt_file in txt_files:
        entries = process_file(txt_file)
        file_unique = 0

        for entry in entries:
            url = entry.get('url', '')
            if url and url not in seen_urls:
                seen_urls.add(url)
                all_entries.append(entry)
                file_unique += 1
            elif not url:
                all_entries.append(entry)
                file_unique += 1

        print(f"    - {txt_file.name}: {len(entries)} entries ({file_unique} unique)")

    if not all_entries:
        print(f"    No citations found in {dir_path.name}")
        return 0

    fieldnames = collect_all_fields(all_entries)

    output_file = dir_path / OUTPUT_FILENAME

    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_entries)

    print(f"    Created {output_file} with {len(all_entries)} unique citations")
    return len(all_entries)


def main():
    pdfs_dir = Path(PDFS_DIR)

    if not pdfs_dir.exists():
        print(f"Error: Directory {pdfs_dir} does not exist")
        sys.exit(1)

    dirs_with_citations = []

    for subdir in pdfs_dir.iterdir():
        if subdir.is_dir():
            citations_file = subdir / CITATIONS_FILENAME
            if citations_file.exists():
                dirs_with_citations.append(subdir)

    if not dirs_with_citations:
        print(f"No directories with {CITATIONS_FILENAME} found in {PDFS_DIR}/")
        sys.exit(1)

    print(f"Found {len(dirs_with_citations)} directories with {CITATIONS_FILENAME}")
    print("Processing citations...\n")

    total_citations = 0
    processed_dirs = 0

    for dir_path in sorted(dirs_with_citations):
        count = process_directory(dir_path)
        if count > 0:
            total_citations += count
            processed_dirs += 1

    print(f"\nSummary: Processed {processed_dirs} directories, {total_citations} total unique citations")


if __name__ == "__main__":
    main()
