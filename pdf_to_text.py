#!/usr/bin/env python3

import os
import sys
from pathlib import Path
from PyPDF2 import PdfReader

def pdf_to_text(pdf_path, output_path):
    try:
        reader = PdfReader(pdf_path)
        text = ""

        for page_num, page in enumerate(reader.pages):
            page_text = page.extract_text()
            if page_text:
                text += page_text

        lines = text.split('\n')
        processed_lines = []
        i = 0

        while i < len(lines):
            line = lines[i]

            while i + 1 < len(lines) and line.rstrip().endswith('-'):
                line = line.rstrip()[:-1] + lines[i + 1].lstrip()
                i += 1

            processed_lines.append(line)
            i += 1

        processed_text = '\n'.join(processed_lines)

        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(processed_text)

        print(f"Converted: {pdf_path} -> {output_path}")

    except Exception as e:
        print(f"Error processing {pdf_path}: {e}", file=sys.stderr)

def process_directory(directory):
    dir_path = Path(directory)

    if not dir_path.exists():
        print(f"Directory {directory} does not exist", file=sys.stderr)
        sys.exit(1)

    pdf_files = list(dir_path.rglob("*.pdf"))

    if not pdf_files:
        print(f"No PDF files found in {directory}")
        return

    print(f"Found {len(pdf_files)} PDF files")

    for pdf_file in pdf_files:
        output_file = pdf_file.with_suffix('.txt')
        pdf_to_text(pdf_file, output_file)

def main():
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <directory>", file=sys.stderr)
        sys.exit(1)

    directory = sys.argv[1]
    process_directory(directory)

if __name__ == "__main__":
    main()