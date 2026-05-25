#!/bin/bash
#SBATCH --cpus-per-task=10
#SBATCH --mem-per-cpu=2000
#SBATCH --time=7-00:00:00
#SBATCH -e sync_logs/authors_works.err
#SBATCH -o sync_logs/authors_works.out

PYTHONPATH="${SRC_DIR}" uv run authors_works.py

