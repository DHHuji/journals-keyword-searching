#!/bin/bash
#SBATCH --cpus-per-task=10
#SBATCH --mem-per-cpu=2000
#SBATCH --time=7-00:00:00
#SBATCH -e sync_logs/rclone.err
#SBATCH -o sync_logs/rclone.out

PYTHONPATH="${SRC_DIR}" uv run authors_works.py
