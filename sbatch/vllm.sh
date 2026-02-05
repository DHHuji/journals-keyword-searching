#!/bin/bash
#SBATCH --cpus-per-task=8
#SBATCH --mem=200G
#SBATCH --time=1-00:00:00
#SBATCH -e sync_logs/vllm_%j.err
#SBATCH -o sync_logs/vllm_%j.out
#SBATCH --gres=gpu:h200:2

cd /sci/labs/dh_huji/liri.sokol/vllm-playground
SRC_DIR="/sci/labs/dh_huji/liri.sokol/journals-keyword-searching/vllm"

PYTHONPATH="${SRC_DIR}" uv run --directory "${SRC_DIR}" process.py
