#!/bin/bash
#SBATCH --cpus-per-task=16
#SBATCH --mem=500G
#SBATCH --time=1-00:00:00
#SBATCH -e sync_logs/vllm_%j.err
#SBATCH -o sync_logs/vllm_%j.out
#SBATCH --gres=gpu:h200:4

cd /sci/labs/dh_huji/liri.sokol/vllm-playground
SRC_DIR="/sci/labs/dh_huji/liri.sokol/journals-keyword-searching/vllm"

export OMP_NUM_THREADS=8

PYTHONPATH="${SRC_DIR}" uv run "${SRC_DIR}/process.py" llama4 --gpu-count 4
