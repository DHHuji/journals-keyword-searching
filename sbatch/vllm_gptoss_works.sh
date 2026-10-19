#!/bin/bash
#SBATCH --cpus-per-task=8
#SBATCH --mem=160G
#SBATCH --time=7-00:00:00
#SBATCH -e sync_logs/vllm_gptoss_%j.err
#SBATCH -o sync_logs/vllm_gptoss_%j.out
#SBATCH --gres=gpu:h200:1

cd /sci/labs/dh_huji/liri.sokol/vllm-playground
SRC_DIR="/sci/labs/dh_huji/liri.sokol/journals-keyword-searching/llm_analysis"

export OMP_NUM_THREADS=8

PYTHONPATH="${SRC_DIR}" uv run "${SRC_DIR}/process.py" gptoss --gpu-count 1
