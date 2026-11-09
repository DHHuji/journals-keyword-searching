#!/bin/bash
# Run the independent v3 theme classification.
#SBATCH --cpus-per-task=8
#SBATCH --mem=160G
#SBATCH --time=7-00:00:00
#SBATCH -e sync_logs/vllm_gptoss_themes_%j.err
#SBATCH -o sync_logs/vllm_gptoss_themes_%j.out
#SBATCH --gres=gpu:h200:1

# Optional: sbatch vllm_gptoss_works_theme.sh /path/to/work_selection.csv
# The CSV contains exact id,focal_country pairs. Omit it to run all works.

cd /sci/labs/dh_huji/liri.sokol/vllm-playground
SRC_DIR="/sci/labs/dh_huji/liri.sokol/journals-keyword-searching/llm_analysis"

export OMP_NUM_THREADS=8

SELECTION_CSV_ARGS=()
if [ "$#" -gt 1 ]; then
  echo "Usage: sbatch $0 [work_selection_csv]" >&2
  exit 2
fi
if [ "$#" -eq 1 ]; then
  SELECTION_CSV_ARGS=(--work-selection-csv "$1")
fi

PYTHONPATH="${SRC_DIR}" uv run "${SRC_DIR}/process.py" gptoss themes --gpu-count 1 "${SELECTION_CSV_ARGS[@]}"
