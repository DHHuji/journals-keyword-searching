# LLM analysis v3: abstract metadata only

V3 runs the two independent classifications separately over every `search_results/works_*.csv` record. Each model request contains only the record ID, title, publication date, OpenAlex keywords, abstract, and `focal_country`; it does not use PDFs or full text.

The country field sent to the model and written to v3 results is `focal_country`. The OpenAlex topic-tags field remains `keywords`.

## Run

Run sentiment first (recommended for the pilot), then themes:

```bash
PYTHONPATH=llm_analysis uv run llm_analysis/process.py gptoss sentiment --gpu-count 1
PYTHONPATH=llm_analysis uv run llm_analysis/process.py gptoss themes --gpu-count 1
```

To run a pilot subset, provide `--work-selection-csv` with a UTF-8 CSV containing `id` and `focal_country` columns. The runner selects exact ID/country pairs, so a work that appears under more than one country is run only for the country selected in the CSV. A legacy `keyword` country column is also accepted.

```bash
PYTHONPATH=llm_analysis uv run llm_analysis/process.py gptoss sentiment --gpu-count 1 --work-selection-csv pilot_selection.csv
sbatch sbatch/vllm_gptoss_works_theme.sh /absolute/path/pilot_selection.csv
```

Results are resumable and kept separate:

- `search_results/llm_outputs_v3/sentiment/`
- `search_results/llm_outputs_v3/themes/`

An existing output file is skipped, so use a different `--output-dir` for a deliberately fresh run. Before spending GPU time, inspect the exact constructed prompts with:

```bash
PYTHONPATH=llm_analysis uv run llm_analysis/process.py gptoss sentiment --gpu-count 1 --dry-run --batch-size 1
PYTHONPATH=llm_analysis uv run llm_analysis/process.py gptoss themes --gpu-count 1 --dry-run --batch-size 1
```

## Consolidate results

After each run, join its JSON output with all original OpenAlex metadata:

```bash
PYTHONPATH=llm_analysis uv run llm_analysis/outputs_combine_v3.py sentiment
PYTHONPATH=llm_analysis uv run llm_analysis/outputs_combine_v3.py themes
```

This writes `search_results/llm_outputs_v3_sentiment.{csv,json}` and `search_results/llm_outputs_v3_themes.{csv,json}`. The two output tables are intentionally separate: their analyses are independent and may be run or reviewed on different schedules.
