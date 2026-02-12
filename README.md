# Methodology

This project examines how Israel is discussed in academic journals across several disciplines (Sociology, Anthropology, Middle East Studies, Genocide Studies, Human Rights). Below is a step-by-step description of how each data file was produced, in the order they were built.

---

# Part I: Broad Search Across OpenAlex

This part covers the collection and analysis of article metadata (titles, abstracts, keywords, authors) across all 69 journals, using the OpenAlex open database as the primary data source.

---

## Step 1: Defining the Journals (`journals.csv`)

A list of 69 academic journals was manually compiled, spanning five disciplines. For each journal, we recorded its name, category, access type, and its identifier in the OpenAlex database (a free, open catalog of academic publications).

---

## Step 2: Searching for Articles (`search_results/`)

Using the OpenAlex API, we searched each of the 69 journals for all articles whose title or abstract mentions "Israel." The raw results were saved as one JSON file per journal inside the `search_results/` folder — roughly 72 files in total.

**Tool used:** OpenAlex API (a free, publicly available index of scholarly works).

---

## Step 3: Building the Articles Dataset (`works.csv`)

The raw search results were then converted into a single, structured spreadsheet. For each article, we extracted:
- Title, authors, publication date, DOI (digital identifier)
- The journal it appeared in and its category
- The abstract (reconstructed from OpenAlex's internal format)
- Keywords, citation count, and open-access status

This produced `works.csv` with 5,788 unique articles.

---

## Step 4: Publication Volume Statistics (`journal_stats.csv`)

To understand how often Israel is discussed relative to other countries, we queried OpenAlex for publication counts per journal per year — not just for "Israel," but also for comparison countries like Australia, France, Syria, Egypt, South Africa, Germany, Japan, and others (17 countries total). This produced `journal_stats.csv` with about 3,400 rows showing article counts broken down by journal, year, and country keyword.

**Tool used:** OpenAlex API.

---

## Step 5: Author Enrichment (`authors_works.csv`)

For every author who appeared in the Israel-related articles, we queried OpenAlex to retrieve their full publication history — not just the Israel-related articles but everything they've ever published. This allowed us to understand each author's broader academic profile: how many total works they have, which institutions they're affiliated with, which countries they're based in, and whether their other publications also reference Israel.

This produced `authors_works.csv` with about 344,000 author-work pairs.

**Tool used:** OpenAlex API.

---

## Step 6: Author Aggregation (`authors_works_aggregated_by_author.csv`, `authors_works_aggregated_by_work.csv`)

The detailed author-work pairs were then rolled up into two summary views:

- **By author** (~4,500 rows): For each author, how many total works they have, how many are Israel-specific, how many journals they publish in, their citation count, career span, institutions, and countries.
- **By work** (~327,000 rows): For each work, all of its authors' names, affiliations, institutions, and countries consolidated into a single row.

Author names were normalized (removing accents and punctuation) to merge duplicates.

---

## Step 7: Linguistic Analysis (`words.csv`, `words_bigrams.csv`, `words_trigrams.csv`, `words_graph.csv`)

Each article's title, keywords, and abstract were processed through a natural language processing (NLP) pipeline to extract vocabulary patterns:

- **`words.csv`** (~457,000 rows): Every meaningful word in each article, reduced to its base form (lemma), with its frequency, grammatical role (noun, verb, adjective, etc.), and whether it's a named entity (person, place, organization). Common function words (the, and, of, etc.) were filtered out.
- **`words_bigrams.csv`** (~538,000 rows): Every pair of consecutive words.
- **`words_trigrams.csv`** (~557,000 rows): Every triplet of consecutive words.
- **`words_graph.csv`** (~5.6 million rows): A word co-occurrence network — for every pair of words that appeared in the same sentence, we recorded how often they co-occurred and how far apart they typically were. This can be used to map which concepts tend to cluster together in the literature.

**Tool used:** Stanza (Stanford NLP Group's natural language processing library).

---

# Part II: Deep Dive on Specific Journals

This part focuses on a subset of journals where full article PDFs were obtained, enabling more detailed analysis using the complete text rather than just metadata.

---

## Step 8: Downloading Full Article Texts (PDFs)

Article PDFs were downloaded semi-manually and then converted to plain text files using a PDF text-extraction tool.

**Tools used:** pdfminer (a Python library for extracting text from PDFs).

---

## Step 9: PDF Text Analysis (`pdfs_analyze.py`)

For collections of downloaded PDFs, additional quantitative analysis was performed: counting how many times "Israel" appears in each article's full text (both overall and in the central 80% of the text, to exclude headers/footers), along with total word and line counts. Optional word clouds were also generated to visualize the most frequent terms.

**Tools used:** Python regex for counting, WordCloud library for visualization.

---

## Step 10: Sentiment & Theme Analysis Using a Local LLM (`sentiments.csv`)

Each of the ~5,750 articles was analyzed by a large language model (Llama 3, running locally via Ollama) to determine:

1. **Sentiment toward Israel**: Is the article's framing of Israel positive, negative, neutral, or unclassifiable? The model also provided a brief explanation of its reasoning.
2. **Themes**: Which themes from a predefined list of 38 are present in the article? Themes include concepts like colonialism, settler colonialism, apartheid, genocide, occupation, resistance, democracy, peace, Zionism, antisemitism, Nakba, and others.

The model analyzed each article's abstract (or keywords if no abstract was available), along with its title and authors. Results were saved as individual JSON files and then combined into `sentiments.csv`.

**Tool used:** Ollama running Meta's Llama 3 model locally.

---

## Step 11: Advanced LLM Analysis on Full Article Texts (`llm_analysis/`)

For articles where full text was available (from the downloaded PDFs), a more detailed analysis was performed using multiple large language models running on GPU servers via vLLM. Each article was analyzed by up to four different models:

- **Meta Llama 3.1 70B** (70 billion parameters) - https://huggingface.co/meta-llama/Llama-3.1-70B
- **DeepSeek-R1 70B** (70 billion parameters) - https://huggingface.co/deepseek-ai/DeepSeek-R1-Distill-Llama-70B
- **Meta Llama 4 Maverick 17B** (400 billion parameters) - https://huggingface.co/meta-llama/Llama-4-Maverick-17B-128E-Instruct-FP8

Each model received the full article text along with a structured prompt asking it to:
- Classify sentiment toward Israel (with supporting quotes and page numbers)
- Identify 1–5 themes from the same predefined list of 38
- Assign a confidence level (High / Medium / Low)

The results from all models were then parsed and consolidated into `llm_outputs.csv` and `llm_outputs.json`.

**Tools used:** vLLM (a high-performance inference engine for large language models), running on multi-GPU servers (H200 GPUs).
