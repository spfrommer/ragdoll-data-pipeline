This repository contains the dataset collection pipeline for the paper "Ranking Manipulation for Conversational Search Engines." This is distinct from our [main experimental repository](https://github.com/spfrommer/ranking_manipulation).

The `dataset` directory contains the collected URLs for each product category (`latest.csv`) along with URLs from various stages of processing.

The final RAGDOLL dataset used in our experiments is a curated subset with exactly 8 pages per product category. Please find this subset in the [main experimental repository](https://github.com/spfrommer/ranking_manipulation). This repository strictly pertains to the data collection pipeline and the complete set of product URLs we have indexed. 


Required packages:
```
click pandas torch requests bs4 lxml unidecode selenium openai cdx_toolkit
```

To query GPT-4-Turbo to collect a set of brands and products, run
```
python find_sites.py --model "gpt-4-turbo"
```

To clean the dataset, run
```
python clean_sites.py --max_workers <CPU_THREADS_TO_USE>
```

Website HTMLs, Google CSE search responses, and OpenAI LLM responses will be cached.

For all categories, dataset v1.0 is the GPT-3.5-Turbo raw and v2.0 is the GPT-4-Turbo raw. \
The latest processed dataset will be shown as `dataset/<CATEGOTY>/latest.csv`.
