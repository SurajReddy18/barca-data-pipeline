# Barça Data Pipeline

An end-to-end data engineering project on FC Barcelona match data, built stage by stage.

| Stage | What | Tools | Status |
|---|---|---|---|
| 1 | Ingest raw match + event data | Python, requests, Git | ✅ |
| 2 | Store raw files in the cloud | Azure Data Lake Storage | ⬜ |
| 3 | Model fact/dimension tables | SQL, data modeling | ⬜ |
| 4 | Transform at scale | PySpark, Databricks | ⬜ |
| 5 | Orchestrate on a schedule | Airflow / Azure Data Factory | ⬜ |
| 6 | Dashboard | Power BI / Streamlit | ⬜ |
| 7 | Ask questions in plain English | LLM + RAG | ⬜ |

**Data source:** [StatsBomb Open Data](https://github.com/statsbomb/open-data) (free, historical La Liga seasons 2004/05–2020/21).

## Run it

```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt

python src/ingest.py --season 2020/2021              # matches + events (~130 MB)
python src/ingest.py --season 2010/2011 --no-events  # matches only, fast
```

Output lands in a partitioned raw layer:

```
data/raw/season=2020_2021/ingest_date=2026-10-11/
├── _manifest.json   # what this run produced
├── matches.json     # Barça's matches that season
└── events/          # every pass, shot, tackle... one file per match
```

## Stage 1 concepts

- **Raw (bronze) layer**: store data exactly as the source sent it; clean later.
- **Partitioning**: folders by season and ingest date, so reruns never overwrite history.
- **Retries with backoff**: networks fail; pipelines shouldn't.
- **Manifest file**: metadata that tells downstream stages what arrived.
