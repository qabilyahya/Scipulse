# Architecture

## Flux de données

| Couche | Stockage | Format | Produit par |
|---|---|---|---|
| Bronze | `arxiv-raw/`, `hn-raw/`, `citations-raw/` | JSON Lines, brut | `src/scipulse/ingestion/` |
| Silver | `arxiv-clean/`, `hn-clean/`, `citations-clean/` | Parquet partitionné | `src/scipulse/cleaning/` + GE |
| Gold | ES `arxiv-papers` (M1), `arxiv-papers-enriched` (M2) | Index ES | `common/es_indexer.py`, `spark/gold_job.py` |

## DAGs Airflow

| DAG | Planification | Tâches |
|---|---|---|
| `arxiv_batch` | hebdo | bronze → silver (GE) → gold_partial |
| `hn_microbatch` | */5 min | poll (état : Variable `hn_last_seen_id`) |
| `hn_backfill` | manuel | backfill Algolia (posts vers arxiv.org) |
| `scipulse_pipeline` | quotidien | hn_silver → citations_bronze → citations_silver → spark_gold |

## Clés de jointure

- `arxiv_id` sans version (`2301.12345v2` → `2301.12345`) : clé principale
- DOI normalisé (minuscules, sans `https://doi.org/`) ; DOI ArXiv = `10.48550/arxiv.<id>`
- HN : id ArXiv extrait de l'URL (`abs/`, `pdf/`, `html/`)

## Monitoring

`scipulse.common.monitoring.emit()` → Logstash (HTTP) → index `pipeline-monitoring` :
volumes par couche, taux de succès GE, couverture de jointure, horodatage (fraîcheur).
