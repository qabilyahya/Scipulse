# SciPulse & SciPulse Insights

> Plateforme de veille scientifique à grande échelle : **ArXiv × Hacker News × citations académiques**,
> architecture **Medallion** (MinIO), orchestration **Airflow**, qualité **Great Expectations**,
> traitement **Spark**, serving & visualisation **Elasticsearch / Kibana**.

**Objectif métier :** identifier les articles les plus significatifs, qu'ils soient influents
dans la communauté scientifique, populaires auprès du public tech, ou les deux.

---

## Architecture

```
BRONZE   ArXiv (batch) ─────────────┐
         Hacker News (micro-batch) ─┼──► MinIO  arxiv-raw/ hn-raw/ citations-raw/   (JSON Lines)
         OpenAlex (bulk) ───────────┘
SILVER   Great Expectations + nettoyage ─► MinIO  arxiv-clean/ hn-clean/ citations-clean/ (Parquet)
GOLD     Spark : jointure × 3 + score ───► Elasticsearch  arxiv-papers-enriched ──► Kibana
```

| Composant | Rôle | URL locale |
|---|---|---|
| Airflow | Orchestration (DAGs) | http://localhost:8081 |
| MinIO | Data lake Medallion | http://localhost:9001 |
| Spark | Traitement distribué | http://localhost:8080 |
| Elasticsearch | Couche Gold (serving) | http://localhost:9200 |
| Kibana | Dashboards | http://localhost:5601 |
| Logstash | Métriques pipeline → `pipeline-monitoring` | :8090 |

Détails : [docs/architecture.md](docs/architecture.md)

## Prérequis

- Docker + Docker Compose v2, **12–16 Go de RAM** alloués à Docker
- Compte Kaggle (clé API gratuite) pour le dump ArXiv
- `make`

## Démarrage

```bash
cp .env.example .env        # renseigner KAGGLE_USERNAME / KAGGLE_KEY / OPENALEX_MAILTO
make up                     # démarre toute la stack de zéro
make init-es                # crée les index ES avec leurs mappings
```

Puis dans Airflow (admin / admin), activer les DAGs dans cet ordre :

1. `arxiv_batch` — Bronze → Silver → index `arxiv-papers`
2. `hn_backfill` (une fois) puis `hn_microbatch` (toutes les 5 min)
3. `scipulse_pipeline` — HN Silver, citations, job Spark Gold → `arxiv-papers-enriched`

Importer les dashboards : Kibana → Stack Management → Saved Objects → Import → `kibana/*.ndjson`.

| Commande | Effet |
|---|---|
| `make up` / `make down` | démarrer / arrêter |
| `make reset` | tout supprimer (volumes inclus) |
| `make test` / `make lint` | tests unitaires / lint |
| `make spark-gold` | lancer le job Spark à la main |

## Structure du dépôt

```
├── dags/                    # DAGs Airflow (fins : appellent src/scipulse/tasks.py)
├── src/scipulse/
│   ├── ingestion/           # Bronze : arxiv, hn, citations
│   ├── quality/             # suites Great Expectations + runner
│   ├── cleaning/            # Silver : nettoyage + normalisation des clés de jointure
│   ├── spark/               # Gold : jointure, indicateurs, TF-IDF, score, écriture ES
│   ├── common/              # MinIO, Parquet, Spark, ES, monitoring, logging
│   └── tasks.py             # point d'entrée des tâches Airflow
├── elasticsearch/mappings/  # mappings des index
├── logstash/pipeline/       # ingestion des métriques pipeline
├── great_expectations/      # contexte GX + Data Docs
├── kibana/                  # export des dashboards (.ndjson)
├── notebooks/               # analyse qualité, pipeline Spark, validation
├── docs/                    # architecture, décisions, score, journal des blocages
└── tests/
```

## Choix techniques

| Choix | Justification |
|---|---|
| **OpenAlex** comme source de citations | Gratuit, données ouvertes, requête par DOI (`10.48550/arxiv.<id>`) par lots de 50, `counts_by_year` pour la vélocité, `referenced_works` pour le graphe (option B). *À compléter.* |
| Backfill HN via **API Algolia** | L'API Firebase ne fournit que les items récents : sans historique, la jointure HN × ArXiv serait quasi vide. |
| **Python 3.10** dans Airflow | Même version que les workers Spark (sinon `PYTHON_VERSION_MISMATCH`). |
| Sécurité ES désactivée | Environnement de développement local uniquement. |
| `BashOperator` + `spark-submit` | Même commande en local et dans Airflow, plus simple à déboguer. |
| Logstash pour le monitoring | Les tâches envoient leurs métriques (volumes, résultats GE) → dashboard de monitoring. |

Journal complet : [docs/decisions.md](docs/decisions.md) · Score d'impact : [docs/impact_score.md](docs/impact_score.md)

## Résultats

*À compléter : couverture des jointures, top articles, cas d'usage analytiques, captures Kibana (`docs/screenshots/`).*

## Auteur

Yahya Qabil — Projet #4, L'École Multimédia
