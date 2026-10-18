# Blocages rencontrés et solutions

> Documenter chaque problème : symptôme, cause, solution. C'est une compétence évaluée.

| Symptôme | Cause | Solution |
|---|---|---|
| `PYTHON_VERSION_MISMATCH` dans Spark | Driver et workers n'ont pas la même version de Python | Image Airflow en Python 3.10 = Python de l'image Spark |
| Elasticsearch redémarre en boucle (Linux) | `vm.max_map_count` trop bas | `sudo sysctl -w vm.max_map_count=262144` |
| `pull access denied for minio/minio` | MinIO a retiré ses images Docker Hub (sept. 2026) | Image `cgr.dev/chainguard/minio` + création des buckets en Python (`scripts/init_buckets.py`) |
| `ResolutionImpossible` pandas au build Airflow | great_expectations 1.2.4 exige pandas<2.2 | `pandas==2.1.4` (résolution vérifiée avec apache-airflow 2.10.3) |
| `Fragment would be written into 4714 partitions` | Partitionnement HN par jour sur 15 ans d'historique | Partitionnement par année |
| `KeyError: run_id` dans les Data Docs GE | `ValidationDefinition.run()` sans Checkpoint | `gx.Checkpoint` + `UpdateDataDocsAction` |
| | | |
