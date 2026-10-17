# Blocages rencontrés et solutions

> Documenter chaque problème : symptôme, cause, solution. C'est une compétence évaluée.

| Symptôme | Cause | Solution |
|---|---|---|
| `PYTHON_VERSION_MISMATCH` dans Spark | Driver et workers n'ont pas la même version de Python | Image Airflow en Python 3.10 = Python de l'image Spark |
| Elasticsearch redémarre en boucle (Linux) | `vm.max_map_count` trop bas | `sudo sysctl -w vm.max_map_count=262144` |
| | | |
