# Python 3.10 pour correspondre au Python de l'image Spark (driver = workers)
FROM apache/airflow:2.10.3-python3.10

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl \
 && apt-get clean && rm -rf /var/lib/apt/lists/*
# Lien stable : fonctionne sur amd64 et arm64 (Mac M1/M2)
RUN ln -s /usr/lib/jvm/java-17-openjdk-* /usr/lib/jvm/java-17
ENV JAVA_HOME=/usr/lib/jvm/java-17

USER airflow
COPY docker/airflow/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Dépendances des notebooks (couche séparée : un ajout ici ne réinstalle pas pyspark)
COPY docker/airflow/requirements-notebooks.txt /tmp/requirements-notebooks.txt
RUN pip install --no-cache-dir -r /tmp/requirements-notebooks.txt
