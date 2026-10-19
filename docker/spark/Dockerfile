# Image Spark officielle + dépendances Python des workers (numpy requis par pyspark.ml / UDF)
FROM apache/spark:3.5.3-scala2.12-java17-python3-ubuntu

USER root
RUN (command -v pip3 >/dev/null || (apt-get update && apt-get install -y --no-install-recommends python3-pip \
      && rm -rf /var/lib/apt/lists/*)) \
 && pip3 install --no-cache-dir numpy==1.26.4 pandas==2.1.4 pyarrow==17.0.0
USER spark
