"""Création d'une SparkSession configurée pour MinIO (s3a) et Elasticsearch."""
import shutil
import socket
from pathlib import Path

from pyspark.sql import SparkSession

from scipulse.config import settings

ES_VERSION = "8.15.3"
PACKAGES = ",".join([
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    f"org.elasticsearch:elasticsearch-spark-30_2.12:{ES_VERSION}",
    # Option B : "graphframes:graphframes:0.8.3-spark3.5-s_2.12",
])


def get_spark(app_name: str = "scipulse") -> SparkSession:
    builder = SparkSession.builder.appName(app_name).master(settings.spark_master)
    if not settings.spark_master.startswith("local"):
        # Les executors doivent pouvoir joindre le driver (conteneur Airflow) par IP
        builder = builder.config("spark.driver.host", socket.gethostbyname(socket.gethostname()))
    spark = (
        builder
        .config("spark.jars.packages", PACKAGES)
        # MinIO via s3a
        .config("spark.hadoop.fs.s3a.endpoint", settings.minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", settings.minio_user)
        .config("spark.hadoop.fs.s3a.secret.key", settings.minio_password)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # Elasticsearch
        .config("es.nodes", settings.es_host.replace("http://", "").split(":")[0])
        .config("es.port", "9200")
        .config("es.nodes.wan.only", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    _ship_package(spark)
    return spark


def _ship_package(spark: SparkSession) -> None:
    """Envoie le package scipulse aux executors (nécessaire pour les UDF)."""
    pkg = Path(__file__).resolve().parents[1]          # .../src/scipulse
    archive = shutil.make_archive("/tmp/scipulse", "zip", root_dir=pkg.parent, base_dir="scipulse")
    spark.sparkContext.addPyFile(archive)


def s3a(bucket: str, path: str = "") -> str:
    return f"s3a://{bucket}/{path}".rstrip("/")
