"""Lecture/écriture Parquet sur MinIO (couche Silver) avec pyarrow."""
from __future__ import annotations

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import fs

from scipulse.config import settings


def _s3fs() -> fs.S3FileSystem:
    scheme, host = settings.minio_endpoint.split("://")
    return fs.S3FileSystem(
        access_key=settings.minio_user,
        secret_key=settings.minio_password,
        endpoint_override=host,
        scheme=scheme,
    )


def write_parquet(df: pd.DataFrame, bucket: str, path: str, partition_cols: list[str] | None = None) -> None:
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_to_dataset(
        table,
        root_path=f"{bucket}/{path}".rstrip("/"),
        filesystem=_s3fs(),
        partition_cols=partition_cols,
        existing_data_behavior="overwrite_or_ignore",
    )


def read_parquet(bucket: str, path: str = "", columns: list[str] | None = None) -> pd.DataFrame:
    return pq.read_table(f"{bucket}/{path}".rstrip("/"), filesystem=_s3fs(), columns=columns).to_pandas()
