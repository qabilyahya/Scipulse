"""TF-IDF distribué (Spark MLlib) sur les abstracts ArXiv."""
from __future__ import annotations

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import IDF, CountVectorizer, RegexTokenizer, StopWordsRemover
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_tfidf_pipeline(vocab_size: int = 20_000, min_df: int = 5) -> Pipeline:
    return Pipeline(stages=[
        RegexTokenizer(inputCol="abstract", outputCol="tokens", pattern=r"[^a-zA-Z]+", minTokenLength=3),
        StopWordsRemover(inputCol="tokens", outputCol="filtered"),
        CountVectorizer(inputCol="filtered", outputCol="tf", vocabSize=vocab_size, minDF=min_df),
        IDF(inputCol="tf", outputCol="tfidf"),
    ])


def fit_tfidf(df: DataFrame) -> tuple[PipelineModel, DataFrame]:
    model = build_tfidf_pipeline().fit(df)
    return model, model.transform(df)


def add_top_terms(df: DataFrame, model: PipelineModel, k: int = 8) -> DataFrame:
    """Ajoute les k termes au poids TF-IDF le plus élevé par article (exploitable dans Kibana)."""
    vocab = model.stages[2].vocabulary

    @F.udf("array<string>")
    def top_terms(vec):
        if vec is None:
            return []
        pairs = sorted(zip(vec.indices, vec.values, strict=True), key=lambda p: -p[1])[:k]
        return [vocab[int(i)] for i, _ in pairs]

    return df.withColumn("top_terms", top_terms("tfidf"))
