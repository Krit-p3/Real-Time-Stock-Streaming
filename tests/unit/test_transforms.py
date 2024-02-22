import os.path
import pytest
import pandas as pd
from transformers import pipeline
from pyspark.sql.functions import col, concat, lit, date_format, from_unixtime, udf
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType


@pytest.fixture()
def spark():
    spark = SparkSession.builder.getOrCreate()

    yield spark
    spark.stop()


def test_clean_news(spark):
    current_dir = os.path.dirname(os.path.abspath(__name__))
    csv_path = os.path.join(current_dir,"tests/news.csv")


    df = spark.read.csv(
       csv_path, header=True
    )
    df = df.select("datetime", "headline", "id", "ticker", "summary", "source")

    quote_dict = {
        "AAPL": "Apple",
        "GOOGL": "Google",
        "MSFT": "Microsoft",
        "TSLA": "Tesla",
    }

    def apply_dict_trans(key):
        if key in quote_dict:
            return quote_dict[key]
        else:
            return None

    df = df.dropDuplicates(["headline", "summary"])

    apply_dict = udf(apply_dict_trans, StringType())

    df = df.withColumn("Company", apply_dict(df["ticker"]))

    df = df.filter(col("company").isNotNull())

    df = df.filter(
        (
            col("headline").contains(col("Company"))
            | col("headline").contains(col("Company"))
        )
        | (
            col("summary").contains(col("ticker"))
            | col("summary").contains(col("ticker"))
        )
    )

    df = df.withColumn(
        "datetime", from_unixtime(df["datetime"], format="yyyy-MM-dd HH:mm:ss")
    )

    assert df.count() > 0
    #Assert company columns 
    assert (
        df.filter(df["Company"].isin(["Apple", "Google", "Microsoft", "Tesla"])).count()
        == df.count()
    )
    assert df.select(df["Company"]).distinct().count() == 3


def sentiment_news():
    df = test_clean_news(spark)
    sentiment_pipeline = pipeline(model="yiyanghkust/finbert-tone")
    df = df.withColumn("text", concat(col("headline"), lit(" "), col("summary")))
    df = df.toPandas()
    news_cols = sentiment_pipeline(df.text.to_list())

    df = pd.concat([df, pd.DataFrame(news_cols)], axis=1)
    df.drop(columns="text", inplace=True)

    df = spark.createDataFrame(df)
    df = df.withColumn("date", date_format("datetime", "yyyy-MM-dd"))

    expected_cols = ["date"]
    assert df.count() > 0
    assert df.columns == expected_cols
