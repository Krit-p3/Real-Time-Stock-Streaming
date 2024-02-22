import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    concat,
    date_format,
    from_unixtime,
    lit,
    udf,
)
from pyspark.sql.types import StringType
from transformers import pipeline

# Create a Spark session without specifying appName
spark = SparkSession.builder.getOrCreate()


def clean_news(input_table):
    df = spark.read.format("delta").table(input_table)
    df = df.select("datetime", "headline", "id", "ticker", "summary", "source")

    quote_dict = {"AAPL": "Apple", "SNOW": "Snowflake", "MSFT": "Microsoft", "GOOGL": "Alphabet", "TSLA": "Tesla"}

    def apply_dict_trans(key):
        if key in quote_dict:
            return quote_dict[key]
        else:
            return None

    df = df.dropDuplicates(["headline", "summary"])

    apply_dict = udf(apply_dict_trans, StringType())

    df = df.withColumn("Company", apply_dict(df["ticker"]))

    df = df.filter(col("Company").isNotNull())

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

    return df


def sentiment_news(df, mode, path, output_table):
    sentiment_pipeline = pipeline(model="yiyanghkust/finbert-tone")
    df = df.withColumn("text", concat(col("headline"), lit(" "), col("summary")))
    df = df.toPandas()
    news_cols = sentiment_pipeline(df.text.to_list())

    df = pd.concat([df, pd.DataFrame(news_cols)], axis=1)
    df.drop(columns="text", inplace=True)

    df = spark.createDataFrame(df)
    df = df.withColumn("date", date_format("datetime", "yyyy-MM-dd"))

    df.write.format("delta").partitionBy("date").mode(mode).option(
        "path", path
    ).saveAsTable(output_table)
