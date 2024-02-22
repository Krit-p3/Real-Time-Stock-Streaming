import os
import finnhub
from datetime import datetime, timedelta
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType
import pytest
from dotenv import load_dotenv
from pyspark.sql import SparkSession


@pytest.fixture()
def spark():
    spark = SparkSession.builder.getOrCreate()

    yield spark
    spark.stop()


@pytest.fixture()
def finnhub_api():
    load_dotenv()
    finnhub_api = os.getenv("FINNHUB_API")
    return finnhub_api


@pytest.mark.parametrize("tickers", ["AAPL", "MSFT", "SNOW", "GOOGL", "TSLA"])
def test_company_news(tickers, finnhub_api):
    finnhub_client = finnhub.Client(finnhub_api)
    start_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    end_date = start_date
    data = []
    for ticker in tickers:
        news = finnhub_client.company_news(ticker, _from=start_date, to=end_date)
        data.extend(news)

    assert isinstance(data, list)
    assert all(isinstance(item, dict) for item in data)
    assert all("datetime" in item for item in data)
    assert all("headline" in item for item in data)


def test_transform(spark, finnhub_api):
    schema = StructType(
        [
            StructField("category", StringType(), True),
            StructField("datetime", LongType(), True),
            StructField("headline", StringType(), True),
            StructField("id", IntegerType(), True),
            StructField("image", StringType(), True),
            StructField("related", StringType(), True),
            StructField("source", StringType(), True),
            StructField("summary", StringType(), True),
            StructField("url", StringType(), True),
        ]
    )
    #Sample data
    data = [
        {
            "category": "company",
            "datetime": 1708161600,
            "headline": "Baron Technology Fund Q4 2023 Shareholder Letter",
            "id": 125951739,
            "image": "https://static.seekingalpha.com/cdn/s3/uploads/getty_images/1177116437/image_1177116437.jpg?io=getty-c-w1536",
            "related": "S",
            "source": "SeekingAlpha",
            "summary": "During Q4 2023, Baron Technology Fund rose 18.95% (inst. shares), outperforming the benchmark index. Click here to read the full fund letter.",
            "url": "https://finnhub.io/api/news?id=1447b34b99224f66623da2ec7b079640307592f0e9541c4ad89fe2d25b9a4efc",
        }
    ]
    data = spark.createDataFrame(data, schema)
    data = data.select(col("related").alias("ticker"))
    expected_cols = ["ticker"]
    assert data.columns == expected_cols


if __name__ == "__main__":
    pytest.main(["-s"])
