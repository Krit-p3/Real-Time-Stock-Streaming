from datetime import datetime, timedelta
import finnhub
from databricks.sdk.runtime import * #noqa


def company_news(tickers, finnhub_api):
    finnhub_client = finnhub.Client(api_key=finnhub_api)
    start_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    end_date = start_date
    data = []
    for ticker in tickers:
        news = finnhub_client.company_news(ticker, _from=start_date, to=end_date)
        data.extend(news)
    return data


def transform(data):
    df = spark.createDataFrame(data) #noqa
    df = df.withColumnRenamed("related", "ticker")
    return df


def get_news(tickers, path):
    finnhub_api = dbutils.secrets.get(scope="from-cli", key="FINNHUB_API") #noqa
    data = company_news(tickers, finnhub_api)
    df = transform(data)
    df.write.format("delta").mode("append").save(path)
