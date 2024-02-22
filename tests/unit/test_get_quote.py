import os
import pytz
import json
import finnhub
import pytest
from datetime import datetime, time
from dotenv import load_dotenv


@pytest.fixture()
def finnhub_api():
    load_dotenv()
    finnhub_api = os.getenv("FINNHUB_API")
    return finnhub_api


@pytest.mark.parametrize("ticker", ["AAPL"])
def test_get_quote_data(ticker, finnhub_api):
    finnhub_client = finnhub.Client(finnhub_api)

    quote = finnhub_client.quote(ticker)
    ticker = {"ticker": f"{ticker}"}
    data = json.dumps({**ticker, **quote})

    expect_data_keys = {"ticker", "c", "d", "dp", "h", "l", "o", "pc", "t"}
    assert (
        set(json.loads(data).keys()) == expect_data_keys
    ), f"Data mismatch for ticker {ticker}. Expect keys: :{expect_data_keys}, Actual keys: {set(json.load(data).keys())}"


def test_convert_timezone():
    tz = pytz.timezone("America/New_York")

    
    expected_time_str = "09:00:00"

    #Setup America/New York timezone
    us_nine_am = tz.localize(datetime.combine(datetime.today(), time(9, 0)))

    #Convert to String
    us_nine_am_str = us_nine_am.strftime("%H:%M:%S")

    assert us_nine_am_str == expected_time_str


if __name__ == "__main__":
    pytest.main()
