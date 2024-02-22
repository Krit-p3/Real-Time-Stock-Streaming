import asyncio
import json
import os
from dotenv import load_dotenv
import finnhub
from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient
import logging
import argparse
from datetime import datetime, time
import sys
import pytz


load_dotenv()

EVENT_HUB_CONNECTION_STR = os.getenv("EVENTHUB_CONNECTION_STRING")
EVENTHUBS_NAME = os.getenv("EVENTHUBS_NAME")

# Set up logging
logging.basicConfig(filename="quote_data.log", level=logging.ERROR)


def get_quote_data(ticker):
    finnhub_client = finnhub.Client(os.getenv("FINNHUB_API"))

    try:
        quote = finnhub_client.quote(ticker)
        ticker = {"ticker": f"{ticker}"}
        data = json.dumps({**ticker, **quote})
        print(f"Sending... {data}")
        return data

    except Exception as e:
        logging.error(f"Error retrieving quote for {ticker}: {e}")
        return None


async def run(tickers, interval=60):
    # Define the timezone for America/New_York
    tz = pytz.timezone("America/New_York")

    while True:
        current_time = datetime.now(tz).replace(microsecond=0)

        # Define the start and end times in Eastern Time
        morning_start_time = tz.localize(
            datetime.combine(datetime.today(), time(9, 30))
        )
        morning_end_time = tz.localize(datetime.combine(datetime.today(), time(12, 0)))
        afternoon_start_time = tz.localize(
            datetime.combine(datetime.today(), time(13, 0))
        )
        afternoon_end_time = tz.localize(
            datetime.combine(datetime.today(), time(17, 0))
        )

        # check if the current time in the specified time ranges
        if (morning_start_time <= current_time <= morning_end_time) or (
            afternoon_start_time <= current_time <= afternoon_end_time
        ):
            async with EventHubProducerClient.from_connection_string(
                conn_str=EVENT_HUB_CONNECTION_STR, eventhub_name=EVENTHUBS_NAME
            ) as producer:
                event_data_batch = await producer.create_batch()

                for data in tickers:
                    event = get_quote_data(data)
                    event_data_batch.add(EventData(event))
                await producer.send_batch(event_data_batch)

        else:
            raise RuntimeError("Script is outside of the specified time ranges")

        await asyncio.sleep(interval)


async def stop():
    tasks = asyncio.all_tasks()
    for task in tasks:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run an Event Hubs producer to send stock data"
    )
    parser.add_argument(
        "--tickers",
        "-t",
        nargs="+",
        required=True,
        help="Specify the stock name for retrieving data",
    )
    parser.add_argument(
        "--interval",
        "-i",
        type=int,
        default=60,
        help="Specify the time interval (in seconds) ",
    )

    args = parser.parse_args()

    try:
        asyncio.run(run(args.tickers, args.interval))
    except KeyboardInterrupt:
        print("Received keyboard interrupt, stopping...")
        asyncio.run(stop())
        sys.exit(0)
    except RuntimeError as e:
        print(e)
        asyncio.run(stop())
        sys.exit(0)
