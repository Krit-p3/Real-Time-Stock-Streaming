#!/bin/bash

# Set the timezone to America/New_York
export TZ="America/New_York"

# Write out current crontab
crontab -l > mycron

# Echo new cron into cron file
echo "30 9 * * * cd /src/streaming_quote && poetry run python -m get_quote -t AAPL MSFT SNOW GOOGL TSLA >> /src/log_file.log 2>&1 && echo 'Python script is running'" >> mycron
echo "0 13 * * * cd /src/streaming_quote && poetry run python -m get_quote -t AAPL MSFT SNOW GOOGL TSLA >> /src/log_file.log 2>&1 && echo 'Python script is running'" >> mycron

# Install new cron file
crontab mycron

# Remove temporary cron file
rm mycron
