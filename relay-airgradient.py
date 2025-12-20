#!/usr/bin/env python3

# relay-airgradient.py
#   ./relay_airgradient.py <influx_host>/<influx_org>/<influx_bucket> <airgradient_host> loc:<location> <n*period_sec>
#
# Arguments can occur in any order and all are required.
# Host arguments are hostnames, not URLs.
# The variable INFLUX_TOKEN must be set in the environment.

import os
import sys
import time
import logging
from datetime import datetime, timedelta

import requests


# Timeout constants
AIRGRADIENT_TIMEOUT_SEC = 5
INFLUX_TIMEOUT_SEC = 10


# Convert Airgradient JSON to JSON with a subset of fields (renamed)
# Customize as desired
def convert_data(data: dict) -> dict:
    field_mapping = {
        "pm02Compensated": "pm_025_comp",
        "pm003Count": "pm_003_ct",
        "pm01": "pm_010",
        "pm10": "pm_100",
        "rco2": "co2",
        "atmp": "temperature_c",
        "rhum": "humidity_pct",
        "tvocIndex": "tvoc_index",
        "tvocRaw": "tvoc_raw",
        "noxIndex": "nox_index",
    }

    converted = {}
    for orig_key, new_key in field_mapping.items():
        if orig_key in data:
            converted[new_key] = data[orig_key]

    return converted


# Configure logging
class PaddedLevelFormatter(logging.Formatter):
    def format(self, record):
        # Pad the levelname to 7 characters (max length of 'WARNING')
        record.levelname = f"{record.levelname:<7}"
        return super().format(record)


handler = logging.StreamHandler()
handler.setFormatter(
    PaddedLevelFormatter(
        fmt="%(asctime)s  %(levelname)s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
)
logging.basicConfig(level=logging.INFO, handlers=[handler])
logger = logging.getLogger(__name__)


# AirGradient server configuration and session
class AirgradientServer:
    def __init__(self, host: str):
        self.host = host
        self.session = requests.Session()


# Call GET http://${airgradient_host}/measures/current and return resulting JSON
def get_airgradient(airgradient: AirgradientServer) -> dict:
    url = f"http://{airgradient.host}/measures/current"
    response = airgradient.session.get(url, timeout=AIRGRADIENT_TIMEOUT_SEC)
    response.raise_for_status()
    logger.debug(f"AirGradient response: {response.text}")
    return response.json()


# InfluxDB server configuration and session
class InfluxServer:
    def __init__(self, host: str, org: str, bucket: str, token: str):
        self.host = host
        self.org = org
        self.bucket = bucket
        self.session = requests.Session()
        self.session.headers.update({"Authorization": f"Token {token}"})


# Global queue for pending Influx posts
post_queue = []
POST_QUEUE_LIMIT = 20000


# Process Influx FIFO post queue, raising exception on first failure
def process_post_queue(influx: InfluxServer):
    logger.info(f"Processing InfluxDB post queue with {len(post_queue)} items")
    while post_queue:
        post_data = post_queue[0]

        status = influx.session.post(
            f"https://{influx.host}/api/v2/write",
            params={"bucket": influx.bucket, "org": influx.org},
            data=post_data,
            timeout=INFLUX_TIMEOUT_SEC,
        )
        logger.debug(f"InfluxDB response status: {status.status_code}")
        status.raise_for_status()

        post_queue.pop(0)


# Post metrics as measurement "airquality" tagged by location to InfluxDB's v2 API
def post_influx(
    influx: InfluxServer,
    location: str,
    timestamp: datetime,
    metrics: dict,
):
    ts = int(timestamp.timestamp())
    values = ",".join([f"{k}={v}" for k, v in metrics.items()])
    post_data = f"airquality,location={location} {values} {ts}000000000"
    logger.debug(f"\n{post_data}\n")

    if len(post_queue) == POST_QUEUE_LIMIT:
        post_queue.pop(0)
        logger.warning(
            f"Post queue exceeded limit of {POST_QUEUE_LIMIT}. dropped oldest item."
        )

        post_queue.append(post_data)
        process_post_queue(influx)


def run(
    airgradient: AirgradientServer,
    influx: InfluxServer,
    location: str,
    num_samples: int,
    period_sec: int,
):
    # Target time for first sample
    next_sample_time = datetime.now()

    while True:
        # Collect n samples
        samples = []
        window_start_time = next_sample_time
        for i in range(num_samples):
            # Sleep until target time for this sample
            now = datetime.now()
            sleep_time = max(0, (next_sample_time - now).total_seconds())
            logger.debug(
                f"Sleeping for {sleep_time:.2f} seconds before sample {i + 1}/{num_samples}"
            )
            if sleep_time > 0:
                time.sleep(sleep_time)
            elif sleep_time < 0:
                logger.warning(f"Sampling slipped by {-sleep_time:.2f}s")
                next_sample_time = now

            try:
                data = get_airgradient(airgradient)
                converted = convert_data(data)
                samples.append(converted)
            except Exception as e:
                logger.warning(f"Error collecting sample: {type(e).__name__}: {e}")

            # Set target time for next sample
            next_sample_time += timedelta(seconds=period_sec)

        # Average the samples
        if samples:
            logger.info("Averaging samples and posting to InfluxDB")
            window_center_time = window_start_time + timedelta(
                seconds=period_sec * num_samples / 2
            )
            averaged_data = {}
            for key in samples[0].keys():
                values = [s[key] for s in samples if key in s]
                averaged_data[key] = round(sum(values) / len(values), 2)

            try:
                post_influx(
                    influx,
                    location,
                    window_center_time,
                    averaged_data,
                )
            except Exception as e:
                logger.warning(f"Error posting to InfluxDB: {type(e).__name__}: {e}")
        else:
            logger.warning("No samples collected; posting to InfluxDB skipped.")


# Extract / check args and call run()
def main():
    usage_str = (
        sys.argv[0]
        + " <influx_host>/<influx_org>/<influx_bucket> <airgradient_host> loc:<location> <n*period_sec>"
    )

    if len(sys.argv) != 5:
        logger.error(f"usage: {usage_str}")
        sys.exit(1)

    influx_token = os.environ.get("INFLUX_TOKEN")
    if not influx_token:
        logger.error("Error: INFLUX_TOKEN environment variable not set")
        sys.exit(1)

    args = sys.argv[1:]
    influx_host = None
    influx_org = None
    influx_bucket = None
    airgradient_host = None
    location = None
    num_samples = None
    period_sec = None

    for arg in args:
        if "/" in arg and "*" not in arg:
            parts = arg.split("/")
            if len(parts) == 3:
                influx_host = parts[0]
                influx_org = parts[1]
                influx_bucket = parts[2]
        elif "*" in arg:
            parts = arg.split("*")
            if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit():
                num_samples = int(parts[0])
                period_sec = int(parts[1])
        elif arg.startswith("loc:"):
            location = arg[4:]
        else:
            airgradient_host = arg

    if not all(
        [
            influx_host,
            influx_org,
            influx_bucket,
            airgradient_host,
            location,
            num_samples,
            period_sec,
        ]
    ):
        logger.error(f"usage: {usage_str}")
        logger.error("Error: Missing or invalid arguments")
        sys.exit(1)

    logger.info(
        f"Polling data from '{airgradient_host}' every {period_sec} seconds, "
        f"averaging over {num_samples} samples, and posting for location "
        f"'{location}' to InfluxDB (org '{influx_org}', bucket "
        f"'{influx_bucket}')."
    )

    influx = InfluxServer(influx_host, influx_org, influx_bucket, influx_token)
    airgradient = AirgradientServer(airgradient_host)

    run(
        airgradient,
        influx,
        location,
        num_samples,
        period_sec,
    )


if __name__ == "__main__":
    main()
