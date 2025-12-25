#!/usr/bin/env python3

# relay-airgradient.py
#   ./relay_airgradient.py [<influx_host>/<influx_org>/<influx_bucket>] [<airgradient_host>] [loc:<location>] [<n*period_sec>] [day:HHMM-HHMM] [off:HHMM-HHMM]
#
# Config: relay-airgradient.ini
# All arguments are optional overrides to the config file and can occur in any order.
# Host arguments are hostnames, not URLs.
# The variable INFLUX_TOKEN must be set in the environment.

import os
import sys
import time
import logging
import json
from datetime import datetime, timedelta
from configparser import ConfigParser
from urllib.request import Request, urlopen
from urllib.parse import urlencode


# Constants, set as desired
LOG_LEVEL = logging.INFO
AIRGRADIENT_TIMEOUT_SEC = 5
INFLUX_TIMEOUT_SEC = 10


# Convert Airgradient JSON to JSON with a subset of fields (renamed)
# Customize as desired
def convert_data(data: dict) -> dict:
    field_mapping = {
        "atmpCompensated":  "temperature_c",
        "noxIndex":         "nox_index",
        "pm003Count":       "pm_003_ct",
        "pm01Count":        "pm_010_ct",
        "pm02Compensated":  "pm_025_comp",
        "pm10Count":        "pm_100_ct",
        "pm50Count":        "pm_050_ct",
        "rco2":             "co2",
        "rhumCompensated":  "humidity_pct",
        "tvocIndex":        "tvoc_index",
    }

    converted = {}
    for orig_key, new_key in field_mapping.items():
        if orig_key in data:
            converted[new_key] = data[orig_key]

    return converted


# No customization needed below here
#############################################################################
UTF8_ENCODING = "utf-8"


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
logging.basicConfig(level=LOG_LEVEL, handlers=[handler])
logger = logging.getLogger(__name__)


#############################################################################
# Utility classes and functions

class Sampling:
    def __init__(
        self,
        location: str = None,
        num_samples: int = None,
        period_sec: int = None,
    ):
        self.location = location
        self.num_samples = num_samples
        self.period_sec = period_sec

    def is_complete(self) -> bool:
        return all([self.location, self.num_samples, self.period_sec])


# AirGradient server configuration and session
class AirgradientServer:
    def __init__(self, host: str):
        self.host = host
        self.headers = {"Content-Type": "application/json"}


# Call GET http://${airgradient_host}/measures/current and return resulting JSON
def get_airgradient(airgradient: AirgradientServer) -> dict:
    url = f"http://{airgradient.host}/measures/current"
    req = Request(url, headers=airgradient.headers)
    with urlopen(req, timeout=AIRGRADIENT_TIMEOUT_SEC) as response:
        data = response.read().decode(UTF8_ENCODING)
        logger.debug(f"AirGradient get measure response: {data}")
        return json.loads(data)


# Call PUT http://${airgradient_host}/config
def put_airgradient(airgradient: AirgradientServer, config: dict) -> bool:
    url = f"http://{airgradient.host}/config"
    data = json.dumps(config).encode(UTF8_ENCODING)
    req = Request(url, data=data, headers=airgradient.headers, method='PUT')
    with urlopen(req, timeout=AIRGRADIENT_TIMEOUT_SEC) as response:
        response.read()  # Success if no exception raised


# LED / Display schedule brightness configuration
class LightSchedule:
    def __init__(
        self,
        day_start: datetime.time = None,
        day_end: datetime.time = None,
        led_night_level: int = None,
        led_day_level: int = None,
        off_start: datetime.time = None,
        off_end: datetime.time = None,
        disp_night_level: int = None,
        disp_day_level: int = None,
    ):
        self.day_start = day_start
        self.day_end = day_end
        self.led_night_level = led_night_level
        self.led_day_level = led_day_level
        self.off_start = off_start
        self.off_end = off_end
        self.disp_night_level = disp_night_level
        self.disp_day_level = disp_day_level

    def __str__(self):
        # Format the light schedule as "led:LL/HHMM-HHMM/LL disp:LL/HHMM-HHMM/LL"
        led_schedule = f"{self.led_night_level}/{self.day_start.strftime('%H%M')}-{self.day_end.strftime('%H%M')}/{self.led_day_level}"
        disp_schedule = f"{self.disp_night_level}/{self.off_start.strftime('%H%M')}-{self.off_end.strftime('%H%M')}/{self.disp_day_level}"
        return f"led:{led_schedule} disp:{disp_schedule}"


def process_light_schedule(
    airgradient: AirgradientServer,
    light_schedule: LightSchedule,
):
    now = datetime.now().time()

    if light_schedule.day_start == light_schedule.day_end and \
       light_schedule.off_start == light_schedule.off_end:
        return  # No schedule defined

    # Determine desired brightness levels based on schedule
    if light_schedule.day_start < now < light_schedule.day_end:
        led_brightness = light_schedule.led_day_level
        disp_brightness = light_schedule.disp_day_level
    else:
        led_brightness = light_schedule.led_night_level
        disp_brightness = light_schedule.disp_night_level

    # Override display brightness if in off window
    if light_schedule.off_start < now < light_schedule.off_end:
        disp_brightness = 0

    # Send configuration (AirGradient ignores unchanged values)
    config = {
        "ledBarBrightness": led_brightness,
        "displayBrightness": disp_brightness,
    }
    logger.debug(f"Setting LED brightness to {led_brightness}%, "
                 f"display brightness to {disp_brightness}%")
    put_airgradient(airgradient, config)


# InfluxDB server configuration and session
class InfluxServer:
    def __init__(self, host: str, org: str, bucket: str, token: str):
        self.host = host
        self.org = org
        self.bucket = bucket
        self.headers = {"Authorization": f"Token {token}"}


# Global queue for pending Influx posts
post_queue = []
POST_QUEUE_LIMIT = 8640  # 30 days at 5-minute intervals


# Process Influx FIFO post queue, raising exception on first failure
def process_post_queue(influx: InfluxServer):
    logger.debug(f"Processing InfluxDB post queue with {len(post_queue)} items")
    while post_queue:
        post_data = post_queue[0]

        url = f"https://{influx.host}/api/v2/write?{urlencode({'bucket': influx.bucket, 'org': influx.org})}"
        data = post_data.encode(UTF8_ENCODING)
        req = Request(url, data=data, headers=influx.headers, method='POST')

        with urlopen(req, timeout=INFLUX_TIMEOUT_SEC) as response:
            status_code = response.status
            logger.debug(f"InfluxDB response status: {status_code}")

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


#############################################################################
# Main run loop

def run(
    airgradient: AirgradientServer,
    influx: InfluxServer,
    sampling: Sampling,
    light_schedule: LightSchedule,
):
    # Target time for first sample
    next_sample_time = datetime.now()

    while True:
        # Collect n samples
        samples = []
        window_start_time = next_sample_time
        for i in range(sampling.num_samples):
            # Sleep until target time for this sample
            now = datetime.now()
            sleep_time = max(0, (next_sample_time - now).total_seconds())
            logger.debug(
                f"Sleeping for {sleep_time:.2f} seconds before sample {i + 1}/{sampling.num_samples}"
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

            # Process light schedule after first sample
            if i == 0:
                try:
                    process_light_schedule(airgradient, light_schedule)
                except Exception as e:
                    logger.warning(f"Error processing light schedule: {type(e).__name__}: {e}")

            # Set target time for next sample
            next_sample_time += timedelta(seconds=sampling.period_sec)

        if samples:
            window_center_time = window_start_time + timedelta(
                seconds=sampling.period_sec * sampling.num_samples / 2
            )
            averaged_data = {}
            for key in samples[0].keys():
                values = [s[key] for s in samples if key in s]
                averaged_data[key] = round(sum(values) / len(values), 2)

            try:
                post_influx(
                    influx,
                    sampling.location,
                    window_center_time,
                    averaged_data,
                )
            except Exception as e:
                logger.warning(f"Error posting to InfluxDB: {type(e).__name__}: {e}")
        else:
            logger.warning("No samples collected; posting to InfluxDB skipped.")


#############################################################################
# Configuration

# Parse time window argument (e.g., "0800-1800") into start and end datetime.time objects
def parse_time_window(time_str: str) -> tuple:
    try:
        start_str, end_str = time_str.split("-")
        if len(start_str) != 4 or len(end_str) != 4:
            return None
        start_time = datetime.strptime(start_str, "%H%M").time()
        end_time = datetime.strptime(end_str, "%H%M").time()
        return (start_time, end_time)
    except (ValueError, AttributeError):
        return None


# Parse light schedule argument (e.g., "20/0800-1800/80") into night_level, start, end, and day_level
def parse_light_schedule(schedule_str: str) -> tuple:
    try:
        parts = schedule_str.split("/")
        if len(parts) != 3:
            return None
        night_level_str, time_window_str, day_level_str = parts

        night_level = int(night_level_str)
        day_level = int(day_level_str)

        if not (0 <= night_level <= 100 and 0 <= day_level <= 100):
            return None

        time_window = parse_time_window(time_window_str)
        if not time_window:
            return None

        start_time, end_time = time_window
        return (night_level, start_time, end_time, day_level)
    except (ValueError, AttributeError, IndexError):
        return None


# Extract / check args and call run()
def main():
    usage_str = (
        sys.argv[0]
        + " [<influx_host>/<influx_org>/<influx_bucket>] [<airgradient_host>] [loc:<location>] [<n*period_sec>] [led:LL/HHMM-HHMM/LL] [disp:LL/HHMM-HHMM/LL]"
    )

    influx_token = os.environ.get("INFLUX_TOKEN")
    if not influx_token:
        logger.error("Error: INFLUX_TOKEN environment variable not set")
        sys.exit(1)

    # Read config file
    config = ConfigParser()
    config_file = os.path.join(os.path.dirname(__file__), "relay-airgradient.ini")
    config.read(config_file)

    # Data fetch / upload - initialize from config file
    influx_host = config.get("endpoints", "influx_host", fallback=None)
    influx_org = config.get("endpoints", "influx_org", fallback=None)
    influx_bucket = config.get("endpoints", "influx_bucket", fallback=None)
    airgradient_host = config.get("endpoints", "airgradient_host", fallback=None)
    sampling = Sampling(
        location=config.get("sampling", "location", fallback=None),
        num_samples=config.getint("sampling", "num_samples", fallback=None),
        period_sec=config.getint("sampling", "period_sec", fallback=None),
    )

    # LED / display schedule - initialize from config file
    led_schedule = None
    disp_schedule = None
    light_schedule = None

    if config.has_section("light_schedule"):
        try:
            day_start = config.get("light_schedule", "day_start", fallback=None)
            day_end = config.get("light_schedule", "day_end", fallback=None)
            off_start = config.get("light_schedule", "disp_off_start", fallback=None)
            off_end = config.get("light_schedule", "disp_off_end", fallback=None)
            led_night = config.getint("light_schedule", "led_level_night", fallback=None)
            led_day = config.getint("light_schedule", "led_level_day", fallback=None)
            disp_night = config.getint("light_schedule", "disp_level_night", fallback=None)
            disp_day = config.getint("light_schedule", "disp_level_day", fallback=None)

            if all([day_start, day_end, led_night is not None, led_day is not None]):
                led_schedule = parse_light_schedule(
                    f"{led_night}/{day_start}-{day_end}/{led_day}"
                )
            if all([off_start, off_end, disp_night is not None, disp_day is not None]):
                disp_schedule = parse_light_schedule(
                    f"{disp_night}/{off_start}-{off_end}/{disp_day}"
                )
        except (ValueError, AttributeError) as e:
            logger.warning(f"Error parsing light schedule from config file: {e}")

    # Override with command-line arguments
    args = sys.argv[1:]

    for arg in args:
        if "/" in arg and ":" not in arg:
            parts = arg.split("/")
            if len(parts) == 3:
                influx_host = parts[0]
                influx_org = parts[1]
                influx_bucket = parts[2]
        elif "*" in arg:
            parts = arg.split("*")
            if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit():
                sampling.num_samples = int(parts[0])
                sampling.period_sec = int(parts[1])
        elif arg.startswith("loc:"):
            sampling.location = arg[4:]
        elif arg.startswith("led:"):
            led_schedule = parse_light_schedule(arg[4:])
        elif arg.startswith("disp:"):
            disp_schedule = parse_light_schedule(arg[5:])
        else:
            airgradient_host = arg

    if led_schedule and disp_schedule:
        light_schedule = LightSchedule(
            day_start=led_schedule[1],
            day_end=led_schedule[2],
            led_night_level=led_schedule[0],
            led_day_level=led_schedule[3],
            off_start=disp_schedule[1],
            off_end=disp_schedule[2],
            disp_night_level=disp_schedule[0],
            disp_day_level=disp_schedule[3],
        )

    if not (
        all([influx_host, influx_org, influx_bucket, airgradient_host, light_schedule])
        and sampling.is_complete()
    ):
        logger.error("Error: Missing or invalid arguments")
        logger.error("Check that relay-airgradient.ini is present in same directory as this script and/or optional command-line arguments are correct.")
        logger.error(f"Usage: {usage_str}")
        sys.exit(1)

    influx = InfluxServer(influx_host, influx_org, influx_bucket, influx_token)
    airgradient = AirgradientServer(airgradient_host)

    logger.info(
        f"Polling data from '{airgradient_host}' every {sampling.period_sec} seconds, "
        f"averaging over {sampling.num_samples} samples and posting for location "
        f"'{sampling.location}' to InfluxDB at {influx_host} (org '{influx_org}', bucket "
        f"'{influx_bucket}')."
    )
    logger.info(f"Light schedule: {light_schedule}")

    run(
        airgradient,
        influx,
        sampling,
        light_schedule,
    )


if __name__ == "__main__":
    main()
