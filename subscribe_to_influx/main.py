from quixstreams import Application
from influxdb_client import InfluxDBClient, Point, WriteOptions
from datetime import datetime, timezone
import os
import json
from datetime import datetime
import logging

# Load environment variables (useful when working locally)
from dotenv import load_dotenv
# load_dotenv(os.path.dirname(os.path.abspath(__file__))+"/.env")
load_dotenv(".env")

# Logggin env
log_level_str = os.getenv("LOG_LEVEL", "INFO").upper()
log_level = getattr(logging, log_level_str, logging.INFO)

logging.basicConfig(
    level=log_level,
    format='[%(asctime)s] [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)


# --- InfluxDB Setup ---
INFLUX_URL = os.getenv("INFLUX_URL", "http://influxdb86:8086")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN", "your_token")
INFLUX_ORG = os.getenv("INFLUX_ORG", "your_org")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "iot_data")

influx_client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
influx_write = influx_client.write_api(write_options=WriteOptions(batch_size=1, flush_interval=1))

# --- Quix Setup ---
# Config
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "172.16.2.117:9092")
KAFKA_INPUT_TOPIC = os.getenv("KAFKA_INPUT_TOPIC", "event-frames-model")

app = Application(broker_address=KAFKA_BROKER,
                loglevel="INFO",
                auto_offset_reset="earliest",
                state_dir=os.path.dirname(os.path.abspath(__file__))+"/state/",
                consumer_group="model-influxdb"
      )
input_topic = app.topic(KAFKA_INPUT_TOPIC, value_deserializer="json")


def process_event(data):
    try:
        payload = data
        timestamp_ms = payload.get("timestamp_ms", None)
        timestamp = datetime.fromtimestamp(timestamp_ms / 1000.0, tz=timezone.utc) if timestamp_ms else datetime.utcnow()
        logging.info(f"timestamp-{timestamp}")
        # logging.info(f"[📥] Got message: {data}")

        point = (
            Point(KAFKA_INPUT_TOPIC)
            # .tag("id", payload.get("id", "unknow"))

            .field("value", payload.get("value", "N/A"))


            .time(timestamp)
        )

        influx_write.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)
        logging.info(f"[✓] Wrote to InfluxDB: {point.to_line_protocol()}")

    except Exception as e:
        logging.error(f"❌ Error processing message: {e}")



# Stream
sdf = app.dataframe(input_topic)
sdf = sdf.apply(process_event)

logging.info(f"Connecting to ...{KAFKA_BROKER}")
logging.info(f"🚀 Listening to Kafka topic: {KAFKA_INPUT_TOPIC}")
app.run()
