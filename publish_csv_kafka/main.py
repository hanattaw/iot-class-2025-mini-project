# Import the Quix Streams modules for interacting with Kafka:
from quixstreams import Application
from quixstreams.models.serializers.quix import JSONSerializer, SerializationContext

# (see https://quix.io/docs/quix-streams/v2-0-latest/api-reference/quixstreams.html for more details)

# Import additional modules as needed
import pandas as pd
import random
import time
import os
import logging
import json
from datetime import datetime

# for local dev, load env vars from a .env file
from dotenv import load_dotenv
load_dotenv()

# Logggin env
log_level_str = os.getenv("LOG_LEVEL", "INFO").upper()
log_level = getattr(logging, log_level_str, logging.INFO)

logging.basicConfig(
    level=log_level,
    format='[%(asctime)s] [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

KAFKA_INPUT_TOPIC = os.getenv("KAFKA_INPUT_TOPIC", "event-frames-model")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "172.16.2.117:9092")
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "123456789")
UID = os.getenv("UID", "123456789")
DELAY_DATA_INGEST_SECOND = float(os.getenv("DELAY_DATA_INGEST_SECOND", 1))
DEMO_DATA_CSV = os.getenv("DEMO_DATA_CSV", "nyc_taxi.csv")

# Validate the config
if KAFKA_INPUT_TOPIC == "":
    raise ValueError("output (topic) environment variable is required")

# Create a Quix Application, this manages the connection to the Quix platform
app = Application(broker_address=KAFKA_BROKER,
                loglevel="INFO",
                auto_offset_reset="earliest",
                state_dir=os.path.dirname(os.path.abspath(__file__))+"/state/",
                consumer_group=CONSUMER_GROUP,
      )
# Create the producer, this is used to write data to the output topic
producer = app.get_producer()
# create a topic object for use later on
output_topic = app.topic(KAFKA_INPUT_TOPIC, value_serializer="json")

logging.info(f"Connected: KAFKA={KAFKA_BROKER}")
# Define a serializer for messages, using JSON Serializer for ease
serializer = JSONSerializer()

# Get the directory of the current script
script_dir = os.path.dirname(os.path.realpath(__file__))
# Construct the path to the CSV file
csv_file_path = os.path.join(script_dir, DEMO_DATA_CSV)


# this function loads the file and sends each row to the publisher
def read_csv_file(file_path: str):
    """
    A function to read data from a CSV file in an endless manner.
    It returns a generator with stream_id and rows
    """

    # Read the CSV file into a pandas.DataFrame, parsing 'timestamp' as datetime objects
    logging.info("CSV file loading.")
    df = pd.read_csv(file_path, parse_dates=['timestamp'])
    print("File loaded.")

    # Get the number of rows in the dataFrame for printing out later
    row_count = len(df)

    # Generate a unique ID for this data stream.
    # It will be used as a message key in Kafka
    stream_id = f"CSV_DATA_{str(random.randint(1, 100)).zfill(3)}"

    # Get the column headers as a list
    headers = df.columns.tolist()

    # Continuously loop over the data
    while True:
        # Print a message to the console for each iteration 
        print(f"Publishing {row_count} rows.")

        # Iterate over the rows and convert them to
        for _, row in df.iterrows():
            # Create a dictionary that includes both column headers and row values
            # We convert the datetime object to a string for the JSON payload
            row_data = {
                'timestamp': row['timestamp'].strftime('%Y-%m-%d %H:%M:%S'),
                'value': row['value']
            }

            # Convert the datetime object to a timestamp in milliseconds
            row_data["timestamp_ms"] = int(row['timestamp'].timestamp() * 1000)
            
            # Yield the stream ID and the row data
            yield stream_id, row_data

        print("All rows published")

        # Wait a moment before outputting more data.
        time.sleep(5) # wait for next loop


def main():
    """
    Read data from the CSV file and publish it to Kafka
    """

    # Create a pre-configured Producer object.
    producer = app.get_producer()

    with producer:
        # Iterate over the data from CSV file
        for message_key, row_data in read_csv_file(file_path=csv_file_path):
            
            # Serialize row value to bytes
            serialized_value = json.dumps(row_data).encode("utf-8")

            # publish the data to the topic
            producer.produce(
                topic=output_topic.name,
                key=message_key,
                value=serialized_value,
                timestamp=row_data['timestamp_ms'],
            )

            logging.info(f"Publish topic-{output_topic.name} data-{row_data}")
            time.sleep(DELAY_DATA_INGEST_SECOND)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Exiting.")