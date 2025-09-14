# Import Quix Streams and other necessary libraries
import os
import logging
import joblib
import pandas as pd
from dotenv import load_dotenv
import json

from quixstreams import Application

from influxdb_client import InfluxDBClient, Point, WriteOptions

# For local development, load environment variables from a .env file
load_dotenv()

# --- Kafka and Log Configuration ---
log_level = getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO)
logging.basicConfig(level=log_level, format='[%(asctime)s] [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "172.16.2.117:9092")
KAFKA_INPUT_TOPIC = os.getenv("KAFKA_INPUT_TOPIC", "event-frames-model")
KAFKA_ML_TOPIC = os.getenv("KAFKA_ML_TOPIC", "taxi-demand-anomalies")
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "taxi-anomaly-detector")

INFLUX_URL = "http://172.16.2.117:8086" #os.getenv("INFLUX_URL")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
INFLUX_ORG = os.getenv("INFLUX_ORG")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET")

# Initialize InfluxDB client
try:
    influx_client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    influx_writer = influx_client.write_api(write_options=WriteOptions(batch_size=1, flush_interval=1))
    logging.info("✅ InfluxDB client initialized successfully")
except Exception as e:
    logging.error(f"❌ Failed to initialize InfluxDB client: {e}")
    exit(1)

if not all([KAFKA_BROKER, KAFKA_INPUT_TOPIC, KAFKA_ML_TOPIC]):
    raise ValueError("Missing required environment variables for Kafka.")

# --- Load the Model ---
try:
    script_dir = os.path.dirname(os.path.realpath(__file__))
    model_path = os.path.join(script_dir, "isolation_forest_model.joblib")
    if not os.path.exists(model_path):
        raise FileNotFoundError(f"Model file not found at: {model_path}")
    IF_model = joblib.load(model_path)
    logging.info("✅ Isolation Forest model loaded successfully.")
except Exception as e:
    logging.error(f"❌ Failed to load the model: {e}")
    raise

# --- Initialize Quix Application ---
app = Application(
    broker_address=KAFKA_BROKER,
    consumer_group=CONSUMER_GROUP,
    loglevel="INFO",
    state_dir=os.path.dirname(os.path.abspath(__file__)) + "/state/",
    auto_offset_reset="earliest"
)

# Define input and output topics
input_topic = app.topic(KAFKA_INPUT_TOPIC, value_deserializer="json")
output_topic = app.topic(KAFKA_ML_TOPIC, value_serializer="json")

producer = app.get_producer()

# A simple buffer to store historical data for 'Lag' and 'Rolling_Mean'
data_buffer = []
BUFFER_SIZE = 20  # You can adjust this size

def handle_message(row_data):
    try:
        # 1. รับข้อมูลจาก Kafka
        # logging.info(f"Received message: {row_data}")

        # 2. เพิ่มข้อมูลเข้า buffer และจัดการขนาด
        data_buffer.append(row_data)
        if len(data_buffer) > BUFFER_SIZE:
            data_buffer.pop(0)

        # 3. เตรียมข้อมูลสำหรับ Model
        df = pd.DataFrame(data_buffer)
        df['timestamp'] = pd.to_datetime(df['timestamp'])

        # 4. Feature Engineering
        df['Weekday'] = df['timestamp'].dt.strftime('%A')
        df['Hour'] = df['timestamp'].dt.hour
        df['Day'] = df['timestamp'].dt.weekday
        df['Month'] = df['timestamp'].dt.month
        df['Year'] = df['timestamp'].dt.year
        df['Month_day'] = df['timestamp'].dt.day
        df['Lag'] = df['value'].shift(1)
        df['Rolling_Mean'] = df['value'].rolling(window=7, min_periods=1).mean()
        # Note: For 'value_Average', you would need to store historical averages or a pre-calculated lookup table.
        # This example omits it for simplicity in real-time streaming.
        
        current_row = df.iloc[-1].to_dict()
        features_for_model = pd.DataFrame([current_row])[['value', 'Hour', 'Day', 'Month_day', 'Month', 'Rolling_Mean', 'Lag']].dropna()

        if features_for_model.empty:
            logging.warning("Not enough data in buffer to make a prediction. Skipping.")
            return

        # 5. ทำนายด้วย Model
        prediction = IF_model.predict(features_for_model)
        score = IF_model.decision_function(features_for_model)
        
        # 6. เพิ่มผลลัพธ์ลงในข้อมูล
        current_row['Outliers'] = 1.0 if prediction[0] == -1 else 0.0
        current_row['Score'] = float(score[0])
        
        # 7. Serialize the data before publishing
        # --- FIX: Convert Timestamp object to string before serialization ---
        if 'timestamp' in current_row and isinstance(current_row['timestamp'], pd.Timestamp):
            current_row['timestamp'] = current_row['timestamp'].strftime('%Y-%m-%d %H:%M:%S')

        serialized_data = json.dumps(current_row).encode("utf-8")
        
        ts = int(pd.Timestamp(current_row['timestamp']).timestamp() * 1000)
        producer.produce(
            topic=output_topic.name,
            value=serialized_data,
            timestamp=ts
        )
        logging.info(f"✅ Published to {KAFKA_ML_TOPIC} - data: {current_row}")

        # Write to InfluxDB (only fan_speed_predicted)
        # Convert timestamp string to pd.Timestamp with UTC
        ts_val = pd.to_datetime(current_row['timestamp'], utc=True)
        point = (
            Point(KAFKA_ML_TOPIC)
            
            .tag("Hour", current_row.get("Hour"))
            .tag("Day", current_row.get("Day"))
            .tag("Weekday", current_row.get("Weekday"))
            .tag("Month", current_row.get("Month"))
            .tag("Month_day", current_row.get("Month_day"))
            .tag("Year", current_row.get("Year"))
            .field("Lag", current_row.get("Lag"))
            .field("Rolling_Mean", current_row.get("Rolling_Mean"))
            .field("Outliers", current_row.get("Outliers"))
            .field("Score", current_row.get("Score"))
            .field("value", current_row.get("value"))
            .time(ts_val)
        )
        influx_writer.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)
        logging.info("[📊] Wrote prediction to InfluxDB for Abnomalie")

        point = (
            Point(KAFKA_ML_TOPIC+"_DATA")
            
            .field("Lag", current_row.get("Lag"))
            .field("Rolling_Mean", current_row.get("Rolling_Mean"))
            .field("Outliers", current_row.get("Outliers"))
            .field("Score", current_row.get("Score"))
            .field("value", current_row.get("value"))
            .time(ts_val)
        )
        influx_writer.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)
        logging.info("[📊] Wrote prediction to InfluxDB for Data")


        return current_row
    except Exception as e:
        logging.error(f"❌ Error processing message: {e}")

# Run the application
if __name__ == "__main__":
    sdf = app.dataframe(input_topic)
    sdf = sdf.apply(handle_message)

    app.run(sdf)
