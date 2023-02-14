import datetime
import io
import json
import os
import time

import boto3
import pandas as pd
from kafka import KafkaProducer

SMART_THERMO_BUCKET = os.environ.get("SMART_THERMO_BUCKET")
KAFKA_BROKER_URL = os.environ.get("KAFKA_BOOTSTRAP_SERVER")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC")


producer=None


s3 = boto3.client(
    "s3",
    endpoint_url="http://minio:9000",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin",
)


def get_datetime():
    time_now = datetime.datetime.utcnow().replace(microsecond=0, second=0).isoformat()
    return f"{time_now}.csv"


def get_data():
    bucket = SMART_THERMO_BUCKET
    key = get_datetime()
    obj = s3.get_object(Bucket=bucket, Key=f"smart_thermo/{key}")
    res = obj["Body"].read()
    initial_df = pd.read_csv(io.BytesIO(res))
    initial_df = initial_df.iloc[:, 1:]
    return initial_df.to_dict(orient="records")

def loop():
    while True:
        data = get_data()
        producer.send(KAFKA_TOPIC, value=data)
        time.sleep(60)
        
if __name__ == "__main__":
    producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER_URL,
    value_serializer=lambda x: json.dumps(x).encode("utf8"),
    api_version=(0, 10, 1),)

    loop()