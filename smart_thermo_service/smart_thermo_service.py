import io
import json
import logging
import os
import time
from datetime import datetime

import boto3
import pandas as pd
from apscheduler.schedulers.blocking import BlockingScheduler
from kafka import KafkaProducer

SMART_THERMO_BUCKET = os.environ.get("SMART_THERMO_BUCKET")
KAFKA_BROKER_URL = os.environ.get("KAFKA_BOOTSTRAP_SERVER")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC")

logger = logging.getLogger()

producer = None


s3 = boto3.client(
    "s3",
    endpoint_url="http://minio:9000",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin",
)


def get_datetime():
    time_now = datetime.utcnow().replace(microsecond=0, second=0).isoformat()
    return f"{time_now}.csv"


def get_data():
    bucket = SMART_THERMO_BUCKET
    key = get_datetime()
    retries = 0
    while retries < 11:
        try:
            obj = s3.get_object(Bucket=bucket, Key=f"smart_thermo/{key}")
            res = obj["Body"].read()
            initial_df = pd.read_csv(io.BytesIO(res))
            initial_df = initial_df.iloc[:, 1:]
            return initial_df.to_dict(orient="records")
        except Exception:
            logger.info(
                f"File smart_thermo/{key} not present in the bucket yet, retrying in 1 second."
            )
            time.sleep(1)
            retries += 1


def send_to_producer():
    data = get_data()
    producer.send("smart_thermo", value=data)


def scheduling():
    scheduler = BlockingScheduler()
    scheduler.add_job(
        send_to_producer, "cron", second="0", next_run_time=datetime.now()
    )
    scheduler.start()


if __name__ == "__main__":

    logging.basicConfig(level=logging.INFO)

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER_URL,
        value_serializer=lambda x: json.dumps(x).encode("utf8"),
        api_version=(0, 10, 1),
    )

    scheduling()
