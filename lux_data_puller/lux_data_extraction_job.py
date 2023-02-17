import json
import logging
import os
import time
from datetime import datetime

import requests
from apscheduler.schedulers.blocking import BlockingScheduler
from kafka import KafkaProducer

url_prefix = os.environ.get("SENSORSMOCK_URL")
KAFKA_BROKER_URL = os.environ.get("KAFKA_BOOTSTRAP_SERVER")

logger = logging.getLogger()

producer = None


def get_lux_data(room):
    retries = 0
    while retries < 11:
        my_date = datetime.utcnow().replace(second=0, microsecond=0).isoformat()
        count = 0
        while count < 11:
            try:
                data = requests.get(f"{url_prefix}/{room}").json()
                count += 1
                break
            except:
                time.sleep(1)
                continue
        timestamp = data["measurements"][-1]["timestamp"]
        if my_date == timestamp:
            return {data["room_id"]: data["measurements"][-1]}
        else:
            logger.info(f"At this timestamp {timestamp} no new value is received")
            time.sleep(1)
            retries += 1


def send_to_producer():
    rooms = ["kitchen", "bedroom", "bathroom", "living_room"]
    for room in rooms:
        lux_data = get_lux_data(room)
        producer.send("luxmeter", value=lux_data)
        logger.info(f"Sent luxmeter data: {lux_data}")


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
