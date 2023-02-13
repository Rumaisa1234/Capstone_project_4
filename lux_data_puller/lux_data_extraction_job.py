import datetime
import json
import logging
import os
import time

import requests
from kafka import KafkaProducer

logger = logging.getLogger()

url_prefix = os.environ.get("SENSORSMOCK_URL")

KAFKA_BROKER_URL = os.environ.get("KAFKA_BOOTSTRAP_SERVER")
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER_URL,
    value_serializer=lambda x: json.dumps(x).encode("utf8"),
    api_version=(0, 10, 1),
)


def get_lux_data(room):
    seconds = 0
    while seconds < 11:
        my_date = (
            datetime.datetime.utcnow().replace(second=0, microsecond=0).isoformat()
        )
        data = requests.get(f"{url_prefix}/{room}").json()
        timestamp = data["measurements"][-1]["timestamp"]
        if my_date != timestamp:
            print("not a new value")
            time.sleep(1)
            seconds += 1
        else:
            return {data["room_id"]: data["measurements"][-1]}


def send_to_producer():
    rooms = ["kitchen", "bedroom", "bathroom", "living_room"]
    for room in rooms:
        lux_data = get_lux_data(room)
        producer.send("luxmeter", value=lux_data)
        logger.info(f"Sent luxmeter data: {lux_data}")
