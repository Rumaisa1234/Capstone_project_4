import json
import logging
import os
import time

import requests
from kafka import KafkaProducer

url_prefix = os.environ.get("SENSORSMOCK_URL")
KAFKA_BROKER_URL = os.environ.get("KAFKA_BOOTSTRAP_SERVER")

logger = logging.getLogger()

producer = None


def get_lux_data(room):
    data = requests.get(f"{url_prefix}/{room}").json()
    return {data["room_id"]: data["measurements"][-1]}


def send_to_producer():
    rooms = ["kitchen", "bedroom", "bathroom", "living_room"]
    for room in rooms:
        lux_data = get_lux_data(room)
        producer.send("luxmeter", value=lux_data)
        logger.info(f"Sent luxmeter data: {lux_data}")


def loop():
    while True:
        send_to_producer()
        time.sleep(60)


if __name__ == "__main__":

    logging.basicConfig(level=logging.INFO)

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER_URL,
        value_serializer=lambda x: json.dumps(x).encode("utf8"),
        api_version=(0, 10, 1),
    )

    loop()
