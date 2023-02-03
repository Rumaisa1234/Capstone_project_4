import json
import os
import time

import requests
from kafka import KafkaProducer


def get_lux_data():
    url_1 = "http://sensorsmock:3000/api/luxmeter/kitchen"
    url_2 = "http://sensorsmock:3000/api/luxmeter/bedroom"
    url_3 = "http://sensorsmock:3000/api/luxmeter/bathroom"
    url_4 = "http://sensorsmock:3000/api/luxmeter/living_room"
    x_1 = requests.get(url_1)
    x_2 = requests.get(url_2)
    x_3 = requests.get(url_3)
    x_4 = requests.get(url_4)
    return x_1.text, x_2.text, x_3.text, x_4.text


def json_serializer(data):
    return json.dumps(data).encode("utf-8")


KAFKA_BROKER_URL = os.environ.get("KAFKA_BOOTSTRAP_SERVER")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER_URL,
    value_serializer=lambda x: json.dumps(x).encode("utf8"),
    api_version=(0, 10, 1),
)

while True:
    lux_data = get_lux_data()
    record = producer.send("luxmeter", lux_data)
    print(record)
    time.sleep(900)
