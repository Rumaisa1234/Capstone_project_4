import requests
import json
import time
import os
import logging
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')

def get_lux_data():
    url_1 = f"http://sensorsmock:3000/api/luxmeter/kitchen"
    url_2 = f"http://sensorsmock:3000/api/luxmeter/bedroom"
    url_3 = f"http://sensorsmock:3000/api/luxmeter/bathroom"
    url_4 = f"http://sensorsmock:3000/api/luxmeter/living_room"
    x_1 = requests.get(url_1)
    x_2 = requests.get(url_2)
    x_3 = requests.get(url_3)
    x_4 = requests.get(url_4)
    return x_1.text, x_2.text, x_3.text, x_4.text


KAFKA_BROKER_URL = os.environ.get("KAFKA_BOOTSTRAP_SERVER")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER_URL,
    value_serializer=lambda x: json.dumps(x).encode('utf8'),
    api_version=(0, 10, 1)
)

while True:
    lux_data = get_lux_data()
    try:
        record = producer.send("luxmeter", lux_data)
        logging.info(f"Sent luxmeter data: {lux_data}")
    except Exception as e:
        logging.error(f"Failed to send luxmeter data: {e}")
    time.sleep(60)
