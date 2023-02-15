import json
import logging

from kafka import KafkaConsumer

logger = logging.getLogger()
consumer = None


def json_deserializer(data):
    return json.loads(data.decode("utf-8"))


def read_msgs():
    for message in consumer:
        logger.info(f"Received Moisturemate Data: {message.value}")


if __name__ == "__main__":

    consumer = KafkaConsumer(
        "moisturemate",
        bootstrap_servers=["localhost:9092"],
        value_deserializer=json_deserializer,
        auto_offset_reset="latest",
        enable_auto_commit=True,
    )
    logging.basicConfig(level=logging.INFO)
    read_msgs()
