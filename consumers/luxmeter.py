import json

from kafka import KafkaConsumer


def json_deserializer(data):
    return json.loads(data.decode("utf-8"))


consumer = KafkaConsumer(
    "luxmeter",
    bootstrap_servers=["localhost:9092"],
    value_deserializer=json_deserializer,
    auto_offset_reset="latest",
    enable_auto_commit=True,
)

for message in consumer:
    print(f"Received Lux Data: {message.value}")
