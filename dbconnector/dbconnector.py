import json

from kafka import KafkaConsumer
from pymongo import MongoClient

predicted_data_consumer = None


def json_deserializer(data):
    return json.loads(data.decode("utf-8"))


def storing_records(Consumer):
    for message in Consumer:
        collection.insert_many(message.value)


if __name__ == "__main__":
    predicted_data_consumer = KafkaConsumer(
        "predicted_data",
        bootstrap_servers=["broker:29092"],
        value_deserializer=json_deserializer,
        auto_offset_reset="latest",
        enable_auto_commit=True,
    )
    client = MongoClient(
        "mongodb+srv://yamnatahir:0hxYGSdjvGf64ltW@cluster0.pv2pbjv.mongodb.net/?retryWrites=true&w=majority"
    )
    db = client["IOTSensors"]
    collection = db["DataWOccupancy"]
    storing_records(predicted_data_consumer)
