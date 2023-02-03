import json
import logging
import os

import uvicorn
from fastapi import FastAPI, Request
from kafka import KafkaProducer

# Kafka settings
KAFKA_BROKER_URL = os.environ.get("KAFKA_BOOTSTRAP_SERVER")

Producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER_URL,
    value_serializer=lambda x: json.dumps(x).encode("utf8"),
    api_version=(0, 10, 1),
)


app = FastAPI()


@app.post("/api/collect/moisturemate_data")
async def collect_moisture_mate_data(request: Request):
    received_data = await request.json()
    print(f"Received MoistureMate Data: {received_data}")
    record = Producer.send("moisturemate", value=received_data)
    print(record)


@app.post("/api/collect/carbonsense_data")
async def collect_carbon_sense_data(request: Request):
    received_data = await request.json()
    print(f"Received CarbonSense Data: {received_data}")
    record = Producer.send("carbonsense", value=received_data)
    print(record)


def run_app():
    logging.basicConfig(level=logging.INFO)
    uvicorn.run(app, host="0.0.0.0", port=3001)


if __name__ == "__main__":
    run_app()
