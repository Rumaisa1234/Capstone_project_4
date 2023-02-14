import os
import logging
import uvicorn
from fastapi import Request, FastAPI
from kafka import KafkaProducer

import json
# Kafka settings
KAFKA_BROKER_URL = os.environ.get("KAFKA_BOOTSTRAP_SERVER")
producer=None
logger=None


app = FastAPI()
@app.post("/api/collect/moisturemate_data")
async def collect_moisture_mate_data(request: Request):
    received_data = await request.json()
    logger.info(f"Received MoistureMate Data: {received_data}")
    record = producer.send('moisturemate', value=received_data)    
    logger.info('Producer has not sent your message')

@app.post("/api/collect/carbonsense_data")
async def collect_carbon_sense_data(request: Request):
    received_data = await request.json()
    logger.info(f"Received CarbonSense Data: {received_data}")
    record = producer.send('carbonsense', value=received_data)    
    logger.info('Producer has not sent your message')

def run_app():
    logging.basicConfig(level=logging.INFO)
    uvicorn.run(app, host="0.0.0.0", port=3001)


if __name__ == "__main__":
    producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER_URL,
            value_serializer=lambda x: json.dumps(x).encode('utf8'),
            api_version=(0, 10, 1)
        )
    logger=logging.getLogger()
    run_app()

