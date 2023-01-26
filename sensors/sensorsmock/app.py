import logging

import uvicorn
from fastapi import FastAPI, Request
from sensorsmock.service import SensorService

app = FastAPI()

sensor_service = SensorService()


@app.get("/api/luxmeter/{room_id}")
def get_luxmeter(room_id: str):
    if not sensor_service.is_allowed_room(room_id):
        return {"error": f"Room {room_id} not exists!"}

    data = sensor_service.get_lux_meter_data(room_id)

    return data


@app.post("/api/carbonsense")
async def collect_carbon_sense_data(request: Request):
    received_data = await request.json()
    print(f"Received Carbonsense Data: {received_data}")
    return {"msg": f"received moisture_mate data: {received_data}"}


@app.post("/api/moisturemate")
async def collect_moisture_mate_data(request: Request):
    received_data = await request.json()
    print(f"Received MoistureMate Data: {received_data}")
    return {"msg": f"received moisture_mate data: {received_data}"}


@app.post("/api/collect")
def collect():
    return {"msg": "ok"}


@app.on_event("startup")
async def startup():
    await sensor_service.start()


def run_app():
    logging.basicConfig(level=logging.INFO)
    uvicorn.run(app, host="0.0.0.0", port=3000)


if __name__ == "__main__":
    run_app()
