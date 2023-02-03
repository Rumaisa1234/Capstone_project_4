import pytest
import asyncio
from fastapi.testclient import TestClient
from push_data.push_data_handler import app

client = TestClient(app)

from Capstone_project_4.push_data.push_data_handler import app

def test_collect_moisture_mate_data(request: Request):
    response = client.post("/api/collect/moisture_mate_data", json={"test_key":"test_value"},)
    assert response.status_code == 200
    assert response.json() == {"msg": "received carbonsense data"}


def test_collect_carbonsense_data(request: Request):
    response = client.post("/api/collect/carbonsense_data", json={"test_key":"test_value"},)
    assert response.status_code == 200
    assert response.json() == {"msg": "received carbonsense data"}



if __name__ == "__main__":
    app.run_app()