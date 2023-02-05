import pytest
from fastapi.testclient import TestClient
from Capstone_project_4.sensors.sensormock.app import app
def test_get_luxmeter():
    client = TestClient(app)
    response = client.get("/api/luxmeter/room_1")
    assert response.status_code == 200
    assert response.json() == { "lux": 123 }

def test_get_luxmeter_invalid_room():
    client = TestClient(app)
    response = client.get("/api/luxmeter/invalid_room")
    assert response.status_code == 200
    assert response.json() == { "error": "Room invalid_room not exists!" }

def test_post_collect():
    client = TestClient(app)
    response = client.post("/api/collect")
    assert response.status_code == 200
    assert response.json() == { "msg": "ok" }
