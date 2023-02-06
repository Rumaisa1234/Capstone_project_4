import requests
import logging
from fastapi import FastAPI,Request,Response
from fastapi.testclient import TestClient
from Capstone_project_4.push_data_handler.push_data_handler import app

client = TestClient(app)

def test_get_luxmeter_response_200():
    try:
        response = client.get("/api/luxmeter/{room_id}")        
        assert response.status_code == 200
        assert response.json() == {"message": "Data received"}
    except AssertionError as e:
        logging.error("Test failed: {}".format(e))


def test_collect():
    response = requests.post("http://localhost:3000/api/collect")
    assert response.status_code == 200
    assert response.json() == {"msg": "ok"}
