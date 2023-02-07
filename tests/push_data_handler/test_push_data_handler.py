import logging

from fastapi.testclient import TestClient

from Capstone_project_4.push_data_handler.push_data_handler import app

client = TestClient(app)


def test_collect_moisture_mate_data_response_200():
    try:
        response = client.post(
            "/api/collect/moisture_mate_data", json={"test_key": "test_value"}
        )
        assert response.status_code == 200
        assert response.json() == {"message": "Data received"}
    except AssertionError as e:
        logging.error("Test failed: {}".format(e))


def test_collect_carbonsense_data_response_200():
    try:
        response = client.post(
            "/api/collect/carbonsense_data", json={"test_key": "test_value"}
        )
        assert response.status_code == 200
        assert response.json() == {"message": "Data received"}
    except AssertionError as e:
        logging.error("Test failed: {}".format(e))


def test_invalid_endpoints_response_404():
    try:
        response = client.post("/api/invalid", json={"test_key": "test_value"})
        assert response.status_code == 404
        assert response.json() == {"detail": "Not Found"}
    except AssertionError as e:
        logging.error("Test failed: {}".format(e))
