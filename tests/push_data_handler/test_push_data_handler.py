from fastapi.testclient import TestClient

from Capstone_project_4.push_data_handler.push_data_handler import app

client = TestClient(app)


def test_collect_moisture_mate_data_response_200():
    response = client.post(
        "/api/collect/moisturemate_data", json={"test_key": "test_value"}
    )
    assert response.status_code == 200


def test_collect_carbonsense_data_response_200():
    response = client.post(
        "/api/collect/carbonsense_data", json={"test_key": "test_value"}
    )
    assert response.status_code == 200


def test_invalid_endpoints_response_404():
    response = client.post("/api/invalid", json={"test_key": "test_value"})
    assert response.status_code == 404
