from unittest import TestCase

from fastapi.testclient import TestClient

from Capstone_project_4.push_data_handler.push_data_handler import app

client = TestClient(app)


class TestLog(TestCase):
    def test_collect_moisture_mate_data_response_200_and_logs(self):
        with self.assertLogs() as captured:
            response = client.post(
                "/api/collect/moisturemate_data", json={"test_key": "test_value"}
            )
            assert response.status_code == 200
            # check that there is only one message
            self.assertEqual(len(captured.records), 1)
            # check that the correct mesage is logged
            self.assertEqual(
                captured.records[0].getMessage(),
                "Received MoistureMate Data: {'test_key': 'test_value'}",
            )

    def test_collect_carbonsense_data_response_200_and_logs(self):
        with self.assertLogs() as captured:
            response = client.post(
                "/api/collect/carbonsense_data",
                json={"test_key": "test_value"},
            )
        assert response.status_code == 200
        # check that there is only one message
        self.assertEqual(len(captured.records), 1)
        # check that the correct mesage is logged
        self.assertEqual(
            captured.records[0].getMessage(),
            "Received CarbonSense Data: {'test_key': 'test_value'}",
        )

    def test_invalid_endpoints_response_404(self):
        response = client.post(
            "/api/invalid",
            json={"test_key": "test_value"},
        )
        assert response.status_code == 404
