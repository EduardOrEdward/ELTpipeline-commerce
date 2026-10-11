"""Tests for the MockAPI health endpoint."""

import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from mock_api.main import app


class HealthEndpointTests(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(app)

    def test_health_returns_ok(self):
        response = self.client.get("/health")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"status": "ok"})

    def test_health_bypasses_simulated_upstream_failures(self):
        with patch("mock_api.main.FAILURE_RATE", 1.0):
            response = self.client.get("/health")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"status": "ok"})

    def test_health_route_is_registered(self):
        self.assertIn("/health", app.openapi()["paths"])


if __name__ == "__main__":
    unittest.main()
