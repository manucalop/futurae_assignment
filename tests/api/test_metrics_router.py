class TestAggregateMetrics:
    def test_aggregate_all(self, client):
        resp = client.get("/metrics/aggregate")
        assert resp.status_code == 200
        data = resp.json()
        assert data["request_count"] == 3
        assert data["service"] is None

    def test_aggregate_by_service(self, client):
        resp = client.get("/metrics/aggregate", params={"service": "auth"})
        data = resp.json()
        assert data["request_count"] == 2
        assert data["service"] == "auth"
        assert data["avg_latency_ms"] == 150.0
        assert data["error_rate"] == 0.5

    def test_aggregate_with_time_range(self, client):
        resp = client.get(
            "/metrics/aggregate",
            params={
                "service": "auth",
                "start_ts": "2025-01-15T10:00:00Z",
                "end_ts": "2025-01-15T10:03:00Z",
            },
        )
        data = resp.json()
        assert data["request_count"] == 1
        assert data["avg_latency_ms"] == 100.0
        assert data["error_rate"] == 0.0
