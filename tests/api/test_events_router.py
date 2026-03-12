class TestListEvents:
    def test_list_all(self, client):
        resp = client.get("/events")
        assert resp.status_code == 200
        assert len(resp.json()["data"]) == 3

    def test_filter_by_service(self, client):
        resp = client.get("/events", params={"service": "auth"})
        data = resp.json()["data"]
        assert len(data) == 2
        assert all(e["service"] == "auth" for e in data)

    def test_filter_by_event_type(self, client):
        resp = client.get("/events", params={"event_type": "request_failed"})
        data = resp.json()["data"]
        assert len(data) == 1
        assert data[0]["service"] == "catalog"

    def test_filter_by_time_range(self, client):
        resp = client.get(
            "/events",
            params={
                "start_ts": "2025-01-15T10:00:00Z",
                "end_ts": "2025-01-15T10:30:00Z",
            },
        )
        data = resp.json()["data"]
        assert len(data) == 2
        assert {e["event_id"] for e in data} == {"e1", "e2"}


class TestGetEvent:
    def test_existing_event(self, client):
        resp = client.get("/events/e1")
        assert resp.status_code == 200
        assert resp.json()["event_id"] == "e1"

    def test_nonexistent_event(self, client):
        resp = client.get("/events/nonexistent")
        assert resp.status_code == 200
        assert resp.json() is None
