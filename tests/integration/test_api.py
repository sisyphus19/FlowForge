import pytest
from fastapi.testclient import TestClient

from app.main import create_app
from tests.conftest import wait_for

YAML = """
name: api_flow
concurrency: {max_runs: 1, on_overlap: skip}
tasks:
  - {id: a, type: echo, config: {value: 1}}
  - {id: b, type: echo, depends_on: [a]}
"""


@pytest.fixture
def client(settings, store, history, start_worker):
    app = create_app(settings, store, history)
    start_worker()
    with TestClient(app) as c:
        yield c


def done(client, run_id):
    return wait_for(lambda: (r := client.get(f"/runs/{run_id}").json())["state"] != "running" and r, msg="run done")


def test_health_and_unknown_resources(client):
    h = client.get("/health").json()
    assert h["status"] == "ok" and "http_fetch" in h["task_types"]
    assert client.get("/runs/nope").status_code == 404
    assert client.post("/workflows/nope/runs").status_code == 404


def test_create_trigger_and_inspect(client):
    assert client.post("/workflows/yaml", content=YAML).status_code == 201
    r = client.post("/workflows/api_flow/runs", json={"params": {"x": 1}})
    assert r.status_code == 201
    run = done(client, r.json()["run_id"])
    assert run["state"] == "succeeded" and run["tasks"]["b"]["state"] == "succeeded"
    events = client.get(f"/runs/{run['id']}/events").json()
    assert events[0]["type"] == "run_created"
    assert client.get("/runs", params={"workflow": "api_flow"}).json()[0]["id"] == run["id"]


def test_validation_errors(client):
    assert client.post("/workflows/yaml", content="name: x\ntasks: []").status_code == 422
    bad = {"name": "w", "tasks": [{"id": "a", "type": "rm_rf"}]}
    r = client.post("/workflows", json=bad)
    assert r.status_code == 422 and "unknown task type" in r.json()["detail"]
    cyc = {"name": "w", "tasks": [{"id": "a", "type": "noop", "depends_on": ["b"]},
                                  {"id": "b", "type": "noop", "depends_on": ["a"]}]}
    assert client.post("/workflows", json=cyc).status_code == 422


def test_idempotency_key_header(client):
    client.post("/workflows/yaml", content=YAML)
    h = {"Idempotency-Key": "abc"}
    r1 = client.post("/workflows/api_flow/runs", headers=h)
    r2 = client.post("/workflows/api_flow/runs", headers=h)
    assert (r1.status_code, r2.status_code) == (201, 200)
    assert r1.json()["run_id"] == r2.json()["run_id"] and r2.json()["created"] is False


def test_overlap_returns_409_then_allows_after_finish(client):
    wf = {"name": "slow", "concurrency": {"max_runs": 1, "on_overlap": "skip"},
          "tasks": [{"id": "a", "type": "sleep", "config": {"seconds": 0.5}}]}
    client.post("/workflows", json=wf)
    first = client.post("/workflows/slow/runs")
    assert first.status_code == 201
    assert client.post("/workflows/slow/runs").status_code == 409
    done(client, first.json()["run_id"])
    assert client.post("/workflows/slow/runs").status_code == 201


def test_cancel_endpoint(client):
    client.post("/workflows", json={"name": "long", "tasks": [{"id": "a", "type": "sleep", "config": {"seconds": 30}}]})
    rid = client.post("/workflows/long/runs").json()["run_id"]
    assert client.post(f"/runs/{rid}/cancel").status_code == 200
    assert done(client, rid)["state"] == "cancelled"
    assert client.post(f"/runs/{rid}/cancel").status_code == 409


def test_dead_letters_and_replay_endpoints(client):
    wf = {"name": "df", "tasks": [{"id": "a", "type": "flaky", "config": {"fail_attempts": 1},
                                   "retries": {"max_attempts": 1}}]}
    client.post("/workflows", json=wf)
    rid = client.post("/workflows/df/runs").json()["run_id"]
    assert done(client, rid)["state"] == "failed"
    assert client.get("/dead-letters").json()[0]["task_id"] == f"{rid}:a"
    assert client.post(f"/runs/{rid}/tasks/a/replay").status_code == 200
    assert done(client, rid)["state"] == "succeeded" or wait_for(
        lambda: client.get(f"/runs/{rid}").json()["state"] == "succeeded", msg="replay success")
    assert client.post(f"/runs/{rid}/tasks/zzz/replay").status_code == 404


def test_pause_blocks_schedule_flag(client):
    client.post("/workflows/yaml", content=YAML)
    assert client.post("/workflows/api_flow/pause").json()["paused"] is True
    assert client.get("/workflows/api_flow").json()["paused"] is True
    assert client.post("/workflows/api_flow/resume").json()["paused"] is False


def test_metrics_endpoint(client):
    client.post("/workflows/yaml", content=YAML)
    done(client, client.post("/workflows/api_flow/runs").json()["run_id"])
    text = client.get("/metrics").text
    for name in ("flowforge_queue_depth", "flowforge_task_events_total", "flowforge_workers_alive"):
        assert name in text


def test_api_key_enforced(settings, store, history):
    settings.api_key = "s3cret"
    with TestClient(create_app(settings, store, history)) as c:
        assert c.get("/workflows").status_code == 401
        assert c.get("/workflows", headers={"X-API-Key": "wrong"}).status_code == 401
        assert c.get("/workflows", headers={"X-API-Key": "s3cret"}).status_code == 200
        assert c.get("/health").status_code == 200  # liveness stays open
