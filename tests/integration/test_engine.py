"""End-to-end: orchestrator + real workers + real Redis + SQLite history."""
import json

import httpx

from app.tasks import http_fetch
from tests.conftest import wait_for, wait_run, wf


def test_linear_workflow_passes_results(orch, start_worker):
    start_worker()
    d = wf(tasks=[{"id": "a", "type": "echo", "config": {"value": 7}},
                  {"id": "b", "type": "echo", "depends_on": ["a"], "config": {"value": 8}}])
    rid = orch.trigger(d, params={"who": "me"}).run_id
    snap = wait_run(orch, rid)
    assert snap["state"] == "succeeded"
    assert snap["tasks"]["b"]["result"]["inputs"]["a"]["value"] == 7
    assert snap["tasks"]["a"]["result"]["params"] == {"who": "me"}


def test_diamond_runs_parallel_branches_then_join(orch, start_worker):
    start_worker(concurrency=3)
    d = wf(tasks=[{"id": "a", "type": "noop"},
                  {"id": "b", "type": "sleep", "depends_on": ["a"], "config": {"seconds": 0.3}},
                  {"id": "c", "type": "sleep", "depends_on": ["a"], "config": {"seconds": 0.3}},
                  {"id": "d", "type": "noop", "depends_on": ["b", "c"]}])
    rid = orch.trigger(d).run_id
    snap = wait_run(orch, rid)
    assert snap["state"] == "succeeded"
    h = lambda k: orch.task_hash(f"{rid}:{k}")  # noqa: E731
    assert int(h("b")["started_ms"]) < int(h("c")["finished_ms"]) and int(h("c")["started_ms"]) < int(h("b")["finished_ms"])
    assert int(h("d")["started_ms"]) >= max(int(h("b")["finished_ms"]), int(h("c")["finished_ms"]))


def test_retry_with_backoff_then_success(orch, start_worker):
    start_worker()
    d = wf(tasks=[{"id": "a", "type": "flaky", "config": {"fail_attempts": 2},
                   "retries": {"max_attempts": 4, "base_delay_s": 0.05, "jitter": False}}])
    snap = wait_run(orch, orch.trigger(d).run_id)
    assert snap["state"] == "succeeded"
    assert snap["tasks"]["a"]["attempt"] == 3 and snap["tasks"]["a"]["failures"] == 2
    types = [e for e in orch.history.get_events(snap["id"])]
    assert sum(e["type"] == "task_retry_scheduled" for e in types) == 2


def test_dead_letter_skips_downstream_alerts_and_replay_recovers(orch, start_worker, alerts, store):
    start_worker()
    d = wf(tasks=[{"id": "a", "type": "flaky", "config": {"fail_attempts": 2},
                   "retries": {"max_attempts": 2, "base_delay_s": 0.01, "jitter": False}},
                  {"id": "b", "type": "noop", "depends_on": ["a"]}])
    rid = orch.trigger(d).run_id
    snap = wait_run(orch, rid)
    assert snap["state"] == "failed"
    assert snap["tasks"]["a"]["state"] == "failed" and snap["tasks"]["b"]["state"] == "skipped"
    assert len(alerts.sent) == 1 and "dead-lettered" in alerts.sent[0][0]
    assert store.queue.dead_letters()[0]["task_id"] == f"{rid}:a"
    # replay: fresh retry budget; attempt counter keeps growing so (3 > fail_attempts) it now succeeds
    assert orch.replay_task(rid, "a") is True
    snap = wait_for(lambda: (s := orch.snapshot(rid)) and s["state"] == "succeeded" and s, msg="replayed run")
    assert snap["tasks"]["b"]["state"] == "succeeded"
    import pytest
    with pytest.raises(ValueError, match="succeeded"):
        orch.replay_task(rid, "a")  # finished runs cannot be replayed again


def test_non_retryable_fails_without_retries(orch, start_worker):
    start_worker()
    d = wf(tasks=[{"id": "a", "type": "fatal", "retries": {"max_attempts": 5}}])
    snap = wait_run(orch, orch.trigger(d).run_id)
    assert snap["state"] == "failed" and snap["tasks"]["a"]["attempt"] == 1


def test_task_timeout_is_a_retryable_failure(orch, start_worker):
    start_worker()
    d = wf(tasks=[{"id": "a", "type": "sleep", "config": {"seconds": 5}, "timeout_seconds": 0.2,
                   "retries": {"max_attempts": 2, "base_delay_s": 0.01, "jitter": False}}])
    snap = wait_run(orch, orch.trigger(d).run_id)
    assert snap["state"] == "failed" and snap["tasks"]["a"]["attempt"] == 2
    assert "timeout" in snap["tasks"]["a"]["last_error"].lower()


def test_idempotent_trigger_returns_same_run(orch, start_worker):
    d = wf()
    r1 = orch.trigger(d, idempotency_key="order-42")
    r2 = orch.trigger(d, idempotency_key="order-42")
    assert r1.created and not r2.created and r1.run_id == r2.run_id
    assert len(orch.r.smembers(orch.k.active_runs)) == 1


def test_overlap_skip_policy_and_slot_release(orch, start_worker):
    d = wf(tasks=[{"id": "a", "type": "sleep", "config": {"seconds": 0.4}}],
           concurrency={"max_runs": 1, "on_overlap": "skip"})
    first = orch.trigger(d)
    second = orch.trigger(d)
    assert first.created and second.skipped and second.run_id is None
    start_worker()
    wait_run(orch, first.run_id)
    assert orch.trigger(d).created  # slot freed after the first run finished


def test_cancel_run_cancels_pending_and_running(orch, start_worker):
    start_worker()
    d = wf(tasks=[{"id": "a", "type": "sleep", "config": {"seconds": 10}},
                  {"id": "b", "type": "noop", "depends_on": ["a"]}])
    rid = orch.trigger(d).run_id
    wait_for(lambda: orch.task_hash(f"{rid}:a").get("state") == "running", msg="a running")
    assert orch.cancel_run(rid)
    snap = wait_run(orch, rid, timeout=10)
    assert snap["state"] == "cancelled"
    assert snap["tasks"]["a"]["state"] == "cancelled" and snap["tasks"]["b"]["state"] == "cancelled"
    assert orch.cancel_run(rid) is False


def test_history_persisted_to_database(orch, start_worker, history):
    start_worker()
    rid = orch.trigger(wf(tasks=[{"id": "a", "type": "echo", "config": {"value": 1}}])).run_id
    wait_run(orch, rid)
    row = wait_for(lambda: (r := history.get_run(rid)) and r["state"] == "succeeded" and r, msg="db history")
    assert row["tasks"]["a"]["state"] == "succeeded" and row["tasks"]["a"]["result"]["value"] == 1
    kinds = [e["type"] for e in history.get_events(rid)]
    assert kinds[0] == "run_created" and "task_succeeded" in kinds and kinds[-1] == "run_succeeded"


def test_daily_pipeline_end_to_end(orch, start_worker, history, alerts):
    """The concept doc's example: fetch -> validate -> upsert -> report -> notify, with a malformed record."""
    payload = [{"id": 1, "title": " Dune "}, {"id": 2, "title": "Emma"}, {"id": 3, "title": ""}, {"id": 4, "title": "Ulysses"}]
    http_fetch._transport = httpx.MockTransport(lambda req: httpx.Response(200, json=payload))
    try:
        start_worker()
        d = wf(name="daily", tasks=[
            {"id": "fetch", "type": "http_fetch", "config": {"url": "https://api.example.com/books"}},
            {"id": "validate", "type": "validate_records", "depends_on": ["fetch"],
             "config": {"required_fields": ["id", "title"]}},
            {"id": "save", "type": "database_upsert", "depends_on": ["validate"], "config": {"key_field": "id"}},
            {"id": "report", "type": "report_generation", "depends_on": ["save", "validate"]}])
        snap = wait_run(orch, orch.trigger(d).run_id)
        assert snap["state"] == "succeeded", snap
        assert snap["tasks"]["validate"]["result"]["rejected_count"] == 1
        assert history.count_records() == 3
        # re-running the pipeline is idempotent: same stable ids -> no duplicate rows
        wait_run(orch, orch.trigger(d).run_id)
        assert history.count_records() == 3
        assert "validate" in snap["tasks"]["report"]["result"]["report"]
    finally:
        http_fetch._transport = None


def test_http_fetch_blocks_disallowed_hosts_and_retries_5xx(orch, start_worker):
    calls = {"n": 0}

    def handler(req):
        calls["n"] += 1
        return httpx.Response(503 if calls["n"] < 3 else 200, json={"ok": True})
    http_fetch._transport = httpx.MockTransport(handler)
    try:
        start_worker()
        ok = wf(tasks=[{"id": "f", "type": "http_fetch", "config": {"url": "https://api.example.com/x"},
                        "retries": {"max_attempts": 4, "base_delay_s": 0.01, "jitter": False}}])
        assert wait_run(orch, orch.trigger(ok).run_id)["state"] == "succeeded" and calls["n"] == 3
        bad = wf(tasks=[{"id": "f", "type": "http_fetch", "config": {"url": "http://169.254.169.254/latest"}}])
        snap = wait_run(orch, orch.trigger(bad).run_id)
        assert snap["state"] == "failed" and snap["tasks"]["f"]["attempt"] == 1  # blocked, not retried
    finally:
        http_fetch._transport = None


def test_scheduler_fire_is_deduplicated_and_respects_pause(orch, history):
    from datetime import datetime, timezone
    from app.engine.scheduler import FlowScheduler
    d = wf(name="sched")
    history.save_workflow(d)
    s1, s2 = FlowScheduler(orch, history), FlowScheduler(orch, history)  # two scheduler instances
    now = datetime(2026, 11, 1, 8, 0, 30, tzinfo=timezone.utc)
    a, b = s1.fire("sched", now), s2.fire("sched", now)
    assert a.created and not b.created and a.run_id == b.run_id
    history.set_paused("sched", True)
    assert s1.fire("sched", datetime(2026, 11, 2, 8, 0, tzinfo=timezone.utc)) is None
