import os
import shutil
import socket
import subprocess
import time
import uuid

import pytest
import redis

os.environ.setdefault("FLOWFORGE_ENABLE_TEST_TASKS", "1")

from app.config import Settings  # noqa: E402
from app.db.repository import HistoryStore  # noqa: E402
from app.engine.orchestrator import Orchestrator  # noqa: E402
from app.kv.redis_store import RedisStore  # noqa: E402
from app.tasks.notifications import LoggingAlertSink  # noqa: E402
from app.tasks.testing import register_test_tasks  # noqa: E402
from app.workers.worker import Worker  # noqa: E402

register_test_tasks()


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="session")
def redis_url(tmp_path_factory):
    """A throwaway real redis-server (or REDIS_URL if provided), so Lua/stream behaviour is genuinely tested."""
    if os.getenv("REDIS_URL"):
        yield os.environ["REDIS_URL"]
        return
    exe = shutil.which("redis-server")
    if not exe:
        pytest.skip("redis-server not installed")
    port = _free_port()
    proc = subprocess.Popen([exe, "--port", str(port), "--save", "", "--appendonly", "no",
                             "--dir", str(tmp_path_factory.mktemp("redis"))],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    url = f"redis://127.0.0.1:{port}/0"
    for _ in range(100):
        try:
            redis.Redis.from_url(url).ping()
            break
        except redis.ConnectionError:
            time.sleep(0.05)
    yield url
    proc.terminate()
    proc.wait(5)


@pytest.fixture
def settings(redis_url, tmp_path):
    return Settings(redis_url=redis_url, database_url=f"sqlite:///{tmp_path/'ff.db'}",
                    key_prefix=f"t{uuid.uuid4().hex[:8]}:", lease_ttl_s=1.0, maintenance_interval_s=0.2,
                    pending_idle_ms=800, enable_test_tasks=True, allowed_http_hosts=("api.example.com",))


@pytest.fixture
def store(settings):
    s = RedisStore(url=settings.redis_url, prefix=settings.key_prefix)
    yield s
    for k in s.r.scan_iter(f"{settings.key_prefix}*"):
        s.r.delete(k)
    s.close()


@pytest.fixture
def history(settings):
    h = HistoryStore(settings.database_url)
    h.init_schema()
    return h


@pytest.fixture
def alerts():
    return LoggingAlertSink()


@pytest.fixture
def orch(store, history, settings, alerts):
    return Orchestrator(store, history, settings, alerts)


@pytest.fixture
def start_worker(orch, settings):
    workers = []

    def _start(n=1, **kw):
        w = Worker(orch, settings, **kw).start()
        workers.append(w)
        return w
    yield _start
    for w in workers:
        w.stop()


def wait_for(pred, timeout=15, interval=0.05, msg="condition"):
    end = time.time() + timeout
    while time.time() < end:
        v = pred()
        if v:
            return v
        time.sleep(interval)
    raise AssertionError(f"timed out waiting for {msg}")


def wait_run(orch, run_id, timeout=20):
    return wait_for(lambda: (s := orch.snapshot(run_id)) and s["state"] != "running" and s,
                    timeout, msg=f"run {run_id} to finish")


def wf(name="w", tasks=None, **kw):
    from app.engine.workflow import WorkflowDef
    return WorkflowDef.model_validate({"name": name, "concurrency": {"max_runs": 50, "on_overlap": "allow"},
                                       "tasks": tasks or [{"id": "a", "type": "noop"}], **kw})
