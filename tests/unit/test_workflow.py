import pytest
from pydantic import ValidationError

from app.engine.dependency_resolver import CycleError, ready_tasks, topological_order
from app.engine.retry import compute_delay
from app.engine.state_machine import InvalidTransition, TaskState, assert_transition
from app.engine.workflow import RetryPolicy, WorkflowDef, load_workflow

EXAMPLE = open("examples/daily_data_pipeline.yaml").read()


def mk(tasks, **kw):
    return WorkflowDef.model_validate({"name": "w", "tasks": tasks, **kw})


def test_example_yaml_parses():
    d = load_workflow(EXAMPLE)
    assert d.name == "daily_book_pipeline" and d.schedule == "0 8 * * *"
    assert [t.id for t in d.tasks][:2] == ["fetch_books", "validate_books"]


def test_duplicate_ids_rejected():
    with pytest.raises(ValidationError, match="duplicate"):
        mk([{"id": "a", "type": "noop"}, {"id": "a", "type": "noop"}])


def test_unknown_dependency_rejected():
    with pytest.raises(ValidationError, match="unknown task"):
        mk([{"id": "a", "type": "noop", "depends_on": ["zzz"]}])


def test_self_dependency_and_cycle_rejected():
    with pytest.raises(ValidationError, match="itself"):
        mk([{"id": "a", "type": "noop", "depends_on": ["a"]}])
    with pytest.raises(ValidationError, match="cycle"):
        mk([{"id": "a", "type": "noop", "depends_on": ["b"]}, {"id": "b", "type": "noop", "depends_on": ["a"]}])


def test_bad_ids_cron_and_handlers():
    with pytest.raises(ValidationError):
        mk([{"id": "has space", "type": "noop"}])
    with pytest.raises(ValidationError, match="cron"):
        mk([{"id": "a", "type": "noop"}], schedule="not a cron")
    d = mk([{"id": "a", "type": "os_system"}])
    with pytest.raises(ValueError, match="unknown task type"):
        d.validate_handlers({"noop"})


def test_topological_order_deterministic_and_respects_deps():
    d = mk([{"id": "c", "type": "x", "depends_on": ["a", "b"]}, {"id": "b", "type": "x"},
            {"id": "a", "type": "x"}, {"id": "d", "type": "x", "depends_on": ["c"]}])
    assert topological_order(d.tasks) == ["b", "a", "c", "d"]
    assert topological_order(d.tasks) == topological_order(d.tasks)


def test_cycle_error_type():
    class T:
        def __init__(s, i, d): s.id, s.depends_on = i, d
    with pytest.raises(CycleError):
        topological_order([T("a", ["b"]), T("b", ["a"])])


def test_ready_tasks():
    d = mk([{"id": "a", "type": "x"}, {"id": "b", "type": "x", "depends_on": ["a"]}, {"id": "c", "type": "x"}])
    assert ready_tasks(d.tasks, {"a": "pending", "b": "pending", "c": "pending"}) == ["a", "c"]
    assert ready_tasks(d.tasks, {"a": "succeeded", "b": "pending", "c": "running"}) == ["b"]


def test_state_machine():
    assert_transition(TaskState.RUNNING, TaskState.SUCCEEDED)
    assert_transition(TaskState.FAILED, TaskState.PENDING)
    with pytest.raises(InvalidTransition):
        assert_transition(TaskState.SUCCEEDED, TaskState.RUNNING)
    with pytest.raises(InvalidTransition):
        assert_transition(TaskState.PENDING, TaskState.RUNNING)


def test_backoff_exponential_capped_and_jitter_bounded():
    p = RetryPolicy(base_delay_s=1, factor=2, max_delay_s=10, jitter=False)
    assert [compute_delay(p, n) for n in (1, 2, 3, 4, 5)] == [1, 2, 4, 8, 10]
    pj = p.model_copy(update={"jitter": True})
    assert all(0 <= compute_delay(pj, 4) <= 8 for _ in range(200))
    assert compute_delay(RetryPolicy(backoff="fixed", base_delay_s=3, jitter=False), 7) == 3
