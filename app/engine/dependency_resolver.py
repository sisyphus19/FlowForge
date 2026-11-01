"""Deterministic dependency resolution."""
from __future__ import annotations

from typing import Iterable, Mapping, Protocol


class _TaskLike(Protocol):
    id: str
    depends_on: list[str]


class CycleError(ValueError):
    pass


def topological_order(tasks: Iterable[_TaskLike]) -> list[str]:
    """Kahn's algorithm; ties broken by definition order, so output is deterministic."""
    tasks = list(tasks)
    position = {t.id: i for i, t in enumerate(tasks)}
    indegree = {t.id: len(set(t.depends_on)) for t in tasks}
    children: dict[str, list[str]] = {t.id: [] for t in tasks}
    for t in tasks:
        for d in set(t.depends_on):
            children[d].append(t.id)
    ready = sorted((i for i, n in indegree.items() if n == 0), key=position.get)
    order: list[str] = []
    while ready:
        cur = ready.pop(0)
        order.append(cur)
        for c in sorted(children[cur], key=position.get):
            indegree[c] -= 1
            if indegree[c] == 0:
                ready.append(c)
        ready.sort(key=position.get)
    if len(order) != len(tasks):
        stuck = sorted(i for i, n in indegree.items() if n > 0)
        raise CycleError(f"dependency cycle involving: {', '.join(stuck)}")
    return order


def ready_tasks(tasks: Iterable[_TaskLike], states: Mapping[str, str]) -> list[str]:
    """Pending tasks whose dependencies have all succeeded (in definition order)."""
    return [t.id for t in tasks
            if states.get(t.id) == "pending" and all(states.get(d) == "succeeded" for d in t.depends_on)]
