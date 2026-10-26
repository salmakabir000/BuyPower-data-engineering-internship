import os
import tempfile
import time

import pytest

from mini_orchestrator import dag, task, clear_registry, get_dag
from mini_orchestrator import state
from mini_orchestrator.runner import run_dag


@pytest.fixture(autouse=True)
def isolated_env():
    """Fresh DAG registry and a throwaway SQLite file per test."""
    clear_registry()
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    tmp.close()
    os.environ["MINI_ORCH_DB_PATH"] = tmp.name
    state.DB_PATH = state._default_db_path()
    yield
    os.unlink(tmp.name)
    del os.environ["MINI_ORCH_DB_PATH"]


def test_topological_order_respects_dependencies():
    calls = []

    @dag(name="linear")
    def linear():
        a = task("a", run=lambda: calls.append("a"))
        b = task("b", run=lambda: calls.append("b"), depends_on=[a])
        task("c", run=lambda: calls.append("c"), depends_on=[b])

    order = linear.topological_order()
    assert order.index("a") < order.index("b") < order.index("c")

    run_dag(linear)
    assert calls == ["a", "b", "c"]


def test_diamond_dependency_runs_each_task_once():
    calls = []

    @dag(name="diamond")
    def diamond():
        a = task("a", run=lambda: calls.append("a"))
        b = task("b", run=lambda: calls.append("b"), depends_on=[a])
        c = task("c", run=lambda: calls.append("c"), depends_on=[a])
        task("d", run=lambda: calls.append("d"), depends_on=[b, c])

    run_dag(diamond)
    assert calls[0] == "a"
    assert calls[-1] == "d"
    assert set(calls[1:3]) == {"b", "c"}
    assert len(calls) == 4


def test_cycle_is_detected():
    @dag(name="broken")
    def broken():
        # Build two tasks that depend on each other by patching after
        # construction, since task() normally can't express a forward
        # reference to a task defined later.
        a = task("a", run=lambda: None)
        b = task("b", run=lambda: None, depends_on=[a])
        a.depends_on.append(b)

    with pytest.raises(ValueError, match="Cycle detected"):
        broken.topological_order()


def test_failed_task_skips_downstream():
    calls = []

    def boom():
        raise ValueError("kaboom")

    @dag(name="failing")
    def failing():
        extract = task("extract", run=lambda: calls.append("extract"))
        transform = task("transform", run=boom, depends_on=[extract])
        load = task("load", run=lambda: calls.append("load"), depends_on=[transform])
        task("quality_check", run=lambda: calls.append("quality_check"), depends_on=[load])

    run_id = run_dag(failing)

    task_runs = {t["task_name"]: t for t in state.get_task_runs(run_id)}
    assert task_runs["extract"]["status"] == "success"
    assert task_runs["transform"]["status"] == "failed"
    assert "kaboom" in task_runs["transform"]["error_message"]
    assert task_runs["load"]["status"] == "skipped"
    assert task_runs["quality_check"]["status"] == "skipped"
    assert calls == ["extract"]  # load and quality_check never ran

    dag_run = state.get_recent_runs("failing", limit=1)[0]
    assert dag_run["status"] == "failed"


def test_independent_branch_still_runs_after_sibling_fails():
    calls = []

    def boom():
        raise ValueError("nope")

    @dag(name="partial_failure")
    def partial_failure():
        a = task("a", run=boom)
        task("a_child", run=lambda: calls.append("a_child"), depends_on=[a])
        b = task("b", run=lambda: calls.append("b"))
        task("b_child", run=lambda: calls.append("b_child"), depends_on=[b])

    run_id = run_dag(partial_failure)
    task_runs = {t["task_name"]: t["status"] for t in state.get_task_runs(run_id)}
    assert task_runs["a"] == "failed"
    assert task_runs["a_child"] == "skipped"
    assert task_runs["b"] == "success"
    assert task_runs["b_child"] == "success"


def test_state_persists_across_multiple_runs():
    @dag(name="repeatable")
    def repeatable():
        task("only", run=lambda: None)

    run_dag(repeatable)
    run_dag(repeatable)
    run_dag(repeatable)

    runs = state.get_recent_runs("repeatable", limit=10)
    assert len(runs) == 3
    assert all(r["status"] == "success" for r in runs)
    # newest first
    assert runs[0]["started_at"] >= runs[1]["started_at"] >= runs[2]["started_at"]


def test_get_dag_raises_for_unknown_name():
    with pytest.raises(KeyError):
        get_dag("does_not_exist")


def test_parallel_mode_runs_same_level_concurrently():
    started = []
    finished = []

    def slow_task(name):
        started.append(name)
        time.sleep(0.2)
        finished.append(name)

    @dag(name="parallel_test")
    def parallel_test():
        task("p1", run=lambda: slow_task("p1"))
        task("p2", run=lambda: slow_task("p2"))
        task("p3", run=lambda: slow_task("p3"))

    t0 = time.time()
    run_dag(parallel_test, parallel=True)
    elapsed = time.time() - t0

    # Three tasks each sleeping 0.2s: sequential would take >=0.6s,
    # concurrent should finish well under that.
    assert elapsed < 0.5
    assert set(started) == {"p1", "p2", "p3"}


def test_parallel_mode_still_skips_downstream_of_failure():
    def boom():
        raise ValueError("bad")

    @dag(name="parallel_failure")
    def parallel_failure():
        a = task("a", run=boom)
        task("a_downstream", run=lambda: None, depends_on=[a])

    run_id = run_dag(parallel_failure, parallel=True)
    task_runs = {t["task_name"]: t["status"] for t in state.get_task_runs(run_id)}
    assert task_runs["a"] == "failed"
    assert task_runs["a_downstream"] == "skipped"
