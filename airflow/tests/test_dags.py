"""DagBag integrity tests for the three sentinel DAGs."""

from __future__ import annotations

import sys

import pytest


def _dag_paths():
    return ["/usr/local/airflow/dags", "airflow/dags"]


@pytest.fixture(scope="module")
def dag_bag():
    from airflow.models import DagBag

    for p in _dag_paths():
        sys.path.insert(0, p.replace("/dags", "/plugins"))
        sys.path.insert(0, p.replace("/dags", "/include"))
    bag = None
    for p in _dag_paths():
        candidate = DagBag(dag_folder=p, include_examples=False)
        if candidate.dags:
            bag = candidate
            break
    assert bag is not None, "DagBag could not load any DAGs"
    return bag


def test_no_import_errors(dag_bag):
    assert not dag_bag.import_errors, dag_bag.import_errors


def test_three_dags_loaded(dag_bag):
    expected = {
        "ratings_etl_with_sentinel",
        "sentinel_regression_suite",
        "sentinel_weekly_digest",
    }
    assert expected.issubset(set(dag_bag.dag_ids))


def test_all_dags_have_tags(dag_bag):
    for dag_id in dag_bag.dag_ids:
        dag = dag_bag.get_dag(dag_id)
        assert dag.tags, f"DAG {dag_id} has no tags"
        assert "sentinel" in dag.tags


def test_etl_dag_uses_decorated_functions(dag_bag):
    from etl_transforms import (  # type: ignore
        aggregate_by_movie,
        clean_and_enrich_ratings,
        ingest_raw_ratings,
    )

    for fn in (ingest_raw_ratings, clean_and_enrich_ratings, aggregate_by_movie):
        assert hasattr(fn, "__sentinel_checks__"), f"{fn.__name__} missing @observe"
        assert hasattr(fn, "__wrapped__"), f"{fn.__name__} not wrapped by functools.wraps"


def test_regression_suite_has_one_task_per_check(dag_bag):
    dag = dag_bag.get_dag("sentinel_regression_suite")
    check_tasks = [t for t in dag.tasks if t.task_id.startswith("test_") and t.task_id.endswith("_check")]
    assert len(check_tasks) == 8, [t.task_id for t in check_tasks]

def test_weekly_digest_schedule(dag_bag):
    dag = dag_bag.get_dag("sentinel_weekly_digest")
    assert dag.schedule == "0 9 * * 1" or str(dag.timetable.summary) == "0 9 * * 1"

def test_etl_dag_schedule(dag_bag):
    dag = dag_bag.get_dag("ratings_etl_with_sentinel")
    assert dag.schedule == "0 3 * * *" or str(dag.timetable.summary) == "0 3 * * *"


def test_regression_dag_schedule(dag_bag):
    dag = dag_bag.get_dag("sentinel_regression_suite")
    assert dag.schedule == "0 0 * * *" or str(dag.timetable.summary) == "0 0 * * *"
