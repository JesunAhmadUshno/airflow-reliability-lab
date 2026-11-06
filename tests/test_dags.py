import os
from pathlib import Path

os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
os.environ.setdefault("AIRFLOW__CORE__UNIT_TEST_MODE", "True")

DAGS_SRC = Path(__file__).resolve().parent.parent / "dags_src"
EXPECTED = {
    "airrel_ingestion",
    "airrel_embedding",
    "airrel_evaluation",
    "airrel_retraining",
    "airrel_monitoring",
}


def test_all_dags_parse_without_errors(tmp_path, monkeypatch):
    monkeypatch.setenv("AIRFLOW_HOME", str(tmp_path / "airflow_home"))
    monkeypatch.setenv(
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN",
        f"sqlite:///{tmp_path}/airflow.db",
    )
    import subprocess
    import sys

    airflow_bin = Path(sys.executable).with_name("airflow")
    subprocess.run(
        [str(airflow_bin), "db", "migrate"],
        env=dict(os.environ),
        check=True,
        capture_output=True,
        timeout=300,
    )
    from airflow.models import DagBag

    bag = DagBag(dag_folder=str(DAGS_SRC))
    assert not bag.import_errors, f"import errors: {bag.import_errors}"
    assert EXPECTED <= set(bag.dag_ids)
    ingestion = bag.get_dag("airrel_ingestion")
    assert [t.task_id for t in ingestion.tasks] == ["prepare", "gate", "write"]
    for dag_id in EXPECTED - {"airrel_ingestion"}:
        dag = bag.get_dag(dag_id)
        assert [t.task_id for t in dag.tasks] == ["prepare", "write"]
        write = dag.get_task("write")
        assert write.retries == 1
