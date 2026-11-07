VENV ?= .venv
PY := $(VENV)/bin/python
AIRFLOW := $(VENV)/bin/airflow

setup:
	python3 -m venv $(VENV)
	$(VENV)/bin/pip install -r requirements-lock.txt

test:
	$(PY) -m pytest -q

study:
	$(PY) harness/run_study.py --root workdir/run --airflow-bin $(AIRFLOW)
	PYTHONPATH=src $(PY) -m airrel.report results RESULTS.md

calibrate:
	PYTHONPATH=src $(PY) -m airrel.drift_calibration results/drift_calibration.json

reproduce: setup test study
