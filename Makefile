PYTHON ?= python3
export CONFIRM
.PHONY: setup build up doctor smoke persistence-test ps logs down reset config test versions ingest test-data

setup build up doctor smoke persistence-test ps logs down reset config versions:
	@$(PYTHON) infra/manage.py $@

test:
	@$(PYTHON) -m unittest discover -s infra/tests -v
	@$(PYTHON) -m unittest discover -s app/tests -v

ingest test-data:
	@$(PYTHON) app/manage.py $@
