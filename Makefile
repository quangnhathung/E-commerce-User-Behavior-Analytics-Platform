PYTHON ?= python3
export CONFIRM
export RUN_ID START END RATE
.PHONY: setup build up doctor smoke persistence-test ps logs down reset config test versions ingest test-data prepare-demo stream replay stream-status test-stream

setup build up doctor smoke persistence-test ps logs down reset config versions:
	@$(PYTHON) infra/manage.py $@

test:
	@$(PYTHON) -m unittest discover -s infra/tests -v
	@$(PYTHON) -m unittest discover -s app/tests -v

ingest test-data:
	@$(PYTHON) app/manage.py $@

prepare-demo stream replay stream-status test-stream:
	@$(PYTHON) app/streaming_manage.py $@
