PYTHON ?= python3
export CONFIRM
export RUN_ID START END RATE ITEM_ID
export AS_OF FEATURE_SNAPSHOT_ID MODEL_VERSION VISITOR_ID
.PHONY: setup build up doctor smoke persistence-test ps logs down reset config test versions ingest test-data prepare-demo stream replay stream-status test-stream analyze-behavior behavior-status test-behavior

setup build up doctor smoke persistence-test ps logs down reset config versions:
	@$(PYTHON) infra/manage.py $@

test:
	@$(PYTHON) -m unittest discover -s infra/tests -v
	@$(PYTHON) -m unittest discover -s app/tests -v

ingest test-data:
	@$(PYTHON) app/manage.py $@

prepare-demo stream replay stream-status test-stream analyze-behavior behavior-status test-behavior:
	@$(PYTHON) app/streaming_manage.py $@

.PHONY: build-features train-segments segment-status test-ml
build-features train-segments segment-status test-ml:
	@$(PYTHON) app/ml_manage.py $@
