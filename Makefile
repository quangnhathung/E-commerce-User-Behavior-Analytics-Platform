PYTHON ?= python3
export CONFIRM
export RUN_ID START END RATE ITEM_ID
export AS_OF FEATURE_SNAPSHOT_ID MODEL_VERSION VISITOR_ID
export PURCHASE_DATASET_ID SCORE_RUN_ID
export POLICY_VERSION
export DEMO_SOURCE SEGMENT_MODEL_VERSION PURCHASE_MODEL_VERSION
export LIVE_ID LIVE_EVENTS LIVE_VISITORS LIVE_ITEMS LIVE_SEGMENTS
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

.PHONY: build-purchase-data train-purchase purchase-status score-purchases purchase-score-status test-purchase
build-purchase-data train-purchase purchase-status score-purchases purchase-score-status test-purchase:
	@$(PYTHON) app/prediction_manage.py $@

.PHONY: tune-purchase-policy apply-purchase-policy purchase-policy-status test-purchase-policy
tune-purchase-policy apply-purchase-policy purchase-policy-status test-purchase-policy:
	@$(PYTHON) app/purchase_policy_manage.py $@

.PHONY: prepare-ml-demo stream-ml replay-ml simulate-ml stream-ml-status test-stream-ml
prepare-ml-demo stream-ml replay-ml simulate-ml stream-ml-status test-stream-ml:
	@$(PYTHON) app/streaming_ml_manage.py $@

.PHONY: api-setup api api-indexes test-api test-api-offline test-web smoke-web
api-setup api api-indexes test-api test-api-offline test-web smoke-web:
	@$(PYTHON) services/manage.py $@

.PHONY: live-demo live-status live-stop test-live
live-demo live-status live-stop:
	@$(PYTHON) app/live_manage.py $@

test-live:
	@$(PYTHON) -m unittest discover -s app/tests -p 'test_live*.py' -v
