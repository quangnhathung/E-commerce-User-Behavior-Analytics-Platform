PYTHON ?= python3
export CONFIRM
.PHONY: setup build up doctor smoke persistence-test ps logs down reset config test versions

setup build up doctor smoke persistence-test ps logs down reset config versions:
	@$(PYTHON) infra/manage.py $@

test:
	@$(PYTHON) -m unittest discover -s infra/tests -v
