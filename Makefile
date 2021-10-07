# Adapted from https://github.com/9gl/python/blob/2d8f03367f7b430738f25b3d1aa891c3df1cf069/py_automation/Makefile

.PHONY: help prepare-dev test lint run doc default

VENV_NAME?=venv
VENV_ACTIVATE=. $(VENV_NAME)/bin/activate
PYTHON_VENV=${VENV_NAME}/bin/python3
PYTHON_LOCAL=python3

default: create-venv run

.DEFAULT: help
help:
	@echo "make prepare-dev"
	@echo "       prepare development environment, use only once"
	@echo "make test"
	@echo "       run tests"
	@echo "make lint"
	@echo "       run pylint and mypy"
	@echo "make run"
	@echo "       run project"
	@echo "make doc"
	@echo "       build sphinx documentation"

prepare-dev:
	sudo apt-get -y install python3.8 python3-pip
	
create-venv:	
	python3 -m pip install virtualenv

venv: requirements.txt
	test -d $(VENV_NAME) || virtualenv -p python3 $(VENV_NAME)
	${PYTHON_VENV} -m pip install -U pip
	${PYTHON_VENV} -m pip  install  -r requirements.txt
	touch $(VENV_NAME)/bin/activate


test: venv
	${PYTHON_VENV} -m pytest

lint: venv
	${PYTHON_VENV} -m pylint src/
	${PYTHON_VENV} -m mypy

recreate-tables:
	${VENV_ACTIVATE}
	${PYTHON_VENV} src/create_tables.py

run: recreate-tables
	export POSTGRES_URL=postgresql://postgres:postgres@localhost/postgres
	${PYTHON_VENV} src/etl.py

doc: venv
	$(VENV_ACTIVATE) && cd docs; make html

package:
	rm -f submission.zip&&cd src&&zip -j   ../submission.zip  etl.py create_tables.py sql_queries.py ../README.md
