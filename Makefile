STAGE?=dev

S3_BUCKET=gretel-opt-$(STAGE)
S3_PREFIX=pub

PYTHON?=python
PIP?=pip
FLAKE?=flake8
PYTEST?=pytest
AWS?=aws

PKG_WHL=gretel_client
PKG_TAR=gretel-client
VERSION=$(shell $(PYTHON) -c "import setuptools_scm;print(setuptools_scm.get_version())")


.PHONY: setup
setup:
	$(PIP) install -U -e .
	$(PIP) install -r dev-requirements.txt
	$(PIP) install dataclasses


.PHONY: lint
lint:
	$(FLAKE) --count --select=E9,F63,F7,F82 --show-source --statistics src/
	$(FLAKE) --count --exit-zero --max-complexity=30 --max-line-length=120 --statistics src/


.PHONY: test
test:
	$(PYTEST) -s -vv --cov src --cov-report term-missing tests/src/gretel_client/unit


.PHONY: test-int
test-int:
	$(PYTEST) -s -vv --cov src --cov-report term-missing tests/src/gretel_client/integration


.PHONY: test-all
test-all:
	$(PYTEST) -s -vv --cov src --cov-report term-missing tests/src/gretel_client/


.PHONY: clean
clean:
	@rm -rf dist/ build/


.PHONY: build
build: clean
	$(PIP) install wheel setuptools_scm
	$(PYTHON) setup.py sdist bdist_wheel
