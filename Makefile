STAGE?=dev

S3_BUCKET=gretel-opt-$(STAGE)
S3_PREFIX=pub

PYTHON?=python
PIP?=pip
FLAKE?=flake8
PYTEST?=pytest
AWS?=aws
NBUTILS?=nbutils

PKG_WHL=gretel_client
PKG_TAR=gretel-client
VERSION=$(shell $(PYTHON) -c "import setuptools_scm;print(setuptools_scm.get_version())")


.PHONY: setup
setup:
	$(PIP) install -U -e ".[fpe]"
	$(PIP) install -r dev-requirements.txt


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


.PHONY: test-nb
test-nb:
	nbutils run --notebooks="notebooks/{priv,pub}/*.ipynb"


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


.PHONY: publish-notebooks
publish-notebooks:
	$(NBUTILS) transform \
		--notebooks="notebooks/pub/*.ipynb" \
		--template-source="notebooks/etc/Bootstrap_Template.ipynb" \
		--output-dir="../gretel-transformers-notebooks/examples" \
		--add-to-index \
		--all
