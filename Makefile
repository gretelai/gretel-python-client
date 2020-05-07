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


.PHONY: lint
lint:
	$(FLAKE) --count --select=E9,F63,F7,F82 --show-source --statistics src/
	$(FLAKE) --count --exit-zero --max-complexity=30 --max-line-length=120 --statistics src/


.PHONY: test
test:
	$(PYTEST) -s -vv --cov src --cov-report term-missing tests/gretel_client/unit


.PHONY: int-test
int-test:
	$(PYTEST) -s -vv --cov src --cov-report term-missing tests/gretel_client/integration


.PHONY: clean
clean:
	@rm -rf dist/ build/


.PHONY: build
build: clean
	$(PIP) install wheel setuptools_scm
	$(PYTHON) setup.py sdist bdist_wheel


.PHONY: release
release: build
	$(AWS) s3 cp --acl public-read \
		dist/$(PKG_WHL)-$(VERSION)-py3-none-any.whl \
		s3://gretel-opt-$(STAGE)/pub/$(PKG_WHL)/$(PKG_WHL)-latest-py3-none-any.whl
	$(AWS) s3 cp --acl public-read \
		dist/$(PKG_TAR)-$(VERSION).tar.gz \
		s3://gretel-opt-$(STAGE)/pub/$(PKG_TAR)/$(PKG_TAR)-latest.tar.gz
