# Gretel Python Client

<p align="left">
<img width=15% src="https://gretel-public-website.s3.amazonaws.com/assets/gobs_the_cat_@1x.png" alt="Gobs the Gretel.ai cat" />
<i>CLI and Python SDKs for interacting with Gretel's privacy engineering APIs.</i>
</p>

[![Tests](https://github.com/gretelai/gretel-python-client/actions/workflows/tests.yml/badge.svg)](https://github.com/gretelai/gretel-python-client/actions/workflows/tests.yml)
[![Documentation Status](https://readthedocs.org/projects/gretel-client/badge/?version=stable)](https://gretel-client.readthedocs.io/en/stable/?badge=stable?badge=stable)

[![License](https://img.shields.io/github/license/gretelai/gretel-python-client)](https://github.com/gretelai/gretel-python-client/blob/main/LICENSE)
[![PyPI](https://badge.fury.io/py/gretel-client.svg)](https://badge.fury.io/py/gretel-client)
[![Python](https://img.shields.io/pypi/pyversions/gretel-client.svg)](https://github.com/gretelai/gretel-python-client)
[![Downloads](https://pepy.tech/badge/gretel-client)](https://pepy.tech/project/gretel-client)

[![Discord](https://img.shields.io/discord/1007817822614847500?label=Discord&logo=Discord&style=social)](https://gretel.ai/discord)
[![GitHub stars](https://img.shields.io/github/stars/gretelai/gretel-python-client?style=social)](https://github.com/gretelai/gretel-python-client)

## Getting Started

The following command will install the latest stable Gretel CLI and Python SDK

```
pip install gretel-client
```

To install the latest development version, you may run

```
pip install git+https://github.com/gretelai/gretel-python-client@main
```

To configure the CLI, run

```
gretel configure
```

## System Requirements

The Gretel CLI and python SDKs require Python version 3.9 or greater. Docker is required for local training and generation jobs.

For more information please refer to the [Gretel Environment Setup](https://docs.gretel.ai/environment-setup) docs.

## Client SDKs

The `gretel-client` package also ships with a set of Python Client SDKs that may be used to interact with Gretel APIs using a familiar pythonic interface. For more information on how to use these SDKs, please refer to the following links

- [Projects SDK Reference](https://python.docs.gretel.ai/en/latest/projects/index.html)
