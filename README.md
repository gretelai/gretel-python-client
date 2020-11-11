# Gretel Python Client
<p align="left">
<img width=15% src="https://gretel-public-website.s3.amazonaws.com/assets/gobs_the_cat_@1x.png" alt="Gobs the Gretel.ai cat" />
<i>An open source data transformation library and bindings to Gretel APIs</i>
</p>


![Run Test Suite](https://github.com/gretelai/gretel-python-client/workflows/Run%20Test%20Suite/badge.svg)
[![Documentation Status](https://readthedocs.org/projects/gretel-client/badge/?version=stable)](https://gretel-client.readthedocs.io/en/stable/?badge=stable?badge=stable)
![GitHub](https://img.shields.io/github/license/gretelai/gretel-python-client)
[![PyPI](https://badge.fury.io/py/gretel-client.svg)](https://badge.fury.io/py/gretel-client)
[![Python](https://img.shields.io/pypi/pyversions/gretel-client.svg)](https://github.com/gretelai/gretel-python-client)
[![Downloads](https://pepy.tech/badge/gretel-client)](https://pepy.tech/project/gretel-client)
[![GitHub stars](https://img.shields.io/github/stars/gretelai/gretel-python-client?style=social)](https://github.com/gretelai/gretel-python-client)

**`Documentation`** |
------------------- |
[![Documentation](https://img.shields.io/badge/api-reference-blue.svg)](https://gretel-client.readthedocs.io/en/stable/api_bindings/api_ref.html) Cloud API 
[![Documentation](https://img.shields.io/badge/api-reference-blue.svg)](https://gretel-client.readthedocs.io/en/stable/transformers/api_ref.html) Transformers SDK

## Try it out now!
If you want to quickly discover the Gretel client and Transformer libraries, simply click the buttons below and follow the tutorials!

* [![Open in Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/gretelai/gretel-blueprints/blob/master/gretel/gc-labeling_pub_sub_basic/blueprint.ipynb) Blueprint example to demonstrate NLP labeling and publish/subscribe

* [![Open in Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/gretelai/gretel-blueprints/blob/master/gretel/gc-auto_anonymize_dataset/blueprint.ipynb) Blueprint example to automatically anonymize a dataset

# Overview

The Gretel Python Client provides bindings to the Gretel REST API and a transformation sub-package that provides interfaces to manipulate data based on a variety of use cases.

The REST API bindings and transformer interfaces can be used separately or together to solve a variety of data analysis, anonymization, and other ETL use cases.

The Gretel REST API provides automated data labeling that generates a metadata record for every JSON record it receives. This combined record + metadata tuple can be fed directly into the transformer interfaces to help automate the transformation of data without needing to know what field certain data elements are in.

The transformer interfaces may be used with un-labeled data as well and operate directly on Python dictionaries if desired.

Please check out client.md or transformers.md for a quick start on these packages or check them out in our documentation.)
