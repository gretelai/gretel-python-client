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

## Documentation
|Component|Links|
|-----|----|
|Cloud API|[API Reference](https://gretel-client.readthedocs.io/en/stable/api_bindings/api_ref.html)|
|Transformers SDK|[API Reference](https://gretel-client.readthedocs.io/en/stable/transformers/api_ref.html)|

## Try it out now!
If you want to quickly discover the Gretel client and Transformer libraries, simply click the buttons below and follow the tutorials!

<a href="https://colab.research.google.com/github/gretelai/gretel-python-client/blob/master/notebooks/simple_pub_sub.ipynb"><img alt="Open in Colab" src="https://colab.research.google.com/assets/colab-badge.svg"></a> Blueprint example to demonstrate NLP labeling and publish/subscribe via the Gretel cloud.

<a href="https://colab.research.google.com/github/gretelai/gretel-python-client/blob/master/notebooks/simple_constant_fakes.ipynb"><img alt="Open in Colab" src="https://colab.research.google.com/assets/colab-badge.svg"></a> Blueprint example to demonstrate deterministically replacing field values with fake entities.
# Overview

The Gretel Python Client provides bindings to the Gretel REST API and a transformation sub-package that provides interfaces to manipulate data based on a variety of use cases.

The REST API bindings and transformer interfaces can be used separately or together to solve a variety of data analysis, anonymization, and other ETL use cases.

The Gretel REST API provides automated data labeling that generates a metadata record for every JSON record it receives. This combined record + metadata tuple can be fed directly into the transformer interfaces to help automate the transformation of data without needing to know what field certain data elements are in.

The transformer interfaces may be used with un-labeled data as well and operate directly on Python dictionaries if desired.

Please check out client.md or transformers.md for a quick start on these packages or check them out in our documentation.)
