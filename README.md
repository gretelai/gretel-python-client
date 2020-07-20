# Gretel Python Client

![Run Test Suite](https://github.com/gretelai/gretel-python-client/workflows/Run%20Test%20Suite/badge.svg)

[![Documentation Status](https://readthedocs.org/projects/gretel-client/badge/?version=latest)](https://gretel-client.readthedocs.io/en/stable/?badge=stable)

The Gretel Python Client provides bindings to the Gretel REST API and a transformation sub-package that provides interfaces to manipulate data based on a variety of use cases.

The REST API bindings and transformer interfaces can be used separately or together to solve a variety of data analysis, anonymization, and other ETL use cases.  

The Gretel REST API provides automated data labeling that generates a metadata record for every JSON record it receives.  This combined record + metadata tuple can be fed directly into the transformer interfaces to help automate the transformation of data without needing to know what field certain data elements are in.

The transformer interfaces may be used with un-labeled data as well and operate directly on Python dictionaries if desired.

Please check out `client.md` or `transformers.md` for a quick start on these packages or check them out in [our documentation.]((https://gretel-client.readthedocs.io/en/stable/?badge=stable))