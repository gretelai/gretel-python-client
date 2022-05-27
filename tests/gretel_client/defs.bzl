"""
Gretel client testing helpers and definitions
"""

load("//bzl/python:defs.bzl", "py_pytest_test", "requirement")

test_deps = [
    requirement("certifi"),
    requirement("faker"),
    requirement("pandas"),
    requirement("smart_open"),
    "//python/tests/gretel_client:conf",
]

client_deps = [
    "//python/src/gretel_client:config",
    "//python/src/gretel_client:projects",
    "//python/src/gretel_client:readers",
    "//python/src/gretel_client:cli",
    "//python/src/gretel_client:helpers",
    "//python/src/gretel_client:agents",
]

all_deps = test_deps + client_deps

def py_client_integration_test(name, flaky = True, **kwargs):
    deps = kwargs.get("deps", [])
    py_pytest_test(
        name = name,
        deps = deps + all_deps + ["//python/tests/gretel_client/integration:conf"],
        flaky = flaky,
        **kwargs
    )
