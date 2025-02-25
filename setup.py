from os import path
from pathlib import Path

from setuptools import find_packages, setup

this_dir = path.abspath(path.dirname(__file__))

with open(path.join(this_dir, "README.md"), encoding="utf-8") as f:
    long_description = f.read()


def reqs(file_path):
    with open(Path(file_path)) as fh:
        return [
            r.strip()
            for r in fh.readlines()
            if not (r.startswith("#") or r.startswith("\n"))
        ]


aws_deps = ["boto3", "smart_open[s3]"]
gcp_deps = ["smart_open[gcs]", "google-cloud-kms"]
azure_deps = ["smart_open[azure]", "azure-identity", "azure-keyvault"]

setup(
    name="gretel-client",
    author="Gretel Labs, Inc.",
    author_email="support@gretel.ai",
    use_scm_version=True,
    setup_requires=["setuptools_scm==8.1.0"],
    description="Balance, anonymize, and share your data. With privacy guarantees.",
    url="https://github.com/gretelai/gretel-python-client",
    long_description=long_description,
    long_description_content_type="text/markdown",
    package_dir={"": "src"},
    packages=find_packages("src"),
    python_requires=">=3.9",
    entry_points={"console_scripts": ["gretel=gretel_client.cli.cli:cli"]},
    install_requires=reqs("requirements/base.txt"),
    tests_require=reqs("requirements/test.txt"),
    extras_require={
        "aws": aws_deps,
        "gcp": gcp_deps,
        "azure": azure_deps,
        "tuner": reqs("requirements/tuner.txt"),
        "test": reqs("requirements/test.txt"),
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: POSIX :: Linux",
        "Operating System :: MacOS",
        "Topic :: Scientific/Engineering :: Artificial Intelligence",
    ],
)
