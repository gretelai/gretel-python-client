from setuptools import setup, find_packages
from os import path

this_dir = path.abspath(path.dirname(__file__))

with open(path.join(this_dir, "README.md"), encoding="utf-8") as f:
    long_description = f.read()


setup(
    name="gretel-client",
    use_scm_version=True,
    setup_requires=["setuptools_scm"],
    description="Python bindings for the Gretel API",
    long_description=long_description,
    long_description_content_type="text/markdown",
    entry_points={"console_scripts": ["gretel=gretel_client.cli:main"]},
    package_dir={"": "src"},
    packages=find_packages("src"),
    install_requires=[
        "faker==4.1.1",
        "requests",
        "smart_open==2.1.0",
        "tqdm==4.45.0",
        "tenacity==6.2.0",
        'dataclasses;python_version<"3.7"'
    ],
    extras_require={
        "pandas": ["pandas>1.0.0,<1.1.0"],
        "fpe": ["numpy", "pycryptodome==3.9.8", "dateparser==0.7.6"]
    },
)
