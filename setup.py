from setuptools import setup, find_packages
from os import path

this_dir = path.abspath(path.dirname(__file__))

with open(path.join(this_dir, "README.md"), encoding="utf-8") as f:
    long_description = f.read()


setup(
    name="gretel-client",
    author='Gretel Labs, Inc.',
    author_email='open-source@gretel.ai',
    use_scm_version=True,
    setup_requires=["setuptools_scm"],
    description="Python bindings for the Gretel API",
    url='https://github.com/gretelai/gretel-python-client',
    long_description=long_description,
    long_description_content_type="text/markdown",
    entry_points={"console_scripts": ["gretel=gretel_client.cli:main"]},
    package_dir={"": "src"},
    packages=find_packages("src"),
    install_requires=[
        "faker==4.1.1",
        "requests>=2.24.0,<3",
        "smart_open>=2.1.0,<3",
        "tqdm==4.45.0",
        "tenacity==6.2.0",
        'dataclasses;python_version<"3.7"'
    ],
    extras_require={
        "pandas": ["pandas>1.1.0,<1.2"],
        "fpe": ["numpy", "pycryptodome==3.9.8", "dateparser==0.7.6"]
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: POSIX :: Linux",
        "Operating System :: MacOS",
        "Operating System :: Microsoft :: Windows"
    ]
)
