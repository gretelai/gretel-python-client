from setuptools import setup, find_packages
from os import path

this_dir = path.abspath(path.dirname(__file__))

with open(path.join(this_dir, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()


with open(path.join(this_dir, 'VERSION')) as f:
    version = f.read()


setup(
    name='gretel-client',
    version=version,
    use_scm_version=True,
    setup_requires=['setuptools_scm'],
    description='Python bindings for the Gretel API',
    long_description=long_description,
    long_description_content_type='text/markdown',
    entry_points={
        'console_scripts': [
            'gretel=gretel_client.cli:main',
        ],
    },
    package_dir={'': 'src'},
    packages=find_packages('src'),
    install_requires=[
        'requests',
        'smart_open==1.10.0'
    ],
    extras_require={
        'pandas': ['pandas==1.0.3'],
        'viz': ['ipywidgets==7.5.1', 'tqdm==4.45.0']
    }
)
