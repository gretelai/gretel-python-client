import os
import sys


def setup_notebook():
    """
    This helper function sets up the correct paths for notebooks
    to run locally installed gretel-transformers code
    """
    notebook_dir = os.getcwd()
    src_dir = os.path.realpath(os.path.join(notebook_dir, '../..', 'src'))
    if src_dir not in sys.path and notebook_dir.endswith('pub'):
        sys.path.append(src_dir)
