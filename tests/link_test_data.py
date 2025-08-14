# Makes test data available for the Python tests by symlinking the Ruby test data directory.
import os
import sys
import shutil

src = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../test/data'))
dst = os.path.join(os.path.dirname(__file__), 'data')

if not os.path.exists(dst):
    shutil.copytree(src, dst)
