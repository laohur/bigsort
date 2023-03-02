# -*- coding: utf-8 -*-

from setuptools import setup, find_packages
from os import path
import os


here = os.path.abspath(os.path.dirname(__file__))

with open(path.join(here, 'readme.md')) as f:
    long_description = f.read()


setup(
    name="bigsort",
    # packages=find_packages(),
    py_modules=['bigsort'],
    version='0.0.5',
    description='sort big file or streams',
    long_description=long_description,
    long_description_content_type="text/markdown",
    python_requires='>=3.0',
    install_requires=[
        "psutil",
        "logzero"
    ],

    entry_points={
        "console_scripts": ["bigsort=bigsort:main"]
    },

    url='https://github.com/laohur/bigsort',
    keywords=['bigsort', 'sort', "external sort", "big file sort"],
    author='laohur',
    author_email='laohur@gmail.com',
    license='[Anti-996 License](https: // github.com/996icu/996.ICU/blob/master/LICENSE)',
)

"""
python setup.py sdist
python setup.py bdist_wheel
twine upload dist/*

"""
