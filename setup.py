#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name="hrpc",
    version='0.1.0',
    url="http://github.com/cloudera/python-hrpc/tree/master",
    maintainer="Todd Lipcon",
    maintainer_email="todd@cloudera.com",
    packages=['hrpc'],
    install_requires = ['setuptools'],
)
