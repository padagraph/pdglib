#!/usr/bin/env python

from setuptools import setup, find_packages

#TODO; better setup
# see https://bitbucket.org/mchaput/whoosh/src/999cd5fb0d110ca955fab8377d358e98ba426527/setup.py?at=default
# for ex

# Read requirements from txt file
with open('requirements.txt') as f:
    required = f.read().splitlines()


setup(
    name='padagraphlib',
    version='1.0',
    description='padagraph graphlib',
    author='ynnk, a-tsioh',
    author_email='contact@padagraph.io',
    url='www.padagraph.io',
    packages=['pdglib'] + ['pdglib.%s' % submod for submod in find_packages('pdglib')],
    install_requires=required,
)
