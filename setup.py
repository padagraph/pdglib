#!/usr/bin/env python

from setuptools import setup, find_packages

print("*** setup pdglib ** ** ")

with open('requirements.txt') as f:
    required = f.read().splitlines()



setup(
    name='pdglib',
    version='1.0.2',
    description='padagraph graphlib',
    author='ynnk, a-tsioh',
    author_email='contact@padagraph.io',
    url='www.padagraph.io',
    packages=['pdglib'] + ['pdglib.%s' % submod for submod in find_packages('pdglib')],
    install_requires=required,
)
