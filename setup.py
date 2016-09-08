# -*- coding: utf-8 -*-

from setuptools import setup, find_packages


with open('README') as f:
    readme = f.read()

with open('LICENSE') as f:
    license = f.read()

setup(
    name='hfttools',
    version='0.0.1',
    description='A package for high-frequency trade research.',
    long_description=readme,
    author='Colin Swaney',
    author_email='colin-swaney@uiowa.edu',
    url='https://github.com/colinswaney/hfttools',
    license=license,
    packages=find_packages(exclude=('tests', 'docs'))
)
