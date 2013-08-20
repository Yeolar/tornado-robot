#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Created on 2013-04-12.  Copyright (C) Yeolar <yeolar@gmail.com>
#

from setuptools import setup, find_packages


setup(
    name='Torobot',
    version='0.0.1',
    description='Torobot is a crawler framework based on Tornado.',
    long_description=open('README.md').read().split('\n\n', 1)[1],
    author='Yeolar',
    author_email='yeolar@gmail.com',
    url='http://www.yeolar.com',
    packages=find_packages(),
    install_requires=[
        'tornado>=2.3',
    ],
    entry_points={
    },
)
