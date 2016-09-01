"""Setup file for the Cachier package."""

# This file is part of Cachier.
# https://github.com/shaypal5/cachier

# Licensed under the MIT license:
# http://www.opensource.org/licenses/MIT-license
# Copyright (c) 2016, Shay Palachy <shaypal5@gmail.com>

from setuptools import setup, find_packages
import versioneer

setup(
    name='Cachier',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description='Persistent, stale-free memoization decorators for Python.',
    license='MIT',
    author='Shay Palachy',
    author_email='shaypal5@gmail.com',
    url='https://github.com/shaypal5/cachier',
    packages=find_packages(),
    install_requires=[
        'pymongo',
        'watchdog'
    ],
    keywords=['cache', 'persistence', 'mongo', 'memoization', 'decorator'],
    classifiers=[],
)
