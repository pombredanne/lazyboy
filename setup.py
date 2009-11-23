# -*- coding: utf-8 -*-
#
# Lazyboy: Setup
#
# © 2009 Digg, Inc. All rights reserved.
# Author: Ian Eure <ian@digg.com>
#

from setuptools import setup, find_packages
import os
import glob

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
DEPS = glob.glob(os.path.dirname(os.path.abspath(__file__)) + "/deps/*")
DEPS.append("http://github.com/ieure/python-cassandra/downloads")

setup(name="Lazyboy",
      version='0.7.4',
      description="Object non-relational manager for Cassandra",
      url="http://github.com/digg/lazyboy/tree/master",
      packages=find_packages(),
      include_package_data=True,
      author="Ian Eure",
      author_email="ian@digg.com",
      license="Three-clause BSD",
      keywords="database cassandra",
      install_requires=['Thrift', 'Cassandra>=0.4.0'],
      zip_safe=False,
      tests_require=['nose', 'coverage>=3.2b1'],
      dependency_links=DEPS)
