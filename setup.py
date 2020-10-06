#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name='target-bigquery',
      version='1.4.0',
      description='Singer.io target for writing data to Google BigQuery',
      author='RealSelf Business Intelligence',
      url='https://github.com/RealSelf/target-bigquery',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['target_bigquery'],
      setup_requires=['wheel'],
      install_requires=[
          'jsonschema==2.6.0',
          'singer-python>=1.5.0',
          'google-api-python-client>=1.12.3',
          'google-cloud>=0.34.0',
          'google-cloud-bigquery>=1.24.0',
          'google-auth',
          'google-api-core[grpc]>=1.22.3',
          'pytz==2018.4',
          'setuptools>=40.3.0'
      ],
      entry_points='''
          [console_scripts]
          target-bigquery=target_bigquery:main
      ''',
      packages=find_packages()
)
