#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name='launch',
      version='0.1',
      description='Python Distributed launcher',
      author='kuizhiqing',
      author_email='kuizhiqing@fake.com',
      url='https://github.com/Artway-ai/launch',
      packages=find_packages(),
      entry_points={
            'console_scripts': [
                'launch = launch.main:launch'
            ]
        },
     )
