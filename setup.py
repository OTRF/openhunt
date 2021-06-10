#!/usr/bin/env python

# Author: Jose Rodriguez (@Cyb3rPandaH)
# License: GNU General Public License v3 (GPLv3)

import setuptools

with open('README.md')as f:
    long_description = f.read()

setuptools.setup (
	name = 'openhunt',
	version = '1.7.8',
	author = 'Jose Rodriguez @Cyb3rPandaH',
	description = 'A Python library to expedite the analysis of data during hunting engagements',
	long_description=long_description,
	long_description_content_type="text/markdown",
	url="https://github.com/OTRF/openhunt",
	keywords="threat hunting python pandas jupyter notebook",
	packages=setuptools.find_packages(),
	install_requires=[
        'pandas',
		'altair',
		'seaborn',
		'matplotlib',
		'pyspark',
		'plotly',
		'networkx'
    	],
	license='GNU General Public License v3 (GPLv3)',
	classifiers=[
        'Development Status :: 4 - Beta',
        'Operating System :: OS Independent',
        'Topic :: Security',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
		'Programming Language :: Python :: 3.7',
    ]
)
