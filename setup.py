#!/usr/bin/env python

# Author: Jose Rodriguez (@Cyb3rPandaH)
# License: GNU General Public License v3 (GPLv3)

import setuptools

with open('README.md')as f:
    long_description = f.read()

setuptools.setup (
	name = 'openhunt',
	version = '1.0',
	author = 'Jose Rodriguez',
	author_email = 'cyb3rpandah@gmail.com',
	description = 'Python scripts to improve hunting campaigns',
	long_description=long_description,
	long_description_content_type="text/markdown",
	url="https://github.com/Cyb3rPanda/openhunt",
	keywords="threat hunting python pandas jupyter notebook",
	packages=setuptools.find_packages(),
	install_requires=[
        'pandas',
    	],
	license='GNU General Public License v3 (GPLv3)',
	classifiers=[
        'Development Status :: 4 - Beta',
        'Operating System :: OS Independent',
        'Topic :: Security',
        'OSI Approved :: GNU General Public License v3 (GPLv3)',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ]
)
