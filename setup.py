#!/usr/bin/env python3
# encoding: utf-8
"""
Package configuration for ch-tools.
"""

from setuptools import find_packages, setup

with open('requirements.txt') as f:
    REQUIREMENTS = list(map(lambda l: l.strip('\n'), f.readlines()))

with open('version.txt') as f:
    VERSION = f.read().strip()

setup(
    name='ch-tools',
    version=VERSION,
    description='A set of tools for administration and diagnostics of ClickHouse DBMS.',
    license='MIT',
    url='https://github.com/yandex/ch-tools',
    author='Yandex LLC',
    author_email='opensource@yandex-team.ru',
    zip_safe=False,
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Intended Audience :: System Administrators",
        "License :: OSI Approved :: MIT License",
        "Operating System :: MacOS",
        "Operating System :: POSIX :: BSD",
        "Operating System :: POSIX :: Linux",
        "Operating System :: Unix",
        "Environment :: Console",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Topic :: Database",
        "Typing :: Typed",
    ],
    platforms=['Linux', 'BSD', 'MacOS'],
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    package_data={'': ['version.txt']},
    entry_points={
        'console_scripts': [
            'chadmin = chtools.chadmin.chadmin_cli:main',
            'ch-monitoring = chtools.monrun_checks.main:main',
            'keeper-monitoring = chtools.monrun_checks_keeper.main:main',
            'ch-s3-credentials = chtools.s3_credentials.main:main',
        ]
    },
    install_requires=REQUIREMENTS,
)
