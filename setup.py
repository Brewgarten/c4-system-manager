"""
Copyright (c) IBM 2015-2017. All Rights Reserved.
Project name: c4-system-manager
This project is licensed under the MIT License, see LICENSE
"""
import sys

from setuptools import setup, find_packages

import versioneer


needs_pytest = {"pytest", "test", "ptr", "coverage"}.intersection(sys.argv)
pytest_runner = ["pytest-runner"] if needs_pytest else []

setup(
    name = "c4-systemmanager",
    version = versioneer.get_version(),
    cmdclass = versioneer.get_cmdclass(),
    packages = find_packages(),
    install_requires = ["c4-utils", "c4-messaging"],
    setup_requires=[] + pytest_runner,
    tests_require=["pytest", "pytest-cov"],
    author = "IBM",
    author_email = "",
    description = "This library provides the system management framework for project C4",
    license = "MIT",
    keywords = "python c4 sm",
    url = "",
    entry_points = {
        "console_scripts" : [
            "c4-systemmanager = c4.system.manager:main"
        ]
    },
    package_data = {
        "c4.data": [
            "sql/*.sql",
            "config/*.json"
        ],
    }
)
