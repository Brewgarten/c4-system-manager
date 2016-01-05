from setuptools import setup, find_packages

import versioneer


setup(
    name = "c4-systemmanager",
    version = versioneer.get_version(),
    cmdclass = versioneer.get_cmdclass(),
    packages = find_packages(),
    install_requires = ["c4-utils", "c4-messaging"],
    author = "IBM",
    author_email = "",
    description = "This library provides the system management framework for project C4",
    license = "IBM",
    keywords = "python c4 sm",
    url = "",
    entry_points = {
        "console_scripts" : [
            "c4-systemmanager = c4.system.manager:main"
        ]
    },
    package_data = {
        "c4.data": [
            "sql/*.sql"
        ],
    }
)
