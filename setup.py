from setuptools import setup, find_packages

# get version without importing
versionFileName = "c4/system/__init__.py"
packageVersion = None

try:
    import imp
    import os
    currentDirectory = os.path.dirname(os.path.abspath(__file__))
    baseDirectory = os.path.dirname(currentDirectory)
    versioningModulePath = os.path.join(baseDirectory, "versioning.py")
    setup = imp.load_source("versioning", versioningModulePath).versionedSetup
except:
    import re
    with open(versionFileName) as f:
        match = re.search("__version__\s*=\s*['\"](.*)['\"]", f.read())
        packageVersion = match.group(1)

setup(
    name = "c4-systemmanager",
    version = packageVersion,
    versionFileName = versionFileName,
    packages = find_packages(),
    install_requires = ['c4-utils', 'c4-messaging'],
    author = 'IBM',
    author_email = '',
    description = '''This library provides the system management framework for project C4''',
    license = 'IBM',
    keywords = 'python c4 sm',
    url = '',
    entry_points = {
        'console_scripts' : [ 'c4-systemmanager = c4.system.manager:main',
                              'dashDB-sm = c4.system.cli:main',
                              'c4-system-cli = c4.system.cli:main' ]
    },
    package_data = {
        "c4.data": ["sql/*.sql", "ssl/ssl.*", "web/*"],
    }
)
