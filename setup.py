#! /usr/bin/env python
#
from datasink.version import version
import os

srcdir = os.path.dirname(__file__)

try:
    from setuptools import setup

except ImportError:
    from distutils.core import setup

long_description = """
datasink is data transfer software.
"""

setup(
    name = "datasink",
    version = version,
    author = "Software Division, OCS Team",
    author_email = "ocs@naoj.org",
    description = ("Data Transfer Software for Subaru Telescope."),
    long_description = long_description,
    license = "BSD",
    keywords = "data transfer subaru telescope",
    url = "http://naojsoft.github.com/datasink",
    packages = ['datasink'],
    package_data = { },
    scripts = ['scripts/ds_client', 'scripts/ds_worker', 'scripts/ds_sleep',
               'scripts/ds_file', 'scripts/datasink'],
    install_requires = ['pika>=1.1.0', 'pyyaml>=5.3.1'],
    classifiers=[
          'Intended Audience :: Science/Research',
          'License :: OSI Approved :: BSD License',
          'Operating System :: MacOS :: MacOS X',
          'Operating System :: Microsoft :: Windows',
          'Operating System :: POSIX',
          'Programming Language :: Python :: 3.5',
          'Topic :: Scientific/Engineering :: Astronomy',
          'Topic :: Scientific/Engineering :: Physics',
          ],
)
