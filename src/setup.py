"""A setuptools based setup module.
See:
https://packaging.python.org/en/latest/distributing.html
https://github.com/pypa/sampleproject
"""

# Always prefer setuptools over distutils
from setuptools import setup
from setuptools import find_packages
from setuptools.command.develop import develop as _develop
from setuptools.command.install import install as _install
from setuptools.command.egg_info import egg_info as _egg_info

# To use a consistent encoding
import os
import sys
from glob import glob
import logging

# TODO
from distutils.core import setup
from codecs import open
from os import path


here = path.abspath(path.dirname(__file__))

packages = ["geopackage"]

# Get the long description from the README file
try:
    with open(path.join(here, "README.md"), encoding="utf-8") as f:
        long_decription = f.read()
except:
    long_decription = "Python GeoPackage Package"

from setuptools import setup


def read_file(file):
    with open(file, "rb") as fh:
        data = fh.read()
    return data.decode("utf-8")


setup(
    name="geopackage",
    version="1.0.0",
    description="Pure Python reader/writer of geopackages",
    long_description=read_file("README.md"),
    long_description_content_type="text/markdown",
    author="Andrew Chapkowski",
    author_email="andrewonboe@gmail.com",
    url="https://github.com/achapkowski/pygeopackage",
    py_modules=["geopackage"],
    packages=packages,
    license="Apache License 2.0",
    keywords="gis, geospatial, geographic, geopackage, ogc, wkb, wkt, geojson, spatial, Esri, ArcGIS, Python, ArcPy, qgis",
    python_requires=">= 2.7",
    classifiers=[
        "Topic :: Scientific/Engineering :: GIS",
        "Topic :: Software Development :: Libraries",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Software Development :: Esri REST API",
        "Intended Audience :: Developers/GIS Users",
        "License :: Apache License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.2",
        "Programming Language :: Python :: 3.3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Development Status :: 5 - Production/Stable",
    ],
    include_package_data=True,
    zip_safe=False,
    install_requires=["pandas", "geomet"],
    extras_require={},
    package_data={"geopackage": ["prj.json"]},
)
