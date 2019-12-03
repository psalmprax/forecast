import os
from setuptools import setup, find_packages

setup(
    # Application name:
    name="COMPREDICT-AI-SDK",

    # Version number (initial):
    version="0.0.1",

    # Application author details:
    author="Ousama Esbel",
    author_email="esbel@compredict.de",

    # Packages
    packages=find_packages(),

    # Include additional files into the package
    include_package_data=True,

    # Details
    url="https://github.com/compredict/ai-sdk-python",

    license="MIT",
    description="Connect Python applications with COMPREDICT AI Core.",
    keywords=["COMPREDICT", "AI", "SDK", "API", "rest"],

    long_description=open(os.path.join(os.path.dirname(__file__), "README.md")).read(),

    # Dependent packages (distributions)
    install_requires=[
        "pycrypto>=1.4.0",
        "requests>=2.1.0",
    ],
)