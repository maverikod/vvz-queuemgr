"""
Setup script for the queue manager system.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="queuemgr",
    version="0.1.0",
    author="Vasiliy Zdanovskiy",
    author_email="vasilyvz@gmail.com",
    description="A Python-based job queue system with per-job processes and signal variables",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/vasilyvz/queuemgr",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: System :: Distributed Computing",
    ],
    python_requires=">=3.10",
    install_requires=[
        # No external dependencies for core functionality
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0",
            "black>=23.0.0",
            "flake8>=6.0.0",
            "mypy>=1.0.0",
        ],
        "examples": [
            "uuid>=1.30",
        ],
    },
    entry_points={
        "console_scripts": [
            "queuemgr-example=queuemgr.examples.simple_job:main",
        ],
    },
)
