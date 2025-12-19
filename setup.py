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
    version="1.0.13",
    author="Vasiliy Zdanovskiy",
    author_email="vasilyvz@gmail.com",
    description="Full-featured job queue system with multiprocessing support for Linux",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/vasilyvz/queuemgr",
    project_urls={
        "Bug Reports": "https://github.com/vasilyvz/queuemgr/issues",
        "Source": "https://github.com/vasilyvz/queuemgr",
        "Documentation": "https://github.com/vasilyvz/queuemgr#readme",
    },
    packages=find_packages(),
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Intended Audience :: System Administrators",
        "License :: OSI Approved :: MIT License",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: System :: Distributed Computing",
        "Topic :: System :: Systems Administration",
        "Topic :: Internet :: WWW/HTTP :: Dynamic Content",
        "Topic :: Scientific/Engineering",
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
        "web": [
            "flask>=2.0.0",
        ],
        "examples": [
            "requests>=2.25.0",
        ],
    },
    entry_points={
        "console_scripts": [
            (
                "queuemgr-daemon="
                "queuemgr.examples.integration_examples.systemd_integration:main"
            ),
            (
                "queuemgr-cli="
                "queuemgr.examples.integration_examples.cli_integration:main"
            ),
            (
                "queuemgr-web="
                "queuemgr.examples.integration_examples.flask_integration:main"
            ),
        ],
    },
)
