""" Setup file """
import os

from setuptools import find_packages, setup

HERE = os.path.abspath(os.path.dirname(__file__))
README = open(os.path.join(HERE, "README.rst")).read()
CHANGES = open(os.path.join(HERE, "CHANGES.rst")).read()

REQUIREMENTS = [
    "dynamo3>=1.0.0",
    "pyparsing==2.1.4",
    "python-dateutil",
    "botocore>=1.17.55",
    "rich>=6.1.1",
]

REQUIREMENTS_TEST = open(os.path.join(HERE, "requirements_test.txt")).readlines()

if __name__ == "__main__":
    setup(
        name="dql",
        version="0.6.1",
        description="DynamoDB Query Language",
        long_description=README + "\n\n" + CHANGES,
        classifiers=[
            "Development Status :: 4 - Beta",
            "Intended Audience :: Developers",
            "License :: OSI Approved :: MIT License",
            "Operating System :: OS Independent",
            "Programming Language :: Python",
            "Programming Language :: Python :: 3",
            "Programming Language :: Python :: 3.6",
            "Programming Language :: Python :: 3.7",
            "Programming Language :: Python :: 3.8",
        ],
        author="Steven Arcangeli",
        author_email="stevearc@stevearc.com",
        url="http://github.com/stevearc/dql",
        keywords="aws dynamo dynamodb sql",
        license="MIT",
        platforms="any",
        include_package_data=True,
        packages=find_packages(exclude=("tests",)),
        python_requires=">=3.6",
        entry_points={"console_scripts": ["dql = dql:main"]},
        install_requires=REQUIREMENTS,
        tests_require=REQUIREMENTS + REQUIREMENTS_TEST,
    )
