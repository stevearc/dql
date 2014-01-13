""" Setup file """
import os
import sys

from setuptools import setup, find_packages
from dql_version import git_version


HERE = os.path.abspath(os.path.dirname(__file__))
README = open(os.path.join(HERE, 'README.rst')).read()
CHANGES = open(os.path.join(HERE, 'CHANGES.rst')).read()

REQUIREMENTS = [
    'boto>=2.20.1',
    'pyparsing',
    'nose',
    'mock',
]

if sys.version_info[:2] < (2, 7):
    REQUIREMENTS.extend(['ordereddict', 'argparse', 'unittest2'])

if __name__ == "__main__":
    setup(
        name='dql',
        description='DynamoDB Query Language',
        long_description=README + '\n\n' + CHANGES,
        classifiers=[
            'Programming Language :: Python',
            'Programming Language :: Python :: 2.6',
            'Programming Language :: Python :: 2.7',
            'Intended Audience :: Developers',
            'License :: OSI Approved :: MIT License',
        ],
        author='Steven Arcangeli',
        author_email='steven@highlig.ht',
        url='http://github.com/mathcamp/dql',
        zip_safe=False,
        include_package_data=True,
        packages=find_packages(),
        entry_points={
            'nose.plugins': [
                'dynamolocal=dql.tests:DynamoLocalPlugin',
            ],
            'console_scripts': [
                'dql = dql:main',
            ],
        },
        install_requires=REQUIREMENTS,
        tests_require=REQUIREMENTS,
        **git_version()
    )
