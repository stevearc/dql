""" Setup file """
import os
import sys

from setuptools import setup, find_packages
from dql_version import git_version, UpdateVersion


HERE = os.path.abspath(os.path.dirname(__file__))
README = open(os.path.join(HERE, 'README.rst')).read()
CHANGES = open(os.path.join(HERE, 'CHANGES.rst')).read()

REQUIREMENTS = [
    'boto>=2.23.0',
    'pyparsing',
]

TEST_REQUIREMENTS = [
    'nose',
    'mock',
]

if sys.version_info[:2] < (2, 7):
    REQUIREMENTS.extend(['ordereddict', 'argparse'])
    TEST_REQUIREMENTS.append('unittest2')

if __name__ == "__main__":
    setup(
        name='dql',
        version=git_version('dql'),
        cmdclass={'update_version': UpdateVersion},
        description='DynamoDB Query Language',
        long_description=README + '\n\n' + CHANGES,
        classifiers=[
            'Intended Audience :: Developers',
            'License :: OSI Approved :: MIT License',
            'Programming Language :: Python',
            'Programming Language :: Python :: 2',
            'Programming Language :: Python :: 2.6',
            'Programming Language :: Python :: 2.7',
        ],
        author='Steven Arcangeli',
        author_email='steven@highlig.ht',
        url='http://github.com/mathcamp/dql',
        keywords='aws dynamo dynamodb sql',
        include_package_data=True,
        packages=find_packages(exclude=('tests',)),
        entry_points={
            'nose.plugins': [
                'dynamolocal=tests:DynamoLocalPlugin',
            ],
            'console_scripts': [
                'dql = dql:main',
            ],
        },
        install_requires=REQUIREMENTS,
        tests_require=REQUIREMENTS + TEST_REQUIREMENTS,
    )
