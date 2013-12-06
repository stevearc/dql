""" Setup file """
import os

from setuptools import setup, find_packages
from version_helper import git_version


HERE = os.path.abspath(os.path.dirname(__file__))
README = open(os.path.join(HERE, 'README.rst')).read()
CHANGES = open(os.path.join(HERE, 'CHANGES.rst')).read()

REQUIREMENTS = [
    'boto',
    'nose',
    'pyparsing',
]

if __name__ == "__main__":
    setup(
        name='dql',
        description='DynamoDB Query Language',
        long_description=README + '\n\n' + CHANGES,
        classifiers=[
            'Programming Language :: Python',
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
