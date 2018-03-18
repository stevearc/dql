""" Setup file """
import os

from setuptools import setup, find_packages


HERE = os.path.abspath(os.path.dirname(__file__))
README = open(os.path.join(HERE, 'README.rst')).read()
CHANGES = open(os.path.join(HERE, 'CHANGES.rst')).read()

REQUIREMENTS = [
    'dynamo3>=0.4.7',
    'future>=0.15.0',
    'pyparsing==2.1.4',
    'python-dateutil<2.7.0',
]

TEST_REQUIREMENTS = [
    'nose',
    'mock',
]

if __name__ == "__main__":
    setup(
        name='dql',
        version='0.5.26',
        description='DynamoDB Query Language',
        long_description=README + '\n\n' + CHANGES,
        classifiers=[
            'Development Status :: 4 - Beta',
            'Intended Audience :: Developers',
            'License :: OSI Approved :: MIT License',
            'Operating System :: OS Independent',
            'Programming Language :: Python',
            'Programming Language :: Python :: 2',
            'Programming Language :: Python :: 2.7',
            'Programming Language :: Python :: 3',
            'Programming Language :: Python :: 3.4',
            'Programming Language :: Python :: 3.5',
            'Programming Language :: Python :: 3.6',
        ],
        author='Steven Arcangeli',
        author_email='stevearc@stevearc.com',
        url='http://github.com/stevearc/dql',
        keywords='aws dynamo dynamodb sql',
        license='MIT',
        platforms='any',
        include_package_data=True,
        packages=find_packages(exclude=('tests',)),
        entry_points={
            'console_scripts': [
                'dql = dql:main',
            ],
        },
        install_requires=REQUIREMENTS,
        tests_require=REQUIREMENTS + TEST_REQUIREMENTS,
    )
