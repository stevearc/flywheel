""" Setup file """
import sys

from setuptools import setup, find_packages

import os
import re


HERE = os.path.abspath(os.path.dirname(__file__))
README = open(os.path.join(HERE, 'README.rst')).read()
CHANGES = open(os.path.join(HERE, 'CHANGES.rst')).read()
# Remove custom RST extensions for pypi
CHANGES = re.sub(r'\(\s*:(issue|pr|sha):.*?\)', '', CHANGES)

REQUIREMENTS = [
    'dynamo3>=0.4.6',
    'six'
]

TEST_REQUIREMENTS = [
    'nose',
    'mock',
]

if sys.version_info[:2] < (2, 7):
    TEST_REQUIREMENTS.extend(['unittest2'])

if __name__ == "__main__":
    setup(
        name='flywheel',
        version='0.4.9',
        description="SQLAlchemy-style ORM for Amazon's DynamoDB",
        long_description=README + '\n\n' + CHANGES,
        classifiers=[
            'Development Status :: 4 - Beta',
            'Intended Audience :: Developers',
            'License :: OSI Approved :: MIT License',
            'Operating System :: OS Independent',
            'Programming Language :: Python',
            'Programming Language :: Python :: 2',
            'Programming Language :: Python :: 2.6',
            'Programming Language :: Python :: 2.7',
            'Programming Language :: Python :: 3',
            'Programming Language :: Python :: 3.3',
            'Programming Language :: Python :: 3.4',
            'Topic :: Database',
        ],
        author='Steven Arcangeli',
        author_email='stevearc@stevearc.com',
        url='http://flywheel.readthedocs.org/',
        license='MIT',
        keywords='aws dynamo dynamodb orm odm',
        platforms='any',
        include_package_data=True,
        packages=find_packages(exclude=('tests',)),
        install_requires=REQUIREMENTS,
        tests_require=REQUIREMENTS + TEST_REQUIREMENTS,
    )
