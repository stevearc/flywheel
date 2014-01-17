""" Setup file """
import os
import sys

from setuptools import setup, find_packages
from flywheel_version import git_version, UpdateVersion


HERE = os.path.abspath(os.path.dirname(__file__))
README = open(os.path.join(HERE, 'README.rst')).read()
CHANGES = open(os.path.join(HERE, 'CHANGES.rst')).read()

REQUIREMENTS = [
    'boto>=2.23.0',
]

TEST_REQUIREMENTS = [
    'nose',
    'mock',
    'httpretty',
    'moto',
]

if sys.version_info[:2] < (2, 7):
    TEST_REQUIREMENTS.extend(['unittest2'])

if __name__ == "__main__":
    setup(
        name='flywheel',
        version=git_version('flywheel'),
        description="Object mapper for Amazon's DynamoDB",
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
            'Topic :: Database',
        ],
        author='Steven Arcangeli',
        author_email='steven@highlig.ht',
        url='http://flywheel.readthedocs.org/',
        keywords='aws dynamo dynamodb orm odm',
        include_package_data=True,
        packages=find_packages(exclude=('tests',)),
        cmdclass={'update_version': UpdateVersion},
        entry_points={
            'nose.plugins': [
                'dynamolocal=flywheel.tests:DynamoLocalPlugin',
            ],
        },
        install_requires=REQUIREMENTS,
        tests_require=REQUIREMENTS + TEST_REQUIREMENTS,
    )
