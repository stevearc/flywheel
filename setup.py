""" Setup file """
import os
import re
import sys

from setuptools import find_packages, setup

HERE = os.path.abspath(os.path.dirname(__file__))
README = open(os.path.join(HERE, 'README.rst')).read()
# Remove syntax highlighting for pypi
README = re.sub(r'.. sourcecode.*', '::', README)
CHANGES = open(os.path.join(HERE, 'CHANGES.rst')).read()
# Remove custom RST extensions for pypi
CHANGES = re.sub(r'\(\s*:(issue|pr|sha):.*?\)', '', CHANGES)

REQUIREMENTS = [
    'dynamo3>=0.4.9,<1.0',
    'six'
]

TEST_REQUIREMENTS = [
    'nose',
    'mock',
]

if __name__ == "__main__":
    setup(
        name='flywheel',
        version='0.5.4',
        description="SQLAlchemy-style ORM for Amazon's DynamoDB",
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
