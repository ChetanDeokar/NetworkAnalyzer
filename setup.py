import os

from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))
README = open(os.path.join(here, 'README')).read()
CHANGES = open(os.path.join(here, 'CHANGES')).read()

requires = [
    'setuptools',
    'wheel',
    'pyspark',
    'pandas',
    'unittest2'
    ]

setup(name='nw_analyzer',
      version='0.0',
      description='nw_analyzer',
      long_description=README + '\n\n' + CHANGES,
      classifiers=[
        "Programming Language :: Python",
        "Framework :: PySpark",
        ],
      author='',
      author_email='',
      url='',
      keywords='pyspark network analyzer',
      packages=find_packages(),
      include_package_data=True,
      zip_safe=False,
      test_suite='nw_analyzer',
      install_requires=requires,
      )
