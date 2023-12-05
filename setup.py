from distutils.core import setup
from setuptools import find_packages
import pathlib
import pkg_resources
import os
import sys

name = 'pubmed_tool'
# Program was built with Python 3.10, has not been tested on otherv versions
min_py_version = (3,10)

rootdir = os.path.abspath(os.path.dirname(__file__))

if sys.version_info[:2] < min_py_version:
    sys.stderr.write(
        ("ERROR: Pubmed_tool requires Python %i.%i or later. " % min_py_version)
        + ("Python %d.%d detected.\n" % sys.version_info[:2])
    )
    sys.exit(1)

# User-friendly description from README.md
current_directory = os.path.dirname(os.path.abspath(__file__))
try:
    with open(os.path.join(current_directory, 'README.md'), encoding='utf-8') as f:
        long_description = f.read()
except Exception:
    long_description = ''

# Extract requirements from requirements.txt
with pathlib.Path('requirements.txt').open() as requirements_txt:
    required_packages = [
        str(requirement)
        for requirement
        in pkg_resources.parse_requirements(requirements_txt)
    ]
    
# Scripts
script_list = []
for dirname, dirnames, filenames in os.walk('scripts'):
    for filename in filenames:
        if not filename.endswith('.bat'):
            script_list.append(os.path.join(dirname, filename))


setup(
	# Name of the package 
	name=name,
	# Packages to include into the distribution 
	packages=find_packages(),
	version='1.0.0',
    #https://help.github.com/articles/licensing-a-repository
	license='MIT',
	# Short description of your library 
	description='A small PubMed Scraper with SQL and Visual capability',
	# Long description of your library 
	long_description=long_description,
	long_description_content_type='text/markdown',
	# Your name 
	author='Morrigan Mahady, Heather Jones, Keelei Perrington, Isabela Baker',
	# Your email 
	author_email='morrigan.mahady@uth.tmc.edu',
	# Either the link to your github or to your website 
	url=r'https://github.com/intro-to-ds-capstone/capstone-project',
	# Link from which the project can be downloaded 
	download_url=r'https://github.com/intro-to-ds-capstone/capstone-project',
    
	# List of packages to install with this one 
	install_requires = required_packages,
    python_requires=">=%i.%i" % min_py_version,
    # https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
          'Development Status :: 2 - Pre-Alpha',
          'Programming Language :: Python :: 3.10'
      ],
      scripts = script_list
)
