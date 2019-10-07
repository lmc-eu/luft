# -*- coding: utf-8 -*-
"""Setup."""
import os
from pathlib import Path

from setuptools import Command, find_packages, setup

ROOT = Path.cwd()
VERSION_PATH = ROOT / 'VERSION'
README_PATH = ROOT / 'README.md'


# Get the Version from the VERSION file
if VERSION_PATH.exists():
    version = VERSION_PATH.read_text()
else:
    tmp_version = os.popen('git describe --tags --always').read().strip()
    try:
        base, _distance, commit_hash = tmp_version.split('-')
    except ValueError:
        pass
    version = '{}+{}'.format(base, commit_hash)

if not version:
    raise RuntimeError('Version was not detected!')

# Get the long description from the README.md file
if README_PATH.exists():
    with open(README_PATH) as f:
        long_description = f.read()

# REQUIREMENTS
install_requires = []
extras_require = {
    'dev': [],
    'bq': ['google-cloud-bigquery==1.18.0'],
    'qlik-cloud': ['selenium==3.141.0'],
    'qlik-metric': ['boto3==1.9.242', 'websocket_client==0.56.0'],
}

# requirements file
reqs = 'requirements.txt'

with open(reqs) as f:
    for line in f.read().splitlines():
        line = line.strip()
        if not line.startswith('#'):
            parts = line.strip().split(';')
            if len(parts) > 1:
                print('Warning: requirements line "{}" ignored, as it uses env markers,'
                      'which are not supported in setuptools'.format(line))
            else:
                install_requires.append(parts)

with open('requirements-dev.txt') as f:
    for line in f.read().splitlines():
        extras_require['dev'].append(line.strip())


class CleanCommand(Command):
    """Command to tidy up the project root.

    Registered as cmdclass in setup() so it can be called with ``python setup.py extra_clean``.
    """

    description = 'Tidy up the project root'
    user_options = []

    def initialize_options(self):
        """Set default values for options."""

    def finalize_options(self):
        """Set final values for options."""

    def run(self):
        """Run command to remove temporary files and directories."""
        os.system('rm -vrf ./build ./dist ./*.pyc ./*.tgz ./*.egg-info')


def do_setup():
    """Perform the Luft package setup."""
    setup(
        name='luft',
        version=version,
        description='Luft is an interactive client (cli)'
        'that help you with common BI tasks (loading, historization, etc.).',
        long_description=long_description,
        long_description_content_type='text/markdown',
        license='',

        author='BI Team @ LMC, s.r.o.',
        author_email='info@lmc.eu',
        maintainer='Radek Tomšej',
        maintainer_email='radek.tomsej@lmc.eu',

        classifiers=[
            'Development Status :: 3 - Alpha',
            'Intended Audience :: Developers',
            'License :: OSI Approved :: MIT License',
            'Operating System :: MacOS :: MacOS X',
            'Operating System :: Microsoft :: Windows',
            'Operating System :: POSIX',
            'Programming Language :: Python :: 3',
            'Programming Language :: Python :: 3.6',
            'Programming Language :: Python :: 3.7',
            'Programming Language :: Python',
            'Topic :: Software Development :: Libraries',
        ],
        keywords=['cli', 'client', 'bi', 'generator',
                  'yaml', 'airflow', 'luft', 'lmc'],

        packages=find_packages(exclude=['tests*', 'docs*']),
        python_requires='>=3.6, <4',
        include_package_data=True,

        entry_points={
            'console_scripts': [
                'luft=cli.luft:luft',
            ]
        },

        install_requires=install_requires,
        extras_require=extras_require,
    )


if __name__ == '__main__':
    do_setup()
