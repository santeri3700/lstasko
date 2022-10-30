#
# Copyright (C) 2022 Santeri Pikarinen <santeri3700>
#
# This file is part of lstasko.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License version 2 published by
# the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
# See the GNU General Public License version 2 for more details.
#
# You should have received a copy of the GNU General Public License version 2
# along with this program; if not, see <http://www.gnu.org/licenses/>.

import setuptools

setuptools.setup(
    name='lstasko',
    version='0.1.0',
    description='List Taskomatic tasks',
    url="https://github.com/santeri3700/lstasko",
    author='Santeri Pikarinen',
    author_email='santeri.pikarinen@gmail.com',
    python_requires='>=3.6',
    packages=setuptools.find_packages(),
    entry_points={'console_scripts': ['lstasko=lstasko.__main__:main']},
    include_package_data=True,
    install_requires=[
        "psycopg2-binary==2.9.5",
        "dataclasses==0.8.0",
        "tabulate==0.9.0",
        "colorlog==6.7.0"
    ],
    classifiers=[
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.10",
        "Natural Language :: English",
        "Development Status :: 2 - Pre-Alpha",
        "License :: OSI Approved :: GNU General Public License v2 (GPLv2)",
        "Operating System :: POSIX :: Linux",
        "Intended Audience :: System Administrators",
        "Topic :: System :: Systems Administration",
        "Topic :: Utilities"
    ]
)
