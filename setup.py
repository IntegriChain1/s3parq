from setuptools import setup, find_packages
from os import path

package_name = "s3parq"
package_version = "2.1.0"
description = "Write and read/query s3 parquet data using Athena/Spectrum/Hive style partitioning."
cur_directory = path.abspath(path.dirname(__file__))
with open(path.join(cur_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()


def get_reqs():
    reqs = []
    with open("requirements.txt") as file:
        for line in file:
            # remove linebreak which is the last character of the string
            currentReq = line[:-1]
            reqs.append(currentReq)
    return reqs

setup(
    name=package_name,
    version=package_version,
    description=description,
    long_description=long_description,
    long_description_content_type='text/markdown',
    author="Integrichain Innovation Team",
    author_email="engineering@integrichain.com",
    url="https://github.com/IntegriChain1/s3parq",
    license="MIT",
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7"
        ],
    packages=find_packages(exclude=("tests",)),
    include_package_data=True,
    install_requires=get_reqs()
    )
