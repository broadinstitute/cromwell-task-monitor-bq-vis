from io import open

from setuptools import find_packages, setup

with open('requirements.txt', 'r') as fh:
    to_be_installed = [l.rstrip('\n') for l in fh.readlines()]

with open('test-requirements.txt', 'r') as fh:
    test_dev_install = [l.rstrip('\n') for l in fh.readlines()]

version = "0.0.1"
setup(
    name="cromwellMonitor",
    version=version,
    description="A python library used to query and visualize the recorded data by "
                "the cromwel-task-monitor-bq tool",
    url="https://github.com/broadinstitute/cromwell-task-monitor-bq-vis",
    author="Beri Shifaw",
    author_email="bshifaw@broadinstitute.org",
    license="BSD 3-Clause",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",

    python_requires=">=3.7",
    install_requires=to_be_installed,
    tests_require=test_dev_install,

    packages=find_packages(
        where="src",
        include=['cromwellMonitor*']
    ),
    package_dir={"": "src"},
    include_package_data=True,

    classifiers=[
        "Development Status :: 1 - preAlpha",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: BSD 3-Clause",
        "Natural Language :: English",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
)