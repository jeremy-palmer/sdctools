from setuptools import setup, find_packages

setup(
    name='sdctools',
    version='0.1',
    packages=find_packages(exclude=['tests*']),
    license='MIT',
    description='File utilities for SDC data',
    long_description=open('README.txt').read(),
    install_requires=['tarfile'
        ,'csv'
        ,'dateutil'
        ,'boto3'
        ,'botocore'
        ,'datetime'
        ,'random'
        ,'tempfile'],
    url='https://',
    author='Jeremy Palmer',
    author_email='jeremy.palmer@chorus.co.nz'
)
