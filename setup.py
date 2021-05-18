from setuptools import setup, find_packages

setup(name='piAwareIOT',
    version='0.1',
    description='Handle signal data from a piAware device',
    url='#',
    author='Eric Yager',
    install_requires=[
        'boto3',
        'awsiotsdk',
    ],
    author_email='',
    packages=find_packages(include=['awsiot*']),
    zip_safe=False\
)