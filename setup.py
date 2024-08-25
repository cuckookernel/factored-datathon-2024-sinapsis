"""Turn this repo into installable package"""
from setuptools import setup  # type: ignore [import-untyped]

setup(
    name='data_proc',
    version='0.1.0',
    description='Data Processing for Team Sinapsis',
    url='https://github.com/cuckookernel/factored-datathon-2024-sinapsis',
    author='Team Sinapsis',
    author_email='mr324@cornell.edu',
    packages=['data_proc', "shared"],
    install_requires=[],

    classifiers=[
        'Development Status :: 1 - Planning',
        'Intended Audience :: Science/Research',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python :: 3.10',
    ],
)
