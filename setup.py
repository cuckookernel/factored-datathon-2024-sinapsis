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
    install_requires=["beautifulsoup4==4.12.3",
                      "boto3==1.35.4",
                      "botocore==1.35.4",
                      "brotlipy==0.7.0",
                      "click==8.1.7", # CLI construction
                      "dataset==1.6.2",
                      "deflate==0.7.0",
                      "pandas==2.2.2",
                      "pydantic==2.8.2",
                      "python-dotenv==1.0.1",
                      "requests==2.31.0",
                      "rich==13.7.1",
                      ],

    classifiers=[
        'Development Status :: 1 - Planning',
        'Intended Audience :: Science/Research',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python :: 3.10',
    ],
)
