from setuptools import find_packages, setup

setup(
    name='src',
    packages=find_packages(),
    version='0.1.0',
    description='Parsing the curriculos lattes em formato XML para gestão de conhecimento.',
    author='Luciano Barosi/CP/PRPG/UFCG',
    license='MIT',
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 3 - Alpha',
    ],
    project_urls={
    'Documentation': 'https://github.com/lbarosi/pylattesLXML',
    'Source': 'https://github.com/pypa/sampleproject/',
    'Tracker': 'https://github.com/pypa/sampleproject/issues',
    'Funding': 'http://prpg.ufcg.edu.br',
    },
    python_requires='>=3.6',
)
