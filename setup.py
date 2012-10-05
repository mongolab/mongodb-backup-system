from setuptools import setup

setup(
    name='mbs',
    version='0.1.0',
    install_requires=[
        "pymongo==2.3",
        "maker-py==0.1.0"
    ],
    dependency_links=[
        "https://github.com/objectlabs/maker-py/zipball/master#egg=makerpy-0.1.0"
    ]


)
