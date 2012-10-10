from setuptools import setup

setup(
    name='mbs',
    version='0.1.0',
    scripts=['bin/mongoctl'],
    packages=['mbs'],
    install_requires=[
        "dargparse",
        "pymongo==2.3",
        "maker-py==0.1.0"
    ],
    dependency_links=[
        "git+ssh://git@github.com/objectlabs/maker-py.git#egg=maker-py-0.1.0"
    ]


)
