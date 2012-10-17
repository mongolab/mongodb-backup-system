from setuptools import setup

setup(
    name='mbs',
    version='0.1.0',
    scripts=['bin/mongo-backup-main'],
    packages=['mbs'],
    install_requires=[
        "dargparse",
        "pymongo==2.3",
        "maker-py==0.1.0",
        'boto==2.6.0',
    ],
    dependency_links=[
        "git+ssh://git@github.com/objectlabs/maker-py.git#egg=maker-py-0.1.0"
    ]


)
