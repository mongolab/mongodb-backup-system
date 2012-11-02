from setuptools import setup

setup(
    name='mbs',
    version='0.1.0',
    scripts=['bin/mbs'],
    packages=['mbs'],
    install_requires=[
        "dargparse",
        "pymongo==2.3",
        "maker-py==0.1.0",
        'boto==2.6.0',
        'Flask==0.8',
        'python-dateutil==1.5'
    ],
    dependency_links=[
        "git+ssh://git@github.com/objectlabs/maker-py.git#egg=maker-py-0.1.0"
    ]


)
