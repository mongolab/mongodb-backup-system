import sys
import os
import pwd

from distribute_setup import use_setuptools
use_setuptools()

from setuptools import setup, find_packages

setup(
    name='mbs',
    version='0.1.2',
    scripts=[
        'bin/mbs',
        'bin/st'
    ],
    packages=find_packages(),
    install_requires=[
        "distribute",
        "dargparse",
        "pymongo==2.4.1",
        "maker-py==0.1.2",
        'boto==2.6.0',
        'Flask==0.8',
        'python-dateutil==1.5',
        'python-cloudfiles==1.7.10',
        'azure==0.6',
        'verlib==0.1'
    ],
    dependency_links=[
        "git+ssh://git@github.com/objectlabs/maker-py.git#egg=maker-py-0.1.2"
    ]


)

###############################################################################
def create_default_config():
    from mbs.config import MBS_CONF_DIR, MBS_CONFIG

    mbs_conf = os.path.expanduser(os.path.join(MBS_CONF_DIR, MBS_CONFIG))

    # do nothing if conf already exists
    print "Checking if configuration '%s' exists..." % mbs_conf
    if os.path.exists(mbs_conf):
        print "Config '%s' already exists" % mbs_conf
        return

    print "Configuration '%s' does not exist. Creating default..." % mbs_conf

    login = os.getlogin()
    conf_dir = os.path.dirname(mbs_conf)
    owner = pwd.getpwnam(login)
    owner_uid = owner[2]
    owner_gid = owner[3]

    # if the conf dir does not exist then create it and change owner
    # This is needs so when pip install is run with sudo then the owner
    # should be logged in user instead of root
    if not os.path.exists(conf_dir):
        os.makedirs(conf_dir)
        os.chown(conf_dir, owner_uid, owner_gid)
        os.chmod(conf_dir, 00755)

    default_conf = {
        "databaseURI": "YOUR DATABASE URI",
        "engines":[
                {
                "_type": "BackupEngine",
                "_id": "DEFAULT",
                "maxWorkers": 10,
                "tempDir": "~/backup_temp",
                "commandPort": 8888,
                "tags": None
            }
        ]
    }

    from mbs.utils import document_pretty_string

    conf_file = open(mbs_conf, mode="w")
    conf_file.write(document_pretty_string(default_conf))
    # chown conf file
    os.chown(mbs_conf, owner_uid, owner_gid)
    os.chmod(mbs_conf, 00644)

    print "Successfully created configuration '%s'!" % mbs_conf

### execute this block after setup "install" command is complete
if "install" in sys.argv:
    try:
        create_default_config()
    except Exception, e:
        print ("WARNING: Error while attempting to create default config."
               "Please create it manually.")
