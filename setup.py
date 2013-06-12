import os
import pwd
import sys

from urlparse import urlparse

from distribute_setup import use_setuptools
use_setuptools()

from setuptools import setup, find_packages

# NOTE: http://bugs.python.org/issue15881#msg170215
try:
    import multiprocessing
except ImportError:
    pass


###############################################################################
def parse_archive(line):
    parts = urlparse(line)
    if len(parts.fragment) == 0:
        raise ValueError('no egg specified')
    if parts.fragment.count('-') > 1:
        raise ValueError('hyphens in package names should be replaced with '
                         'underscores')
    return line, parts.fragment.split('=')[1].replace('-', '==')


###############################################################################
def requirements():
    inst_reqs = []
    test_reqs = []
    dep_links = []
    reqs      = inst_reqs
    setup_dir = os.path.dirname(os.path.abspath(__file__))
    for line in open(os.path.join(setup_dir, 'requirements.txt')):
        line = line.strip()
        if len(line) == 0:
            continue
        elif line.startswith('#'):
            if 'core' in line.lower():
                reqs = inst_reqs
            elif 'test' in line.lower():
                reqs = test_reqs
            continue
        if line.startswith('-e'):
            raise ValueError('editable mode not supported')
        elif '://' in line:
            link, req = parse_archive(line)
            dep_links.append(link)
            reqs.append(req)
        else:
            reqs.append(line)
    return {'links': dep_links, 'core': inst_reqs, 'test': test_reqs}


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


setup(
    name='mbs',
    version='0.1.2',
    scripts=[
        'bin/mbs',
        'bin/st'
    ],
    packages=find_packages(),
    install_requires=requirements()['core'],
    tests_require=requirements()['test'],
    dependency_links=requirements()['links'],
    test_suite='nose.collector'
)


### execute this block after setup "install" command is complete
if "install" in sys.argv:
    try:
        create_default_config()
    except Exception, e:
        print ("WARNING: Error while attempting to create default config."
               "Please create it manually.")
