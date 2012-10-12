__author__ = 'abdul'

import os
import subprocess

import json
import time

import pymongo

from bson import json_util
from datetime import datetime, timedelta
from pymongo import uri_parser, errors

###############################################################################
##################################         ####################################
################################## Helpers ####################################
##################################         ####################################
###############################################################################

def document_pretty_string(document):
    return json.dumps(document, indent=4, default=json_util.default)

###############################################################################
def date_now():
    return datetime.now()

###############################################################################
def seconds_now():
    return date_to_seconds(date_now())

###############################################################################
def date_to_seconds(date):
    return time.mktime(date.timetuple())

###############################################################################
def epoch_date():
    return datetime(1970, 1, 1)

###############################################################################
def seconds_to_date(seconds):
    return datetime.fromtimestamp(seconds)

###############################################################################
def date_plus_seconds(date, seconds):
    return seconds_to_date(date_to_seconds(date) + seconds)

###############################################################################
def timestamp_to_str(timestamp):
    return timestamp.strftime('%Y-%m-%d %H:%M:%S')

###############################################################################
def date_to_str(date):
    return date.strftime('%m/%d/%Y')

###############################################################################
def timestamp_to_dir_str(timestamp):
    return timestamp.strftime('%Y_%m_%d__%H_%M_%S')

###############################################################################
def yesterday_date():
    return today_date() - timedelta(days=1)

###############################################################################
def today_date():
    return date_now().replace(hour=0, minute=0, second=0, microsecond=0)

###############################################################################
# sub-processing functions
###############################################################################
def execute_command(command, call=False, cwd=None):
    if call:
        return subprocess.check_call(command, cwd=cwd)
    # Python 2.7+ : Use the new method because i think its better
    elif  hasattr(subprocess, 'check_output'):
        return subprocess.check_output(command,
            stderr=subprocess.STDOUT,
            cwd=cwd)
    else: # Python 2.6 compatible, check_output is not available in 2.6
        return subprocess.Popen(command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            cwd=cwd).communicate()[0]

###############################################################################
def log_info(msg):
    print >>sys.stderr, msg

###############################################################################
def which(program):

    fpath, fname = os.path.split(program)
    if fpath:
        if is_exe(program):
            return program
    else:
        for path in os.environ["PATH"].split(os.pathsep):
            exe_file = os.path.join(path, program)
            if is_exe(exe_file):
                return exe_file
    return None


def is_exe(fpath):
    return os.path.isfile(fpath) and os.access(fpath, os.X_OK)

###############################################################################
def ensure_dir(dir_path):
    """
    If DIR_PATH does not exist, makes it. Failing that, raises Exception.
    Returns True if dir already existed; False if it had to be made.
    """
    exists = dir_exists(dir_path)
    if not exists:
        try:
            os.makedirs(dir_path)
        except(Exception,RuntimeError), e:
            raise Exception("Unable to create directory %s. Cause %s" %
                            (dir_path, e))
    return exists

###############################################################################
def dir_exists(path):
    return os.path.exists(path) and os.path.isdir(path)


###############################################################################
def read_config_json(name, path):
    json_str = read_json_string(path)
    # minify the json/remove comments and sh*t
    #json_str = minify_json.json_minify(json_str)
    json_val =json.loads(json_str,
        object_hook=json_util.object_hook)

    if not json_val and not isinstance(json_val,list): # b/c [] is not True
        raise Exception("Unable to load %s config file: %s" %
                        (name, path))
    else:
        return json_val

###############################################################################
def read_json_string(path, validate_exists=True):

    path = resolve_path(path)
    # if the path is just filename then append config root

    # check if its a file
    if os.path.isfile(path):
        return open(path).read()
    elif validate_exists:
        raise Exception("Config file %s does not exist." %
                                path)
    else:
        return None



###############################################################################
def mongo_connect(uri):
    try:
        dbname = pymongo.uri_parser.parse_uri(uri)['database']
        return pymongo.Connection(uri)[dbname]
    except Exception, e:
        raise Exception("Could not establish a database connection to "
                        "%s: %s" % (uri, e))


###############################################################################
def is_cluster_mongo_uri(mongo_uri):
    return len(parse_mongo_uri(mongo_uri)["nodelist"]) > 1

###############################################################################
def parse_mongo_uri(uri):
    try:
        uri_obj = uri_parser.parse_uri(uri)
        # validate uri
        nodes = uri_obj["nodelist"]
        for node in nodes:
            host = node[0]
            if not host:
                raise Exception("URI '%s' is missing a host." % uri)

        return uri_obj
    except errors.ConfigurationError, e:
        raise Exception("Malformed URI '%s'. %s" % (uri, e))

    except Exception, e:
        raise Exception("Unable to parse mongo uri '%s'."
                                " Cause: %s" % (e, uri))

###############################################################################
def resolve_path(path):
    # handle file uris
    path = path.replace("file://", "")

    # expand vars
    path =  os.path.expandvars(os.path.expanduser(path))
    # Turn relative paths to absolute
    path = os.path.abspath(path)
    return path
