__author__ = 'abdul'

import os
import subprocess

import json
import time

from date_utils import (datetime_to_bson, is_date_value, seconds_now,
                        utc_str_to_dateime)
from bson import json_util

###############################################################################
##################################         ####################################
################################## Helpers ####################################
##################################         ####################################
###############################################################################

def document_pretty_string(document):
    return json.dumps(document, indent=4, default=_custom_json_default)

###############################################################################
def _custom_json_default(obj):
    if is_date_value(obj):
        return datetime_to_bson(obj)
    else:
        return json_util.default(obj)

###############################################################################
# sub-processing functions
###############################################################################
def call_command(command, bubble_exit_code=False, **popen_kwargs):
    try:
        return subprocess.check_call(command, **popen_kwargs)
    except subprocess.CalledProcessError, e:
        if bubble_exit_code:
            exit(e.returncode)
        else:
            raise e

###############################################################################
def execute_command(command, **popen_kwargs):
    # Python 2.7+ : Use the new method because i think its better
    if  hasattr(subprocess, 'check_output'):
        return subprocess.check_output(command,
            stderr=subprocess.STDOUT, **popen_kwargs)
    else: # Python 2.6 compatible, check_output is not available in 2.6
        return subprocess.Popen(command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            **popen_kwargs).communicate()[0]

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

###############################################################################
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
        object_hook=_custom_json_object_hook)

    if not json_val and not isinstance(json_val,list): # b/c [] is not True
        raise Exception("Unable to load %s config file: %s" %
                        (name, path))
    else:
        return json_val

###############################################################################
def _custom_json_object_hook(dct):
    if "$date" in dct:
        return utc_str_to_dateime(dct["$date"])
    else:
        return json_util.object_hook(dct)

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
def resolve_path(path):
    # handle file uris
    path = path.replace("file://", "")

    # expand vars
    path =  os.path.expandvars(os.path.expanduser(path))
    # Turn relative paths to absolute
    path = os.path.abspath(path)
    return path


###############################################################################
def wait_for(predicate, timeout=None, sleep_duration=2, log_func=None):
    start_time = seconds_now()

    def default_log_func():
        print("--waiting--")

    log_func = log_func or default_log_func
    while (timeout is None) or (seconds_now() - start_time < timeout):

        if predicate():
            return True
        else:
            log_func()
            time.sleep(sleep_duration)

    return False
