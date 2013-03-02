__author__ = 'abdul'

import os
import subprocess
import socket
import pwd

import json
import time

from date_utils import (datetime_to_bson, is_date_value, seconds_now,
                        utc_str_to_datetime)
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
def listify(object):
    if isinstance(object, list):
        return object

    return [object]

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
            raise

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
        return utc_str_to_datetime(dct["$date"])
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
    path =  os.path.expandvars(custom_expanduser(path))
    # Turn relative paths to absolute
    path = os.path.abspath(path)
    return path

###############################################################################
def custom_expanduser(path):
    if path.startswith("~"):
        login = get_current_login()
        home_dir = os.path.expanduser( "~%s" % login)
        path = path.replace("~", home_dir, 1)

    return path

###############################################################################
def get_current_login():
    try:
        pwuid = pwd.getpwuid(os.geteuid())
        return pwuid.pw_name
    except Exception, e:
        raise Exception("Error while trying to get current os login. %s" % e)

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
###############################################################################
def get_local_host_name():
    return socket.gethostname()

###############################################################################
def is_host_local(host):
    if (host == "localhost" or
        host == "127.0.0.1"):
        return True

    return is_same_host(socket.gethostname(), host)

###############################################################################
def is_same_host(host1, host2):

    """
    Returns true if host1 == host2 OR map to the same host (using DNS)
    """

    if host1 == host2:
        return True
    else:
        ips1 = get_host_ips(host1)
        ips2 = get_host_ips(host2)
        return len(set(ips1) & set(ips2)) > 0


###############################################################################
def get_host_ips(host):
    try:

        ips = []
        addr_info = socket.getaddrinfo(host, None)
        for elem in addr_info:
            ip = elem[4]
            if ip not in ips:
                ips.append(ip)

        # TODO remove this temp hack that works around the case where
        # host X has more IPs than X.foo.com.
        if len(host.split(".")) == 3:
            ips.extend(get_host_ips(host.split(".")[0]))
        return ips
    except Exception, e:
        raise Exception("Invalid host '%s'. Cause: %s" % (host, e))
