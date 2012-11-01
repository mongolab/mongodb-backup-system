__author__ = 'abdul'

import os
import subprocess

import json
import time



from bson import json_util
from datetime import datetime, timedelta, date

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
def is_date_value(value):
    return type(value) in [datetime, date]

###############################################################################
def timedelta_total_seconds(td):
    """
    Equivalent python 2.7+ timedelta.total_seconds()
     This was added for python 2.6 compatibilty
    """
    return (td.microseconds + (td.seconds + td.days * 24 * 3600) * 1e6) / 1e6

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
