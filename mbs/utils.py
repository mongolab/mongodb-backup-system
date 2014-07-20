__author__ = 'abdul'

import os
import subprocess
import socket
import pwd
import traceback

import json
import time
import select
import sys
import platform
import signal
import psutil
import string
import random
from date_utils import (datetime_to_bson, is_date_value, seconds_now,
                        utc_str_to_datetime)
from bson import json_util
from distutils.version import StrictVersion

###############################################################################
##################################         ####################################
################################## Helpers ####################################
##################################         ####################################
###############################################################################
def document_pretty_string(document):
    return json.dumps(document, indent=4, default=_custom_json_default)

###############################################################################
def export_mbs_object_list(obj_list, display_only=False):
    return map(lambda o: o.to_document(display_only=display_only), obj_list)

###############################################################################
def mbs_object_list_to_string(obj_list):

    return document_pretty_string(export_mbs_object_list(obj_list,
                                                         display_only=True))

###############################################################################
def dict_to_str(d):
    return '\n'.join("%s: %s" % (key, val)
                     for (key, val) in d.iteritems())

###############################################################################
def _custom_json_default(obj):
    if is_date_value(obj):
        return datetime_to_bson(obj)
    else:
        return json_util.default(obj)

###############################################################################
def listify(object):
    if not object:
        return None

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
def execute_command_wrapper(command, on_output=None, output_path=None,
                            output_line_filter=None, **popen_kwargs):
    """
        Executes the specified command and allows processing/filtering output
        of command
    """
    process = subprocess.Popen(command, stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE, **popen_kwargs)

    output_file = None
    if output_path:
        output_file = open(output_path, 'w')

    while True:
        reads = [process.stdout.fileno(), process.stderr.fileno()]
        ret = select.select(reads, [], [])

        for fd in ret[0]:
            if fd == process.stdout.fileno():
                line = process.stdout.readline()

            elif fd == process.stderr.fileno():
                line = process.stderr.readline()

            if line:
                if on_output:
                    on_output(line.rstrip())
            if output_file:
                line = output_line_filter(line) if output_line_filter else line
                output_file.write(line)
                # flush the output
                output_file.flush()
        if process.poll() is not None:
            break

    return process.returncode

###############################################################################
def force_kill_process_and_children(pid):
    print "Killing process %s and child processes" % pid
    process = psutil.Process(pid=pid)
    children = process.get_children()
    if children:
        for child in children:
            print "Killing child process %s" % child.pid
            force_kill_process_and_children(child.pid)
    else:
        print "Process %s has no children" % pid
    process.kill()


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
    json_val = parse_json(json_str)

    if not json_val and not isinstance(json_val,list): # b/c [] is not True
        raise Exception("Unable to load %s config file: %s" %
                        (name, path))
    else:
        return json_val

###############################################################################
def parse_json(json_str):
    # minify the json/remove comments and sh*t
    #json_str = minify_json.json_minify(json_str)
    return json.loads(json_str, object_hook=_custom_json_object_hook)


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
def resolve_function(full_func_name):
    names = full_func_name.split(".")
    module_name = names[0]
    module_obj = sys.modules[module_name]
    result = module_obj
    for name in names[1:]:
        result = getattr(result, name)

    return result

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
        host_info = socket.gethostbyname_ex(host)
        aliases = host_info[1]
        ips = host_info[2]

        for alias in aliases:
            if alias != host:
                try:
                    ips.extend(get_host_ips(alias))
                except Exception, ex:
                    pass

        # TODO remove this temp hack that works around the case where
        # host X has more IPs than X.foo.com.
        if len(host.split(".")) == 3:
            try:
                ips.extend(get_host_ips(host.split(".")[0]))
            except Exception, ex:
                pass

        return ips
    except Exception, e:
        raise Exception("Invalid host '%s'. Cause: %s" % (host, e))

###############################################################################
def find_mount_point(path):
    """
        Returns the volume that the specified path is under
    """
    path = os.path.abspath(path)
    while not os.path.ismount(path):
        path = os.path.dirname(path)
    return path

###############################################################################
# FS FREEZE SUPPORT
###############################################################################
def freeze_mount_point(mount_point):
    """
        Freezes the specified mount point using fsfreeze
        NOTE: This requires that current login can sudo fsfreeze without
        needing as password.
    """
    validate_fsfreeze()
    freeze_cmd = [
        "sudo",
        "-n",
        get_fsfreeze_exe(),
        "-f",
        mount_point
    ]

    print "Executing freeze command: %s" % freeze_cmd
    execute_command(freeze_cmd)
    print "Freeze command: %s\n executed successfully!" % freeze_cmd

###############################################################################
def unfreeze_mount_point(mount_point):
    """
        Unfreezes the specified mount point using fsfreeze.
        NOTE: This requires that current login can sudo fsfreeze without
        needing as password.
    """
    validate_fsfreeze()
    unfreeze_cmd = [
        "sudo",
        "-n",
        get_fsfreeze_exe(),
        "-u",
        mount_point
    ]
    print "Executing unfreeze command: %s" % unfreeze_cmd
    execute_command(unfreeze_cmd)
    print "Unfreeze command: %s\n executed successfully!" % unfreeze_cmd

###############################################################################
def validate_fsfreeze():
    if not os_supports_freeze():
        distribution = platform.dist()
        dist_name = distribution[0].lower()
        dist_version = distribution[1]
        err = ("Your OS (dist='%s', version='%s') is not fsfreeze compatible."
               " fsfreeze requires Ubunto>= 12.04 or debian and an fsfreeze "
               "exe available in your "
               "path." % (dist_name, dist_version))
        raise Exception(err)
    # validate fsfreeze against sudo
    validate_fsfreeze_sudo()

###############################################################################
def get_fsfreeze_exe():
    return which("fsfreeze")

###############################################################################
def os_supports_freeze():
    """
        Returns true if the current os supports fsfreeze that is os running
        Ubuntu 12.04 or later and there is an fsfreeze exe in PATH
    """
    try:
        return (io_suspend_supported_os() and
                get_fsfreeze_exe() is not None)

    except Exception, e:
        print "Error while trying to check if OS supports fsfreeze: %s" % e
        traceback.print_exc()
        return False

###############################################################################
def validate_fsfreeze_sudo():
    """
        Ensures that fsfreeze could be run with sudo without needing a password
    """
    try:
        cmd = [
            "sudo",
            "-n",
            get_fsfreeze_exe(),
            "--help"
        ]
        execute_command(cmd)
        # we are ok here
    except subprocess.CalledProcessError, e:
        msg = ("Error while validating fsfreeze. Your sudoers config does not "
               "support running 'sudo fsfreeze' without passing a password. "
               "Please tag your fsfreeze with a NOPASSWD tag")
        raise Exception(msg)


###############################################################################
# DM SETUP SUPPORT
###############################################################################
def suspend_lvm_mount_point(mount_point):
    """
        Suspends the specified mount point using dmsetup
        NOTE: This requires that current login can sudo dmsetup without
        needing as password.
    """
    validate_dmsetup()
    freeze_cmd = [
        "sudo",
        "-n",
        get_dmsetup_exe(),
        "suspend",
        get_mount_point_device(mount_point)
    ]

    print "Executing dmsetup suspend command: %s" % freeze_cmd
    execute_command(freeze_cmd)
    print "Suspend command: %s\n executed successfully!" % freeze_cmd

###############################################################################
def resume_lvm_mount_point(mount_point):
    """
        Unfreezes the specified mount point using dmsetup.
        NOTE: This requires that current login can sudo dmsetup without
        needing as password.
    """
    validate_dmsetup()
    unfreeze_cmd = [
        "sudo",
        "-n",
        get_dmsetup_exe(),
        "resume",
        get_mount_point_device(mount_point)
    ]
    print "Executing dmsetup resume command: %s" % unfreeze_cmd
    execute_command(unfreeze_cmd)
    print "Resume command: %s\n executed successfully!" % unfreeze_cmd

###############################################################################
def validate_dmsetup():
    if not os_supports_dmsetup():
        distribution = platform.dist()
        dist_name = distribution[0].lower()
        dist_version = distribution[1]
        err = ("Your OS (dist='%s', version='%s') is not dmsetup compatible. "
               "dmsetup requires Ubunto >= 12.04 or debian and a dmsetup exe "
               "available in your path." % (dist_name, dist_version))
        raise Exception(err)
    # validate dmsetup against sudo
    validate_dmsetup_sudo()

###############################################################################
def get_dmsetup_exe():
    return which("dmsetup")

###############################################################################
def os_supports_dmsetup():
    """
        Returns true if the current os supports dmsetup that is os running
        Ubuntu 12.04 or later and there is an dmsetup exe in PATH
    """
    try:
        return (io_suspend_supported_os() and
                get_dmsetup_exe() is not None)
    except Exception, e:
        print "Error while trying to check if OS supports dmsetup: %s" % e
        traceback.print_exc()
        return False


###############################################################################
def validate_dmsetup_sudo():
    """
        Ensures that dmsetup could be run with sudo without needing a password
    """
    try:
        cmd = [
            "sudo",
            "-n",
            get_dmsetup_exe(),
            "--help"
        ]
        execute_command(cmd)
        # we are ok here
    except subprocess.CalledProcessError, e:
        msg = ("Error while validating dmsetup. Your sudoers config does not "
               "support running 'sudo dmsetup' without passing a password. "
               "Please tag your dmsetup with a NOPASSWD tag")
        raise Exception(msg)

###############################################################################
def get_mount_point_device(mount_point):
    print "Finding device for mount point %s " % mount_point
    if not os.path.ismount(mount_point):
        raise RuntimeError("Not a valid mount point %s" % mount_point)

    mnt_cmd = ['mount', '-l']
    print "Executing command %s" % mnt_cmd
    mounts_output = execute_command(mnt_cmd)
    print "Output for command %s\n\n%s" % (mnt_cmd, mounts_output)
    for line in mounts_output.split('\n'):
        if mount_point in line:
            device = line.split(' ')[0]
            print "Found device %s for mount point %s" % (device, mount_point)
            return device

    raise RuntimeError("No device found for mount point %s" % mount_point)


###############################################################################
def io_suspend_supported_os():
    """
        Returns true if the current os supports io suspend
    """

    distribution = platform.dist()
    dist_name = distribution[0].lower()
    dist_version_str = distribution[1]
    if dist_name and dist_version_str:
        dist_version = StrictVersion(dist_version_str)
        min_version = StrictVersion('12.04')

        return ((dist_name == "ubuntu" and dist_version >= min_version) or
                dist_name == "debian")
    else:
        return False

###############################################################################
# SignalWatcher
###############################################################################
class SignalWatcher(object):
    """
        Watches signals :)
    """
    ###########################################################################
    def __init__(self, sig_handlers, on_exit=None, wait=False, poll_time=1):
        self._sig_handlers = sig_handlers
        self._on_exit = [] if on_exit is None else on_exit
        self._sig_received = None
        self._wait = wait
        self._poll_time = poll_time

        def _handler(sig, frame):
            self._sig_received = sig
        for sig in self._sig_handlers:
            signal.signal(sig, _handler)

    ###########################################################################
    @property
    def signaled(self):
        return self._sig_received is not None

    ###########################################################################
    def __enter__(self):
        return self

    ###########################################################################
    def watch(self):
        while self._wait and not self.signaled:
            time.sleep(self._poll_time)
        if self.signaled:
            self._sig_handlers[self._sig_received]()

    ###########################################################################
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.watch()
        for f in self._on_exit:
            f()



###############################################################################
# Args validation
###############################################################################
def get_validate_arg(args, key, expected_type=None, required=True):
    expected_type = tuple(listify(expected_type))
    if required and (not args or not args.get(key)):
        raise ValueError("'%s' is required" % key)

    if (args and args.get(key) and expected_type and
            not isinstance(args[key], expected_type)):
        raise ValueError(" Invalid '%s' type (%s). Must be of type '%s'" %
        (key, type(args[key]), expected_type))

    if args and key in args:
        return args[key]
    else:
        return None


###############################################################################
# Prompt helpers
###############################################################################
def prompt_execute_task(message, task_function):

    yes = prompt_confirm(message)
    if yes:
        return True, task_function()
    else:
        return False, None

###############################################################################
def prompt_confirm(message):
    valid_choices = {"yes":True,
                     "y":True,
                     "ye":True,
                     "no":False,
                     "n":False}

    while True:
        print >> sys.stderr, message + " [y/n] ",
        sys.stderr.flush()
        choice = raw_input().lower()
        if not valid_choices.has_key(choice):
            print >> sys.stderr, ("Please respond with 'yes' or 'no' "
                                  "(or 'y' or 'n').\n")
        elif valid_choices[choice]:
            return True
        else:
            return False


###############################################################################
# String formatting
###############################################################################
class SafeFormatter(string.Formatter):
    def get_field(self, field_name, args, kwargs):
        first, rest = field_name._formatter_field_name_split()

        if first not in kwargs:
            return "{%s}" % field_name, field_name
        else:
            return super(SafeFormatter, self).get_field(field_name,
                                                        args, kwargs)

###############################################################################
def safe_format(template, **kwargs):
    return SafeFormatter().format(template, **kwargs)

###############################################################################
def random_string(n=10):
    return ''.join(random.choice(string.ascii_uppercase + string.digits)
                   for _ in range(n))



