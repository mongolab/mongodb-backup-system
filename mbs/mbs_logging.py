__author__ = 'abdul'

import os
import sys

###### MBS Global Logger

import mbs_config

from utils import ensure_dir, resolve_path
from logging.handlers import TimedRotatingFileHandler
import logging

###############################################################################
MBS_LOG_DIR = "logs"

root_logger = logging.getLogger()

LOG_TO_STDOUT = False
###############################################################################
def setup_logging(log_to_stdout=False, log_file_name=None):
    global LOG_TO_STDOUT
    LOG_TO_STDOUT = log_to_stdout

    root_logger.setLevel(logging.INFO)

    formatter = logging.Formatter("%(levelname)8s | %(asctime)s | %(message)s")
    if LOG_TO_STDOUT:
        sh = logging.StreamHandler(sys.stdout)
        sh.setFormatter(formatter)
        root_logger.addHandler(sh)
    else:
        log_file_name = log_file_name or "mbs.log"
        log_dir = resolve_path(mbs_config.MBS_LOG_PATH)
        ensure_dir(log_dir)
        logfile = os.path.join(log_dir, log_file_name)
        fh = TimedRotatingFileHandler(logfile, backupCount=50, when="midnight")

        fh.setFormatter(formatter)
        # add the handler to the root logger
        root_logger.addHandler(fh)



###############################################################################
def simple_file_logger(name, log_file_name):
    lgr = logging.getLogger(name)

    if lgr.handlers:
        return lgr
    lgr.propagate = False

    log_dir = resolve_path(mbs_config.MBS_LOG_PATH)
    ensure_dir(log_dir)

    lgr.setLevel(logging.INFO)

    formatter = logging.Formatter("%(levelname)8s | %(asctime)s | %(message)s")

    logfile = os.path.join(log_dir, log_file_name)
    fh = TimedRotatingFileHandler(logfile, backupCount=10, when="midnight")

    fh.setFormatter(formatter)
    # add the handler to the root logger
    lgr.addHandler(fh)

    if LOG_TO_STDOUT:
        sh = logging.StreamHandler(sys.stdout)
        sh.setFormatter(formatter)
        lgr.addHandler(sh)

    return lgr

###############################################################################
class StdRedirectToLogger(object):

    def __init__(self, prefix=""):
        self.prefix = prefix

    def write(self, message):
        root_logger.info("%s: %s" % (self.prefix, message))

    def flush(self):
        pass

###############################################################################
def redirect_std_to_logger():
    # redirect stdout/stderr to log file
    sys.stdout = StdRedirectToLogger("STDOUT")
    sys.stderr = StdRedirectToLogger("STDERR")
    pass
