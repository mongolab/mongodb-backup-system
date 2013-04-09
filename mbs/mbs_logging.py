__author__ = 'abdul'

import os
import sys

###### MBS Global Logger

from utils import resolve_path, ensure_dir
from logging.handlers import TimedRotatingFileHandler
import logging

###############################################################################
MBS_LOG_DIR = resolve_path("~/.mbs/logs")

ensure_dir(MBS_LOG_DIR)

logger = logging.getLogger("MBSLogger")
logger.setLevel(logging.INFO)

formatter = logging.Formatter("%(levelname)8s | %(asctime)s | %(message)s")

logfile = os.path.join(MBS_LOG_DIR, "mbs.log")
fh = TimedRotatingFileHandler(logfile, backupCount=50, when="midnight")

fh.setFormatter(formatter)
# add the handler to the root logger
logging.getLogger().addHandler(fh)

def log_to_stdout():
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(formatter)
    logging.getLogger().addHandler(sh)

