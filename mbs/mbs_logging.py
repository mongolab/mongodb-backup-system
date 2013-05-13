__author__ = 'abdul'

import os
import sys

###### MBS Global Logger

import mbs_config

from utils import ensure_dir
from logging.handlers import TimedRotatingFileHandler
import logging

###############################################################################
MBS_LOG_DIR = "logs"

logger = logging.getLogger("MBSLogger")

def setup_logging(log_to_stdout=False):
    log_dir = os.path.join(mbs_config.MBS_CONF_DIR, MBS_LOG_DIR)
    ensure_dir(log_dir)

    logger.setLevel(logging.INFO)

    formatter = logging.Formatter("%(levelname)8s | %(asctime)s | %(message)s")

    logfile = os.path.join(log_dir, "mbs.log")
    fh = TimedRotatingFileHandler(logfile, backupCount=50, when="midnight")

    fh.setFormatter(formatter)
    # add the handler to the root logger
    logging.getLogger().addHandler(fh)

    if log_to_stdout:
        sh = logging.StreamHandler(sys.stdout)
        sh.setFormatter(formatter)
        logging.getLogger().addHandler(sh)

