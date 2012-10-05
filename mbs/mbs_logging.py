__author__ = 'abdul'

import os
import sys

###### MBS Global Logger

import logging

logger = logging.getLogger("MBSLogger")
logger.setLevel(logging.INFO)

logfile = os.path.join("mbs.log")
formatter = logging.Formatter("%(levelname)8s | %(asctime)s | %(message)s")

sh = logging.StreamHandler(sys.stdout)
sh.setFormatter(formatter)
logger.addHandler(sh)

fh = logging.FileHandler(logfile)
fh.setFormatter(formatter)
logger.addHandler(fh)
