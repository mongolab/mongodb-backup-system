import utils
import os

MBS_CONF_PATH = '~/.mbs/mbs.config'

MBS_LOG_PATH = '~/.mbs/logs'

def mbs_conf_dir():
    return os.path.dirname(utils.resolve_path(MBS_CONF_PATH))

