import copy
import json
import os
import shutil
import tempfile

from unittest import TestCase

import mbs.mbs as mbs
import mbs.mbs_config as mbs_config

from .tutils import truthy, falsey


ENV = {
    'RUN_INT_TESTS': 'MBS_RUN_INT_TESTS',
}


###############################################################################
# BaseTest
###############################################################################
class BaseTest(TestCase):
    CONFIG = {
        '_type': 'mbs.mbs.MBS'
    }

    # ----------------
    # -- class methods
    # ----------------

    ###########################################################################
    @classmethod
    def _update_config(cls, other_config):
        config = copy.deepcopy(cls.CONFIG)
        config.update(other_config)
        return config

    # -----------------
    # -- static methods
    # -----------------

    ###########################################################################
    @staticmethod
    def _createMBSDir():
        return tempfile.mkdtemp()

    ###########################################################################
    @staticmethod
    def _destroyMBSDir(path):
        shutil.rmtree(path)

    # ----------------
    # -- env shortcuts
    # ----------------

    ###########################################################################
    @property
    def run_int_tests(self):
        return truthy(os.environ.get(ENV['RUN_INT_TESTS'], 'no'))

    ###########################################################################
    def _get_env_var_or_skip(self, env_var):
        bucket_name = os.environ.get(env_var, None)
        if bucket_name is None:
            raise self.skipTest('%s is not defined' % (env_var))
        return bucket_name

    # -------------
    # -- properties
    # -------------

    ###########################################################################
    @property
    def mbs(self):
        return self._mbs

    # -----------------
    # -- setup/teardown
    # -----------------

    ###########################################################################
    def setUp(self):
        self._temp_config_dir = self.__class__._createMBSDir()
        self._temp_config_path = \
            os.path.join(self._temp_config_dir, 'mbs.config')
        self._temp_log_path = \
            os.path.join(self._temp_config_dir, 'logs')

        if self.__class__.CONFIG is not None:
            open(self._temp_config_path, 'w').write(
                json.dumps(self.__class__.CONFIG, indent=4, sort_keys=True))

        self._orig_config_path = mbs_config.MBS_CONF_PATH
        mbs_config.MBS_CONF_PATH = self._temp_config_path
        self._orig_log_path = mbs_config.MBS_LOG_PATH
        mbs_config.MBS_LOG_PATH = self._temp_log_path

        self._mbs = mbs.get_mbs()

    ###########################################################################
    def tearDown(self):
        mbs_config.MBS_CONF_PATH = self._orig_config_path
        mbs_config.MBS_LOG_PATH = self._orig_log_path
        mbs.mbs_singleton = None
        self._mbs = None
        self._destroyMBSDir(self._temp_config_dir)

