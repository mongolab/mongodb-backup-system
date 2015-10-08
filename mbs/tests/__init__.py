import hashlib
import os

from unittest import SkipTest, TestCase

from makerpy.maker import Maker

from mbs.type_bindings import TYPE_BINDINGS


###############################################################################
# BaseTest
###############################################################################
class BaseTest(TestCase):

    ###########################################################################
    def setUp(self):
        self.maker = Maker(type_bindings=TYPE_BINDINGS)

    ###########################################################################
    def tearDown(self):
        pass

    ###########################################################################
    def _get_env_var_or_skip(self, env_var):
        bucket_name = os.environ.get(env_var, None)
        if bucket_name is None:
            raise SkipTest('%s is not defined' % (env_var))
        return bucket_name

    ###########################################################################
    @staticmethod
    def md5(path):
        md5 = hashlib.md5()
        with open(path, 'rb') as file_:
            data = file_.read(8192)
            while data:
                md5.update(data)
                data = file_.read(8192)
            return md5.hexdigest()
