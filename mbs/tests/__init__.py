import hashlib

from unittest import TestCase

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
    @staticmethod
    def md5(path):
        md5 = hashlib.md5()
        with open(path, 'rb') as file_:
            data = file_.read(8192)
            while data:
                md5.update(data)
                data = file_.read(8192)
            return md5.hexdigest()
