from unittest import TestCase

from makerpy.maker import Maker

from mbs.type_bindings import TYPE_BINDINGS


class BaseTest(TestCase):
    def setUp(self):
        self.maker = Maker(type_bindings=TYPE_BINDINGS)

    def tearDown(self):
        pass

