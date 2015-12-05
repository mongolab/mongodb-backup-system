import os
import shutil
import tempfile

from .. import BaseTest


###############################################################################
# NotificationBaseTest
###############################################################################
class NotificationBaseTest(BaseTest):
    TEMPLATES = {
        'string': {
            'simple': {
                'foo/bar/bar.string': \
"""
Hi, my name is $name.
"""
            }
        },
        'mustache': {
            'simple': {
                'foo/baz/baz.mustache': \
"""
Hi, my name is {{name}}.
"""
            }
        }
    }

    def setUp(self):
        super(NotificationBaseTest, self).setUp()
        self._temp_dir = tempfile.mkdtemp()
        os.makedirs(os.path.join(self._temp_dir, 'foo/bar'))
        os.makedirs(os.path.join(self._temp_dir, 'foo/baz'))
        cls = self.__class__
        for type_ in cls.TEMPLATES:
            for name in cls.TEMPLATES[type_]:
                for path in cls.TEMPLATES[type_][name]:
                    open(os.path.join(self._temp_dir, path), 'w').write(
                        cls.TEMPLATES[type_][name][path])

    def tearDown(self):
        shutil.rmtree(self._temp_dir)
        super(NotificationBaseTest, self).tearDown()

