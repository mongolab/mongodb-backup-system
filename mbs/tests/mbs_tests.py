import os
import shutil
import tempfile

import mock

import mbs.mbs

from . import BaseTest


###############################################################################
# MBSTest
###############################################################################
class MBSTest(BaseTest):

    ###########################################################################
    def test_resolve_notification_template_path(self):
        tempdir = tempdir2 = None
        try:
            # single root (default)

            tempdir = tempfile.mkdtemp()
            os.makedirs(os.path.join(tempdir, 'foo', 'bar'))
            os.makedirs(os.path.join(tempdir, 'foo', 'baz'))
            os.makedirs(os.path.join(tempdir, 'not', 'in'))

            for template in ['foo.mustache', 'foo/foo.mustache',
                             'foo/bar/bar.string', 'foo/bar/baz.mustache']:
                open(os.path.join(tempdir, template), 'w').write('test')

            with mock.patch.object(mbs.mbs, 'DEFAULT_TEMPLATE_DIR_ROOT', tempdir):
                # test walk find
                self.assertEqual(
                    self.mbs.resolve_notification_template_path('baz.mustache'),
                    os.path.join(tempdir, 'foo', 'bar', 'baz.mustache'))
                # test failure
                self.assertRaisesRegexp(
                    ValueError,
                    '^Template',
                    self.mbs.resolve_notification_template_path,
                    'bar/baz.mustache')
                # test join find
                self.assertEqual(
                    self.mbs.resolve_notification_template_path(
                        'foo/bar/baz.mustache'),
                    os.path.join(tempdir, 'foo', 'bar', 'baz.mustache'))
                # test top level find with nested match
                self.assertEqual(
                    self.mbs.resolve_notification_template_path(
                        'foo.mustache'),
                    os.path.join(tempdir, 'foo.mustache'))
                # test nested match
                self.assertEqual(
                    self.mbs.resolve_notification_template_path(
                        'foo/foo.mustache'),
                    os.path.join(tempdir, 'foo/foo.mustache'))
                # test absolute path
                abs_path = os.path.join( tempdir, 'not', 'in', 'roots.mustache')
                open(abs_path, 'w').write('test')
                self.assertEqual(
                    self.mbs.resolve_notification_template_path(
                        abs_path), abs_path)

            # multiple roots

            tempdir2 = tempfile.mkdtemp()
            os.makedirs(os.path.join(tempdir2, 'foo', 'bar'))
            os.makedirs(os.path.join(tempdir2, 'bar', 'baz'))

            for template in ['bar.mustache', 'bar/foo.string',
                             'foo/bar/bar.string', 'foo/bar/baz.mustache']:
                open(os.path.join(tempdir2, template), 'w').write('test')
            # test shadow
            self.assertEqual(
                self.mbs.resolve_notification_template_path(
                    'baz.mustache', tempdir2, tempdir),
                os.path.join(tempdir2, 'foo', 'bar', 'baz.mustache'))
            # test failure
            self.assertRaisesRegexp(
                ValueError,
                '^Template',
                self.mbs.resolve_notification_template_path,
                'bar/baz.mustache',
                tempdir2, tempdir
            )
            # test find in fallback
            self.assertEqual(
                self.mbs.resolve_notification_template_path(
                    'foo/foo.mustache', tempdir2, tempdir),
                os.path.join(tempdir, 'foo', 'foo.mustache'))
        finally:
            if tempdir is not None:
                shutil.rmtree(tempdir)
            if tempdir2 is not None:
                shutil.rmtree(tempdir2)

