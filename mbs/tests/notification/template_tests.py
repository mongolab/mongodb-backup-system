import os

import mock

import mbs.mbs

from mbs.notification.template import (
    NotificationTemplate, MustacheNotificationTemplate
)

from . import NotificationBaseTest


###############################################################################
# TestNotificationTemplate
###############################################################################
class TestNotificationTemplate(NotificationBaseTest):
    def test_get_template(self):
        with mock.patch.object(NotificationTemplate, '_parsed_templates', {}):
            self.assertNotIn(
                'blah blah blah', NotificationTemplate._parsed_templates)
            t = NotificationTemplate._get_template('blah blah blah')
            self.assertIn(
                'blah blah blah', NotificationTemplate._parsed_templates)
            self.assertEqual(
                t, NotificationTemplate._get_template('blah blah blah'))

    def test_render_path(self):
        with mock.patch.object(mbs.mbs, 'DEFAULT_TEMPLATE_DIR_ROOT',
                               self._temp_dir):
            self.assertEqual(
                NotificationTemplate.render_path(
                    'foo/bar/bar.string', {'name': 'test'}),
                '\nHi, my name is test.\n')

    def test_renderer_case_insensitivity(self):
        with mock.patch.object(mbs.mbs, 'DEFAULT_TEMPLATE_DIR_ROOT',
                               self._temp_dir):
            open(os.path.join(
                self._temp_dir, 'case.STRING'), 'w').write('blah')
            self.assertEqual(
                NotificationTemplate.render_path(
                    'case.STRING', {}), 'blah')

    def test_unrecognized_renderer(self):
        with mock.patch.object(mbs.mbs, 'DEFAULT_TEMPLATE_DIR_ROOT',
                               self._temp_dir):
            self.assertRaises(
                ValueError, NotificationTemplate.render_path,
                'template.notarenderer', {})

    def test_choose_appropriate_renderer(self):
        with mock.patch.object(mbs.mbs, 'DEFAULT_TEMPLATE_DIR_ROOT',
                               self._temp_dir):
            with mock.patch.object(
                    mbs.notification.template.MustacheNotificationTemplate,
                    'render_string') as mnt, \
                 mock.patch.object(
                     NotificationTemplate, 'render_string') as nt:
                NotificationTemplate.render_path('foo/bar/bar.string', {})
                NotificationTemplate.render_path('foo/baz/baz.mustache', {})
                self.assertEqual(mnt.call_count, 1)
                self.assertEqual(nt.call_count, 1)

