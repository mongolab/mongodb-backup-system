import mock

import mbs.mbs as mbs

from mbs.notification.message import NotificationMessage

from . import NotificationBaseTest


###############################################################################
# TestNotificationMessage
###############################################################################
class TestNotificationMessage(NotificationBaseTest):

    ############################################################################
    def test_notification_message(self):
        message = "Simple notification message"
        self.assertEqual(
            self.mbs.maker.make({
                '_type': 'mbs.notification.message.NotificationMessage',
                'message': message
            }).get_message(), message)


###############################################################################
# TestTemplateNotificationMessage
###############################################################################
class TestTemplateNotificationMessage(NotificationBaseTest):

    ############################################################################
    def test_file_template_string(self):
        with mock.patch.object(mbs, 'DEFAULT_TEMPLATE_DIR_ROOT',
                               self._temp_dir):
            template = self.mbs.maker.make({
                '_type': 'mbs.notification.message.TemplateNotificationMessage',
                'type': 'file',
                'template': 'foo/bar/bar.string'
            })
            self.assertEqual(
                template.get_message({'name': 'test'}),
                '\nHi, my name is test.\n'
            )

    ############################################################################
    def test_file_template_mustache(self):
        with mock.patch.object(mbs, 'DEFAULT_TEMPLATE_DIR_ROOT',
                               self._temp_dir):
            template = self.mbs.maker.make({
                '_type': 'mbs.notification.message.TemplateNotificationMessage',
                'type': 'file',
                'template': 'foo/baz/baz.mustache'
            })
            self.assertEqual(
                template.get_message({'name': 'test'}),
                '\nHi, my name is test.\n'
            )

    ############################################################################
    def test_string_template(self):
        with mock.patch.object(mbs, 'DEFAULT_TEMPLATE_DIR_ROOT',
                               self._temp_dir):
            templates = self.__class__.TEMPLATES

            template = self.mbs.maker.make({
                '_type': 'mbs.notification.message.TemplateNotificationMessage',
                'type': 'string',
                'template': templates['string']['simple']['foo/bar/bar.string']
            })
            self.assertEqual(
                template.get_message({'name': 'test'}),
                '\nHi, my name is test.\n'
            )

    ############################################################################
    def test_mustache_template(self):
        with mock.patch.object(mbs, 'DEFAULT_TEMPLATE_DIR_ROOT',
                               self._temp_dir):
            templates = self.__class__.TEMPLATES

            template = self.mbs.maker.make({
                '_type': 'mbs.notification.message.TemplateNotificationMessage',
                'type': 'mustache',
                'template': templates['mustache']['simple']['foo/baz/baz.mustache']
            })
            self.assertEqual(
                template.get_message({'name': 'test'}),
                '\nHi, my name is test.\n'
            )

