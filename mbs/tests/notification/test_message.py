


import mock

from . import NotificationBaseTest

from mbs.notification.message import NotificationMessage


###############################################################################
# TestNotificationMessage
###############################################################################
class TestNotificationMessage(NotificationBaseTest):
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
    pass
