
import mock

from mbs.backup import Backup
from mbs.notification.handler import NotificationHandler
from mbs.notification.message import get_messages

from . import NotificationBaseTest


class TestNotificationHandler(NotificationBaseTest):
    @mock.patch('mbs.notification.handler.NotificationHandler.send_notification')
    def test_notifiy_on_task_failure(self, m):
        task = Backup()
        exception = Exception('blah')

        h = NotificationHandler()
        h.notify_on_task_failure(task, exception, None)

        result = (
            'Task failed', 
            get_messages()['TaskFailureNotification'].get_message({
                'id': task.id,
                'task': task,
                'exception': exception,
                'trace': None})
        )

        self.assertTupleEqual(result, m.call_args[0])

    @mock.patch('mbs.notification.handler.NotificationHandler.send_notification')
    def test_notify_task_reschedule_failed(self, m):
        task = Backup()

        h = NotificationHandler()
        h.notify_task_reschedule_failed(Backup())

        result = (
            'Task Reschedule Failed',
            get_messages()['TaskRescheduleFailed'].get_message({
                'task': task})
        )

        self.assertTupleEqual(result, m.call_args[0])

