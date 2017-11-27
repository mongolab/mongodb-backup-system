__author__ = 'abdul'

import logging
import smtplib
import traceback

from email.mime.text import MIMEText

from sendgrid import Sendgrid, Message

from .message import get_messages
from ..utils import listify
import hipchat

import pypd
from carbonio_client.client import CarbonIOClient
from robustify.robustify import robustify
from ..errors import raise_exception, raise_if_not_retriable

DEFAULT_NOTIFICATION_SUBJECT = "Backup System Notification"


###############################################################################
# LOGGER
###############################################################################
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


###############################################################################
# NotificationPriority class
class NotificationPriority(object):
    NORMAL = "normal"
    CRITICAL = "critical"


###############################################################################
# NotificationPriority class
class NotificationType(object):
    DEFAULT = "default"
    ERROR = "error"
    EVENT = "event"

###############################################################################
# Notifications class
###############################################################################
class Notifications(object):
    def __init__(self):
        self._handlers = {}
        self._handler_mapping = {}

    ###########################################################################
    @property
    def handlers(self):
        return self._handlers

    @handlers.setter
    def handlers(self, val):
        self._handlers = val

    ###########################################################################
    @property
    def handler_mapping(self):
        return self._handler_mapping

    @handler_mapping.setter
    def handler_mapping(self, val):
        self._handler_mapping = val

    ###########################################################################
    def send_notification(self, subject, message, recipient=None,
                          notification_type=NotificationType.DEFAULT,
                          priority=None):
        handlers = self.get_handlers_for(notification_type, priority=priority) or []
        for handler in handlers:
            try:
                handler.send_notification(subject, message, recipient)
            except Exception, ex:
                logger.exception("Exception while sending notification")

    ###########################################################################
    def send_error_notification(self, subject, message):
        self.send_notification(subject, message, notification_type=NotificationType.ERROR)

    ###########################################################################
    def send_event_notification(self, subject, message, priority=None):
        self.send_notification(subject, message, notification_type=NotificationType.EVENT,
                               priority=priority)

    ###########################################################################
    def notify_on_task_failure(self, task, exception, trace):
        self.send_notification(
            "Task failed",
            get_messages()['TaskFailureNotification'].get_message({
                'id': task.id,
                'task': task,
                'exception': exception,
                'trace': trace}))

    ###########################################################################
    def notify_task_reschedule_failed(self, task):
        self.send_notification(
            'Task Reschedule Failed',
            get_messages()['TaskRescheduleFailed'].get_message({
                'task': task}))

    ###########################################################################
    def get_handlers_for(self, notification_type, priority=NotificationPriority.NORMAL):
        handler_names = self.get_handler_names_for(notification_type, priority=priority)

        return map(self.get_handler_by_name, handler_names)

    ###########################################################################
    def get_handler_names_for(self, notification_type, priority=NotificationPriority.NORMAL):
        handlers_conf = self.handler_mapping.get(notification_type)

        if not handlers_conf:
            handle_names = NotificationType.DEFAULT
        else:
            if isinstance(handlers_conf, dict):
                handle_names = handlers_conf.get(priority)
            else:
                handle_names = listify(str(handlers_conf))

        return listify(handle_names)

    ###########################################################################
    def get_default_handler(self):
        return self.handler_mapping.get(NotificationType.DEFAULT)

    ###########################################################################
    def get_handler_by_name(self, name):
        return self._handlers.get(name)

###############################################################################
#################################            ##################################
#################################  handlers  ##################################
#################################            ##################################
###############################################################################

###############################################################################
# NotificationHandler
###############################################################################
class NotificationHandler(object):

    ###########################################################################
    def __init__(self):
        self._error_recipient_mapping = {}

    ###########################################################################
    def send_notification(self, subject, message, recipient=None):
        pass


###############################################################################
# LoggerNotificationHandler
###############################################################################
class LoggerNotificationHandler(NotificationHandler):

    ###########################################################################
    def __init__(self, level=logging.INFO):
        super(LoggerNotificationHandler, self).__init__()
        self._level = level

    ###########################################################################
    @property
    def level(self):
        return self._level

    @level.setter
    def level(self, level):
        self._level = level

    ###########################################################################
    def send_notification(self, subject, message, recipient=None):
        msg = "\n---- LoggerNotificationHandler ------ \n%s\n\n%s\n-------------" % (subject, message)
        logger.log(self.level, msg)

###########################################################################
# Default notifications
DEFAULT_NOTIFICATIONS = Notifications()
DEFAULT_NOTIFICATIONS.handlers = {
    "logError": LoggerNotificationHandler(level=logging.ERROR),
    "logInfo": LoggerNotificationHandler(level=logging.INFO)
}

DEFAULT_NOTIFICATIONS.handler_mapping = {
    "default": "logInfo",
    "error": "logInfo"
}

###############################################################################
# EmailNotificationHandler
###############################################################################
class EmailNotificationHandler(NotificationHandler):

    ###########################################################################
    def __init__(self):
        NotificationHandler.__init__(self)

        self._from_address = None
        self._to_address = None

    ###########################################################################
    @property
    def from_address(self):
        return self._from_address

    @from_address.setter
    def from_address(self, from_address):
        self._from_address = from_address

    ###########################################################################
    @property
    def to_address(self):
        return self._to_address

    @to_address.setter
    def to_address(self, to_address):
        self._to_address = to_address


###############################################################################
# SendgridNotificationHandler
###############################################################################
class SendgridNotificationHandler(EmailNotificationHandler):

    ###########################################################################
    def __init__(self):
        EmailNotificationHandler.__init__(self)
        self._sendgrid = None
        self._sendgrid_username = None
        self._sendgrid_password = None

    ###########################################################################
    # PROPERTIES
    ###########################################################################
    @property
    def sendgrid_username(self):
        return self._sendgrid_username

    @sendgrid_username.setter
    def sendgrid_username(self, sendgrid_username):
        self._sendgrid_username = sendgrid_username

    ###########################################################################
    @property
    def sendgrid_password(self):
        return self._sendgrid_password

    @sendgrid_password.setter
    def sendgrid_password(self, sendgrid_password):
        self._sendgrid_password = sendgrid_password

    ###########################################################################
    def _ensure_sg_initialized(self):
        if self._sendgrid is None:
            self._sendgrid = Sendgrid(self.sendgrid_username,
                                      self.sendgrid_password,
                                      secure=True)

    ###########################################################################
    def send_notification(self, subject, message, recipient=None):
        subject = subject or DEFAULT_NOTIFICATION_SUBJECT
        try:
            self._ensure_sg_initialized()
            logger.info("Sending notification email...")
            s_message = Message(self.from_address, subject=subject,
                                text=message)

            to_address = listify(recipient or self.to_address)
            for address in to_address:
                s_message.add_to(address)

            self._sendgrid.web.send(s_message)

            logger.info("Email sent successfully!")
        except Exception, e:
            print e
            print traceback.format_exc()
            logger.error("Error while sending email:\n%s" %
                         traceback.format_exc())


###############################################################################
# SmtpNotificationHandler
###############################################################################
class SmtpNotificationHandler(EmailNotificationHandler):

    ###########################################################################
    def __init__(self):
        NotificationHandler.__init__(self)

        self._smtp_host = None
        self._smtp_username = None
        self._smtp_password = None

    ###########################################################################

    def send_notification(self, subject, message, recipient=None):

        try:

            logger.info("Sending notification email...")
            msg = MIMEText(message.encode('utf-8'), 'plain', 'UTF-8')

            to_address = listify(recipient or self._to_address)
            msg['From'] = self.from_address
            msg['To'] = ",".join(to_address)

            if subject:
                msg['Subject'] = subject

            smtp = smtplib.SMTP(self.smtp_host)
            if (self.smtp_username is not None or
                self.smtp_password is not None):
                smtp.login(self.smtp_username, self.smtp_password)
            smtp.sendmail(self.from_address, to_address, msg.as_string())
            smtp.quit()
            logger.info("Email sent successfully!")
        except Exception, e:
            logger.error("Error while sending email:\n%s" %
                         traceback.format_exc())

    ###########################################################################
    # PROPERTIES
    ###########################################################################
    @property
    def smtp_host(self):
        return self._smtp_host

    @smtp_host.setter
    def smtp_host(self, smtp_host):
        self._smtp_host = smtp_host

    ###########################################################################
    @property
    def smtp_username(self):
        return self._smtp_username

    @smtp_username.setter
    def smtp_username(self, smtp_username):
        self._smtp_username = smtp_username

    ###########################################################################
    @property
    def smtp_password(self):
        return self._smtp_password

    @smtp_password.setter
    def smtp_password(self, smtp_password):
        self._smtp_password = smtp_password


###############################################################################
# HipchatNotificationHandler
###############################################################################
class HipchatNotificationHandler(NotificationHandler):

    ###########################################################################
    def __init__(self):
        NotificationHandler.__init__(self)

        self._api_token = None
        self._room_id = None
        self._from_name = None
        self._color = None

    ###########################################################################
    @property
    def api_token(self):
        return self._api_token

    @api_token.setter
    def api_token(self, val):
        self._api_token = val

    ###########################################################################
    @property
    def room_id(self):
        return self._room_id

    @room_id.setter
    def room_id(self, val):
        self._room_id = val

    ###########################################################################
    @property
    def from_name(self):
        return self._from_name

    @from_name.setter
    def from_name(self, val):
        self._from_name = val

    ###########################################################################
    @property
    def color(self):
        return self._color

    @color.setter
    def color(self, val):
        self._color = val

    ###########################################################################

    def send_notification(self, subject, message, recipient=None):

        try:

            room_id = recipient or self.room_id
            logger.info("Sending notification hipchat '%s'..." % subject)
            hipster = hipchat.HipChat(token=self.api_token)

            hipchat_message = "%s\n\n%s" % (subject, message)
            # limit message to 9000 chars
            if len(hipchat_message) > 9000:
                hipchat_message = hipchat_message[:9000] + "..."
            hipster.message_room(room_id, self.from_name, hipchat_message, color=self.color)

            logger.info("Hipchat sent successfully!")
        except Exception, e:
            logger.error("Error while sending hipchat message:\n%s" %
                         traceback.format_exc())

###############################################################################
def raise_if_not_pd_retriable(e):
    if "Forbidden" in str(e):
        logger.warn("Caught a retriable PD exception: %s" % e)
    else:
        raise_if_not_retriable(e)

###############################################################################
# PagerDutyNotificationHandler
###############################################################################
class PagerDutyNotificationHandler(NotificationHandler):

    ###########################################################################
    def __init__(self):
        NotificationHandler.__init__(self)

        self._api_key = None
        self._service_key = None
        self._sub_domain = None
        self._client_name = None

    ###########################################################################
    @property
    def api_key(self):
        return self._api_key

    @api_key.setter
    def api_key(self, val):
        self._api_key = val

    ###########################################################################
    @property
    def service_key(self):
        return self._service_key

    @service_key.setter
    def service_key(self, val):
        self._service_key = val
        pypd.api_key = val

    ###########################################################################
    @property
    def sub_domain(self):
        return self._sub_domain

    @sub_domain.setter
    def sub_domain(self, val):
        self._sub_domain = val

    ###########################################################################
    @property
    def client_name(self):
        return self._client_name

    @client_name.setter
    def client_name(self, val):
        self._client_name = val

    ###########################################################################
    def send_notification(self, subject, message, recipient=None):

        try:
            kwargs = {}
            if self.client_name:
                kwargs["client"] = self.client_name

            logger.info("PagerDuty: Sending notification: %s..." % subject)

            response = pypd.EventV2.create(data={
                'routing_key': self.service_key,
                'event_action': 'trigger',
                'payload': {
                    'summary': subject,
                    'severity': 'error',
                    'source': 'test',
                    'custom_details': {'details': message},
                }
            })
            return response["dedup_key"]
        except Exception, e:
            logger.error("Error while creating PagerDuty event:\n%s" %
                         traceback.format_exc())

    ###########################################################################
    @robustify(max_attempts=5, retry_interval=5,
               do_on_exception=raise_if_not_pd_retriable,
               do_on_failure=raise_exception)
    def resolve_incident(self, incident_key):
        try:
            logger.info("PagerDutyNotificationHandler: Resolving incident '%s'" % incident_key)

            response = pypd.EventV2.create(data={
                'routing_key': self.service_key,
                'event_action': 'resolve',
                'dedup_key': incident_key,
                'payload': {
                    'summary': 'Resolving incident %s' % incident_key,
                    'severity': 'error',
                    'source': self.client_name,
                }
            })
            return response["dedup_key"]
        except Exception, e:
            logger.error("Error while trying to resolve PagerDuty event:\n%s" %
                         traceback.format_exc())

###############################################################################
# SlackNotificationHandler
###############################################################################
class SlackNotificationHandler(NotificationHandler):

    ###########################################################################
    def __init__(self):
        NotificationHandler.__init__(self)

        self._webhook_url = None

    ###########################################################################

    @property
    def webhook_url(self):
        return self._webhook_url

    @webhook_url.setter
    def webhook_url(self, val):
        self._webhook_url = val

    ###########################################################################
    def send_notification(self, subject, message, recipient=None):

        try:

            logger.info("Sending slack notification '%s'..." % subject)
            sc_client = CarbonIOClient(url=self.webhook_url)

            text = "*%s*: %s" % (subject, message)

            result = sc_client.post({
                "text": text,
                "mrkdwn": True

            })

            if result.text == "ok":
                logger.info("Slack message sent successfully!")
            else:
                logger.error("Failed to send slack message. Result: %s" % result.text)
        except Exception, e:
            logger.error("Error while sending slack notification:\n%s" %
                         traceback.format_exc())

###############################################################################
def get_class_full_name(clazz):
    return (clazz.__module__ + "." +
            clazz.__name__)
