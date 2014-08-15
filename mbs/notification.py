__author__ = 'abdul'

import traceback
import mbs_logging
from utils import listify
from sendgrid import Sendgrid, Message


import smtplib

from email.mime.text import MIMEText

DEFAULT_NOTIFICATION_SUBJECT = "Backup System Notification"

###############################################################################
# LOGGER
###############################################################################
logger = mbs_logging.logger

###############################################################################
###############################                 ###############################
###############################  Notifications  ###############################
###############################                 ###############################
###############################################################################

###############################################################################
# NotificationHandler
###############################################################################
class NotificationHandler(object):

    ###########################################################################
    def __init__(self):
        self._error_recipient_mapping = None

    ###########################################################################
    def send_notification(self, subject, message, recipient=None):
        pass

    ###########################################################################
    def send_error_notification(self, subject, message, exception):
        recipient = self.get_recipient_by_error_class(exception.__class__)

        self.send_notification(subject, message, recipient)

    ###########################################################################
    def notify_on_task_failure(self, task, exception, trace):
        subject = "Task failed"
        message = ("Task '%s' failed.\n%s\n\nCause: \n%s\nStack Trace:"
                   "\n%s" % (task.id, task, exception, trace))

        self.send_notification(subject, message)

    ###########################################################################
    def notify_task_reschedule_failed(self, task):
        subject = "Task Reschedule Failed"
        message = ("Task Reschedule Failed!.\n\n\n%s" % task)

        self.send_notification(subject, message)

    ###########################################################################
    def get_recipient_by_error_class(self, error_class):
        class_name = get_class_full_name(error_class)
        if class_name in self.error_recipient_mapping:
            return self.error_recipient_mapping[class_name]
        else:
            for base in error_class.__bases__:
                recipient = self.get_recipient_by_error_class(base)
                if recipient:
                    return recipient



    ###########################################################################
    @property
    def error_recipient_mapping(self):
        return self._error_recipient_mapping

    @error_recipient_mapping.setter
    def error_recipient_mapping(self, val):
        self._error_recipient_mapping = val

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
def get_class_full_name(clazz):
    return (clazz.__module__ + "." +
            clazz.__name__)
