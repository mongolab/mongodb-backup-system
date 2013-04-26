__author__ = 'abdul'

import traceback
import mbs_logging
from utils import listify


import smtplib

from email.mime.text import MIMEText

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
    def notify_on_backup_failure(self, backup, exception, trace):
        subject = "Backup failed"
        message = ("Backup '%s' failed.\n%s\n\nCause: \n%s\nStack Trace:"
                   "\n%s" % (backup.id, backup, exception, trace))

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

        self._smtp_host = None
        self._smtp_username = None
        self._smtp_password = None
        self._from_address = None
        self._to_address = None

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
def get_class_full_name(clazz):
    return (clazz.__module__ + "." +
            clazz.__name__)