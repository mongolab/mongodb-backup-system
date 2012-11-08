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
        pass
    ###########################################################################

    def send_notification(self, subject, message):
        pass



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

    def send_notification(self, subject, message):

        try:

            logger.info("Sending notification email...")
            msg = MIMEText(message.encode('utf-8'), 'plain', 'UTF-8')

            to_address = listify(self._to_address)
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