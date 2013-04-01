__author__ = 'abdul'
# Contains classes for contacts used by backup system

from base import MBSObject

###############################################################################
# Contact
###############################################################################
class BackupContact(MBSObject):

    ###########################################################################
    def __init__(self):
        self._email = None

    ###########################################################################
    @property
    def email(self):
        return self._email

    @email.setter
    def email(self, email):
        self._email = email

    ###########################################################################
    def to_document(self, display_only=False):
        return {
            "_type": "BackupContact",
            "email": self.email
        }
