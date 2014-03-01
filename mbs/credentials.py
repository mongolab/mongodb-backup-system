__author__ = 'eric'
from mbs.base import MBSObject

###############################################################################
# Abstract Credentials class
###############################################################################
class Credentials(MBSObject):
    def __init__(self):
        MBSObject.__init__(self)

    ###########################################################################
    def get_credential(self, key):
        return None

    ###########################################################################
    def set_credential(self, key, credential):
        return None


###############################################################################
# BaseCredentials implements a simple dictionary store
###############################################################################
class BaseCredentials(Credentials):
    def __init__(self):
        Credentials.__init__(self)
        self._credentials = {}
        self._encryptor = None

    ###########################################################################
    @property
    def credentials(self):
        raise NotImplementedError("Use get_credential(key) instead.")

    @credentials.setter
    def credentials(self, val):
        self._credentials = val

    ###########################################################################
    @property
    def encryptor(self):
        return self._encryptor

    @encryptor.setter
    def encryptor(self, val):
        self._encryptor = val

    ###########################################################################
    def get_credential(self, key):
        raw_value = self._credentials.get(key, None)
        if self.encryptor:
            return self.encryptor.decrypt_string(raw_value)
        else:
            return raw_value

    ###########################################################################
    def set_credential(self, key, credential):
        if self.encryptor:
            raw_value = self.encryptor.encrypt_string(credential)
        else:
            raw_value = credential
        self._credentials[key] = raw_value

    ###########################################################################
    def to_document(self, display_only=False):
        return {"_type": "BaseCredentials",
                "credentials": self._credentials}


