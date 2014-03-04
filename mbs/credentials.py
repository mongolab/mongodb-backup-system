__author__ = 'eric'

from mbs import get_mbs
from base import MBSObject

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
        self._credentials = None
        self._encryptor = None

    ###########################################################################
    @property
    def credentials(self):
        raise NotImplementedError(
            "Direct access not supported by BaseCredentials - use "
            "get_credential(key) instead.")

    @credentials.setter
    def credentials(self, val):
        self._credentials = val

    ###########################################################################
    @property
    def encryptor(self):
        # defers to mbs's choice of encryptor
        return get_mbs().encryptor

    @encryptor.setter
    def encryptor(self, val):
        raise NotImplementedError(
            "Instanced encryptors not supported by BaseCredentials")

    ###########################################################################
    def get_credential(self, key):
        if self._credentials is None:
            return None
        raw_value = self._credentials.get(key, None)
        if raw_value:
            encrypted_value = raw_value.encode('ascii', 'ignore')
            if self.encryptor:
                return self.encryptor.decrypt_string(encrypted_value)
            else:
                return raw_value
        else:
            msg = "Key %s not available in credential set" % key
            raise KeyError(msg)

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


