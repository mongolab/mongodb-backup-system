__author__ = 'abdul'




###############################################################################
# Encryptor
###############################################################################
class Encryptor(object):
    """
        The no-op default string encryption class used by mbs. Should be
        inherited and overridden for custom encryption
    """
    ###########################################################################
    def __init__(self):
        pass

    def encrypt_string(self, string):
        return string

    def decrypt_string(self, string):
        return string
