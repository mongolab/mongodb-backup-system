__author__ = 'abdul'

from utils import document_pretty_string
###############################################################################
# MBS Object Base
###############################################################################
class MBSObject(object):
    """
    Represents The Object class that all MBS objects inherits
    """
    ###########################################################################
    def __init__(self):
        pass

    ###########################################################################
    def to_document(self):
        pass

    ###########################################################################
    def __str__(self):
        return document_pretty_string(self.to_document())

    ###########################################################################
    def __eq__(self, other):
        if other:
            return self.to_document() == other.to_document()

