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
    def to_document(self, display_only=False):
        """
            Creates a document that represents the object.
            display_only means that the document return will be used for
            displaying only. This should be used when displaying documents
            that contain credentials/etc...
        """

        pass

    ###########################################################################
    @property
    def type_name(self):
        return self.__class__.__name__

    ###########################################################################
    def __str__(self):
        return document_pretty_string(self.to_document(display_only=True))

    ###########################################################################
    def __eq__(self, other):
        if isinstance(other, MBSObject):
            return self.to_document() == other.to_document()

    ###########################################################################
    def __ne__(self, other):
        return not self.__eq__(other)

