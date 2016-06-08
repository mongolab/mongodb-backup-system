__author__ = 'abdul'

import utils
import copy

###############################################################################
# MBS Object Base
###############################################################################
class MBSObject(object):
    """
    Represents The Object class that all MBS objects inherits
    """
    ###########################################################################
    def __init__(self):
        self._id = None

    ###########################################################################
    @property
    def id(self):
        return self._id

    @id.setter
    def id(self, id):
        self._id = id

    ###########################################################################
    def to_document(self, display_only=False):
        """
            Creates a document that represents the object.
            display_only means that the document return will be used for
            displaying only. This should be used when displaying documents
            that contain credentials/etc...
        """
        doc = {
            "_type": self.full_type_name
        }
        if self.id:
            doc["_id"] = self.id

        return doc

    ###########################################################################
    def to_bson(self):
        return utils.document_to_bson(self.to_document())

    ###########################################################################
    @property
    def type_name(self):
        return utils.object_type_name(self)

    ###########################################################################
    @property
    def full_type_name(self):
        return utils.object_full_type_name(self)

    ###########################################################################
    def __str__(self):
        return utils.document_pretty_string(self.to_document(display_only=True))

    ###########################################################################
    def __eq__(self, other):
        if isinstance(other, MBSObject):
            return self.to_document() == other.to_document()

    ###########################################################################
    def __ne__(self, other):
        return not self.__eq__(other)

    ###########################################################################
    def diff(self, other, display_only=False):
        if not isinstance(other, MBSObject):
            raise Exception("Cannot diff against a non mbs object")

        my_dict = self.to_document(display_only=display_only)
        other_dict = other.to_document(display_only=display_only)
        return utils.dict_diff(my_dict, other_dict)

    ###########################################################################
    def clone(self):
        return copy.deepcopy(self)