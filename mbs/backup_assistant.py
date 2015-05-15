__author__ = 'abdul'



from base import MBSObject

class BackupAssistant(MBSObject):
    """

    """
    def to_document(self, display_only=False):
        return {
            "_type": self.full_type_name
        }
