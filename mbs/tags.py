__author__ = 'abdul'


from base import MBSObject


###############################################################################
# DynamicTag class
###############################################################################
class DynamicTag(MBSObject):
    """
        Base class for tag descriptors that generate tag values for backups
        at schedule/reschedule time
    """
    ###########################################################################
    def __init__(self):
        pass

    ###########################################################################
    def generate_tag_value(self, plan):
        pass