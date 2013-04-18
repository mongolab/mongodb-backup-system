__author__ = 'abdul'


from base import MBSObject
from mongo_uri_tools import parse_mongo_uri
from utils import get_host_ips
import mbs_logging

###############################################################################
# LOGGER
###############################################################################

logger = mbs_logging.logger

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



###############################################################################
# SourceIPTag class
###############################################################################
class SourceIPTag(DynamicTag):
    """
        Evaluates to the ip address of the source
    """

    ###########################################################################
    def generate_tag_value(self, plan):
        try:
            uri_wrapper = parse_mongo_uri(plan.source.uri)
            host = uri_wrapper.host
            ips = get_host_ips(host)
            if ips:
                return ips[0][0]
        except Exception, e:
            logger.error("SourceIPTag: Error while generating tag value: %s" %
                         e)
