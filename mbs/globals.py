__author__ = 'abdul'


###############################################################################
# State
###############################################################################
class State(object):
    SCHEDULED = "SCHEDULED"
    IN_PROGRESS = "IN PROGRESS"
    FAILED = "FAILED"
    CANCELED = "CANCELED"
    SUCCEEDED = "SUCCEEDED"

###############################################################################
# Priority
###############################################################################
class Priority(object):
    # Priority constants
    HIGH = 0
    MEDIUM = 5
    LOW = 10

###############################################################################
# EventType
###############################################################################
class EventType(object):
    # event types
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
