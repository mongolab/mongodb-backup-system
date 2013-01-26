__author__ = 'abdul'


###############################################################################
########################                       ################################
########################  Backup System Errors ################################
########################                       ################################
###############################################################################


###############################################################################
# MBSError
###############################################################################
class MBSError(Exception):
    """
        Base class for all backup system error
    """
    ###########################################################################
    def __init__(self, msg, cause=None):
        self.message = msg
        self._cause = cause
    ###########################################################################
    def __str__(self):
        cause_str = "Cause: %s" % self._cause if self._cause else ""
        return "%s: %s. %s" % (self.error_type, self.message, cause_str)

    ###########################################################################
    @property
    def error_type(self):
        """
            returns the error type which is the class name
        """
        return self.__class__.__name__

###############################################################################
# ConfigurationError
###############################################################################
class ConfigurationError(MBSError):
    pass

###############################################################################
class RetriableError(Exception):
    """
        Base class for ALL retriable errors. All retriable errors should
        inherit this class
    """

###############################################################################
# BackupError
###############################################################################
class BackupError(MBSError):
    """
        Base class for backup errors
    """


class ConnectionError(MBSError, RetriableError):
    """
        Base error for connection errors
    """

class ServerError(ConnectionError):
    """
        Base error for server errors
    """

class ReplicasetError(ConnectionError):
    """
        Base error for replicaset errors
    """

class PrimaryNotFoundError(ReplicasetError):
    pass

class NoEligibleMembersFound(ReplicasetError):
    pass

class DumpError(BackupError):
    """
        Base error for dump errors
    """

class BadCollectionNameError(DumpError):
    """
        Raised when a database contains bad collection names such as the ones
        containing "/"
    """

class InvalidBSONObjSizeError(DumpError, RetriableError):
    pass

class CappedCursorOverrunError(DumpError, RetriableError):
    pass

class InvalidDBNameError(DumpError):
    pass

class ArchiveError(BackupError):
    """
        Base error for dump errors
    """

class NoSpaceLeftError(ArchiveError):
    pass

class TargetError(MBSError):
    """
        Base type for target errors
    """

class TargetConnectionError(TargetError, RetriableError):
    pass

class TargetUploadError(TargetError, RetriableError):
    pass

class TargetDeleteError(TargetError, RetriableError):
    pass

class TargetFileNotFoundError(TargetError):
    pass

############ UTILITY ERROR METHODS ##############
def is_connection_exception(exception):
    msg = str(exception)
    return ("timed out" in msg or "refused" in msg or "rest" in msg or
            "Broken pipe" in msg)