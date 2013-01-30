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
    def __init__(self, msg, cause=None, details=None):
        self.message = msg
        self._cause = cause
        self._details = details

    ###########################################################################
    def __str__(self):
        return self.detailed_message

    ###########################################################################
    @property
    def detailed_message(self):
        details_str = "Error Type: %s, Details: %s" % (self.error_type,
                                                       self.message)
        if self._details:
            details_str += ". %s" % self._details
        if self._cause:
            details_str += ", Cause: %s: %s" % (type(self._cause), self._cause)

        return details_str

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

class ConnectionError(MBSError, RetriableError):
    """
        Base error for connection errors
    """

class AuthenticationFailedError(MBSError):
    pass

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

class DumpError(MBSError):
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

class BadTypeError(DumpError, RetriableError):
    pass

class ArchiveError(MBSError):
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
    return ("timed out" in msg or "refused" in msg or "reset" in msg or
            "Broken pipe" in msg or "closed" in msg)