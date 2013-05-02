__author__ = 'abdul'

import mongo_uri_tools

import mbs_logging

###############################################################################
########################                       ################################
########################  Backup System Errors ################################
########################                       ################################
###############################################################################



###############################################################################
# LOGGER
###############################################################################

logger = mbs_logging.logger

###############################################################################
# MBSError
###############################################################################
class MBSError(Exception):
    """
        Base class for all backup system error
    """
    ###########################################################################
    def __init__(self, msg=None, cause=None, details=None):
        self._message = msg
        self._cause = cause
        self._details = details


    ###########################################################################
    @property
    def message(self):
        return self._message

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

    ###########################################################################
    def __str__(self):
        return self.detailed_message

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
    ###########################################################################
    def __init__(self, uri, details=None, cause=None):
        msg = "Could not establish a database connection to '%s'" % uri
        super(ConnectionError, self).__init__(msg=msg, details=details, cause=cause)

###############################################################################
class AuthenticationFailedError(MBSError):

    ###########################################################################
    def __init__(self, uri, cause=None):
        msg = "Failed to authenticate to '%s'" % uri
        super(AuthenticationFailedError, self).__init__(msg=msg, cause=cause)

###############################################################################
class ServerError(ConnectionError):
    """
        Base error for server errors
    """

###############################################################################
class ReplicasetError(MBSError, RetriableError):
    """
        Base error for replicaset errors
    """
    ###########################################################################
    def __init__(self, details=None, cause=None):
        msg = "Could not connect to replica set"
        super(ReplicasetError, self).__init__(msg=msg, details=details,
                                              cause=cause)

###############################################################################
class PrimaryNotFoundError(ReplicasetError):

    ###########################################################################
    def __init__(self, uri):
        details = "Unable to determine primary for cluster '%s'" % uri
        super(PrimaryNotFoundError, self).__init__(details=details)

###############################################################################
class NoEligibleMembersFound(ReplicasetError):

    ###########################################################################
    def __init__(self, uri):
        details = ("No eligible members in '%s' found to take dump from" %
                   mongo_uri_tools.mask_mongo_uri(uri))
        super(NoEligibleMembersFound, self).__init__(details=details)


###############################################################################
class DumpError(MBSError):
    """
        Base error for dump errors
        IMPORTANT NOTE! note that all dump errors DOES NOT pass the cause since
        the cause is a CalledProcessError that contains the full un-censored
        dump command (which might contain username/password). It has been
        omitted to avoid logging credentials
    """
    ###########################################################################
    def __init__(self, dump_cmd, return_code, last_dump_line):
        msg = ("Failed to mongodump")
        details = ("Failed to dump. Dump command '%s' returned a non-zero "
                   "exit status %s.Check dump logs. Last dump log line: "
                   "%s" % (dump_cmd, return_code, last_dump_line))
        super(DumpError, self).__init__(msg=msg, details=details)


###############################################################################
class BadCollectionNameError(DumpError):
    """
        Raised when a database contains bad collection names such as the ones
        containing "/"
    """
    ###########################################################################
    def __init__(self, dump_cmd, return_code, last_dump_line):
        super(BadCollectionNameError, self).__init__(dump_cmd, return_code,
                                                     last_dump_line)
        self._message = ("Failed to mongodump... possibly because you "
                         "have collection name(s) with invalid "
                         "characters (e.g. '/'). If so, please rename or "
                         "drop these collection(s)")

###############################################################################
class InvalidBSONObjSizeError(DumpError, RetriableError):
    pass

###############################################################################
class CappedCursorOverrunError(DumpError, RetriableError):
    pass

###############################################################################
class InvalidDBNameError(DumpError):

    ###########################################################################
    def __init__(self, dump_cmd, return_code, last_dump_line):
        super(InvalidDBNameError, self).__init__(dump_cmd, return_code,
                                                 last_dump_line)
        self._message = ("Failed to mongodump because the name of your "
                         "database is invalid")

###############################################################################
class BadTypeError(DumpError, RetriableError):
    pass

###############################################################################
class ExhaustReceiveError(DumpError, RetriableError):
    pass

###############################################################################
class MongoctlConnectionError(DumpError, RetriableError):
    """
        Raised when mongoctl (used for dump) cannot connect to source
    """

###############################################################################
class CursorDoesNotExistError(DumpError, RetriableError):
    pass

###############################################################################
class DumpConnectivityError(DumpError, RetriableError):
    pass

###############################################################################
class DBClientCursorFailError(DumpError, RetriableError):
    pass

###############################################################################
class ArchiveError(MBSError):
    """
        Base error for archive errors
    """
    def __init__(self, tar_cmd, return_code, cmd_output, cause):
        msg = "Failed to zip and compress your backup"
        details = ("Failed to tar. Tar command '%s' returned a non-zero "
                   "exit status %s. Command output:\n%s" %
                   (tar_cmd, return_code, cmd_output))
        super(ArchiveError, self).__init__(msg=msg, details=details,
                                           cause=cause)

###############################################################################
class NoSpaceLeftError(ArchiveError):
    pass


###############################################################################
class SourceDataSizeExceedsLimits(MBSError):
    """
        Raised when source data size exceeds the limit defined in the strategy
    """
    def __init__(self, data_size, max_size, database_name=None):
        if database_name:
            db_str = "database '%s'" % database_name
        else:
            db_str = "all databases"
        msg = ("Data size of %s (%s bytes) exceeds the maximum limit "
               "(%s bytes)" % (db_str, data_size, max_size))

        super(SourceDataSizeExceedsLimits, self).__init__(msg=msg)

###############################################################################
class TargetError(MBSError):
    """
        Base type for target errors
    """

###############################################################################
class TargetConnectionError(TargetError, RetriableError):
    def __init__(self, container_name, cause=None):
        msg = ("Could not connect to cloud storage "
               "container '%s'" % container_name)
        super(TargetConnectionError, self).__init__(msg, cause=cause)

###############################################################################
class TargetUploadError(TargetError):

    ###########################################################################
    def __init__(self, destination_path, container_name, cause=None):
        msg = ("Failed to to upload your backup to cloud storage "
               "container '%s'" % (container_name))
        super(TargetUploadError, self).__init__(msg, cause=cause)


###############################################################################
class UploadedFileAlreadyExistError(TargetError):
    """
        Raised when the uploaded file already exists in container and
        overwrite_existing is set to False
    """

###############################################################################
class UploadedFileDoesNotExistError(TargetUploadError, RetriableError):

    ###########################################################################
    def __init__(self, destination_path, container_name):
        TargetUploadError.__init__(self, destination_path, container_name)
        self._details = ("Failure during upload verification: File '%s' does"
                         "not exist in container '%s'" %
                        (destination_path, container_name))

###############################################################################
class UploadedFileSizeMatchError(TargetUploadError, RetriableError):

    ###########################################################################
    def __init__(self, destination_path, container_name, dest_size, file_size):
        TargetUploadError.__init__(self, destination_path, container_name)
        self._details = ("Failure during upload verification: File '%s' size"
                         " in container '%s' (%s bytes) does not match size on"
                         " disk (%s bytes)" %
                         (destination_path, container_name, dest_size,
                          file_size))

###############################################################################
class TargetDeleteError(TargetError, RetriableError):
    pass

###############################################################################
class TargetFileNotFoundError(TargetError):
    pass


###############################################################################
class RetentionPolicyError(MBSError):
    """
        Thrown when there is an error when applying retention policy error
    """

###############################################################################
class DumpNotOnLocalhost(MBSError, RetriableError):
    """
        Thrown when strategy.ensureLocalHost is set and dump runs on a host
        that is not localhost
    """

###############################################################################
# Block Storage Snapshot Errors
###############################################################################
class BlockStorageSnapshotError(MBSError):
    """
        Base classes for all volume snapshot errors
    """


###############################################################################
# Dynamic Tag Errors
###############################################################################
class TagError(MBSError):
    """
        Base classes for all volume snapshot errors
    """


###############################################################################
# Plan Errors
###############################################################################
class PlanError(MBSError):
    """
        Base classes for all plan errors
    """

###############################################################################
# Invalid Plan Error
###############################################################################
class InvalidPlanError(PlanError):
    """
        raised by manager when plan config is invalid
    """

###############################################################################
# UTILITY ERROR METHODS
###############################################################################
def is_connection_exception(exception):
    msg = str(exception)
    return ("timed out" in msg or "refused" in msg or "reset" in msg or
            "Broken pipe" in msg or "closed" in msg)


###############################################################################
def is_exception_retriable(exception):
    return isinstance(exception, RetriableError)

###############################################################################

def raise_if_not_retriable(exception):
    if is_exception_retriable(exception):
        logger.warn("Caught a retriable exception: %s" % exception)
    else:
        logger.debug("Re-raising a a NON-retriable exception: %s" % exception)
        raise

###############################################################################
def raise_exception():
    raise

