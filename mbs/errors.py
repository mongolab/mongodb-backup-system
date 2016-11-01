__author__ = 'abdul'

import mongo_uri_tools
from pymongo.errors import ConnectionFailure
from boto.exception import BotoServerError
from base import MBSObject
import logging
import utils

###############################################################################
########################                       ################################
########################  Backup System Errors ################################
########################                       ################################
###############################################################################



###############################################################################
# LOGGER
###############################################################################

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

###############################################################################
# MBSError
###############################################################################
class MBSError(Exception, MBSObject):
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

    @message.setter
    def message(self, m):
        self._message = m

    ###########################################################################
    @property
    def cause(self):
        return self._cause

    @cause.setter
    def cause(self, c):
        self._cause = c

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

    ###########################################################################
    def to_document(self, display_only=False):
        doc = {
            "_type": self.full_type_name,
            "message": self.message
        }

        if self.cause:
            if isinstance(self.cause, MBSError):
                doc["cause"] = self.cause.to_document(display_only=display_only)
            else:
                doc["cause"] = {
                    "causeType": utils.object_full_type_name(self.cause),
                    "message": utils.safe_stringify(self.cause)
                }

        return doc


###############################################################################
class RetriableError(Exception):
    """
        Base class for ALL retriable errors. All retriable errors should
        inherit this class
    """

###############################################################################
# MBSErrorWrapper
###############################################################################
class MBSErrorWrapper(MBSError):
    """
    Used for wrapping generic exceptions that are non-mbs exceptions
    """


###############################################################################
# BackupSystemError
###############################################################################
class BackupSystemError(MBSError):
    pass

###############################################################################
# BackupSchedulingError
###############################################################################
class BackupSchedulingError(BackupSystemError):
    pass

###############################################################################
# CreatePlanError
###############################################################################
class CreatePlanError(BackupSystemError):
    pass

###############################################################################
# BackupEngineError
###############################################################################
class BackupEngineError(MBSError):
    pass


###############################################################################
# EngineCrashedError
###############################################################################
class EngineCrashedError(BackupEngineError):
    pass

###############################################################################
# EngineWorkerCrashedError
###############################################################################
class EngineWorkerCrashedError(BackupEngineError, RetriableError):
    pass

###############################################################################
# ConfigurationError
###############################################################################
class ConfigurationError(MBSError):
    pass

###############################################################################

class ConnectionError(MBSError, RetriableError):
    """
        Base error for connection errors
    """
    ###########################################################################
    def __init__(self, uri=None, details=None, cause=None):
        msg = "Could not establish a database connection to '%s'" % uri
        super(ConnectionError, self).__init__(msg=msg, details=details, cause=cause)

###############################################################################
class AuthenticationFailedError(MBSError):

    ###########################################################################
    def __init__(self, uri=None, cause=None):
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
    def __init__(self, msg=None, details=None, cause=None):
        msg = msg or "Replicaset Error"
        super(ReplicasetError, self).__init__(msg=msg, details=details,
                                              cause=cause)

###############################################################################
class PrimaryNotFoundError(ReplicasetError):

    ###########################################################################
    def __init__(self, uri=None):
        details = "Unable to determine primary for cluster '%s'" % uri
        super(PrimaryNotFoundError, self).__init__(details=details)

###############################################################################
class NoEligibleMembersFound(ReplicasetError):

    ###########################################################################
    def __init__(self, uri=None, msg=None, rs_status=None, rs_conf=None):
        if uri:
            details = ("No eligible members in '%s' found to take backup from" %
                       mongo_uri_tools.mask_mongo_uri(uri))
        else:
            details = None

        self._rs_status = rs_status
        self._rs_conf = rs_conf

        super(NoEligibleMembersFound, self).__init__(details=details, msg=msg)

    ###########################################################################
    @property
    def rs_status(self):
        return self._rs_status

    @rs_status.setter
    def rs_status(self, val):
        self._rs_status = val

    ###########################################################################
    @property
    def rs_conf(self):
        return self._rs_conf

    @rs_conf.setter
    def rs_conf(self, val):
        self._rs_conf = val

    ####################################################################################################################
    def to_document(self, display_only=False):
        doc = super(NoEligibleMembersFound, self).to_document(display_only=display_only)
        doc.update({
            "rsStatus": self.rs_status,
            "rsConf": self.rs_conf
        })

        return doc

###############################################################################
class DBStatsError(MBSError):
    """
        Raised on dbstats command error
    """

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
    def __init__(self, return_code=None, error_log_line=None, last_namespace=None):
        msg = "Failed to mongodump"
        details = ("Failed to dump. Dump command returned a non-zero "
                   "exit status %s.Check dump logs. mongodump error log: "
                   "%s" % (return_code, error_log_line))
        self._return_code = return_code
        self._error_log_line = error_log_line
        self._last_namespace = last_namespace
        super(DumpError, self).__init__(msg=msg, details=details)

    ###########################################################################
    @property
    def return_code(self):
        return self._return_code

    @return_code.setter
    def return_code(self, val):
        self._return_code = val

    ###########################################################################
    @property
    def error_log_line(self):
        return self._error_log_line

    @error_log_line.setter
    def error_log_line(self, val):
        self._error_log_line = val

    ###########################################################################
    @property
    def last_namespace(self):
        return self._last_namespace

    @last_namespace.setter
    def last_namespace(self, val):
        self._last_namespace = val

    ###########################################################################
    def to_document(self, display_only=False):
        doc = super(DumpError, self).to_document(display_only=display_only)
        # truncate log line
        error_line = self.error_log_line
        if error_line and len(error_line) > 1000:
            error_line = self.error_log_line[:1000]

        doc.update({
            "returnCode": self.return_code,
            "errorLogLine": error_line,
            "lastNamespace": self.last_namespace
        })

        return doc

###############################################################################
class RetriableDumpError(DumpError, RetriableError):
    pass

###############################################################################
class BadCollectionNameError(DumpError):
    """
        Raised when a database contains bad collection names such as the ones
        containing "/"
    """
    ###########################################################################
    def __init__(self, return_code=None, error_log_line=None, last_namespace=None):
        super(BadCollectionNameError, self).__init__(return_code=return_code,
                                                     error_log_line=error_log_line,
                                                     last_namespace=last_namespace)
        self._message = ("Failed to mongodump... possibly because you "
                         "have collection name(s) with invalid "
                         "characters (e.g. '/'). If so, please rename or "
                         "drop these collection(s)")

###############################################################################
class InvalidBSONObjSizeError(RetriableDumpError):
    pass

###############################################################################
class CorruptionError(RetriableDumpError):
    pass

###############################################################################
class CappedCursorOverrunError(RetriableDumpError):
    pass

###############################################################################
class IndexOutOfRangeDumpError(RetriableDumpError):
    pass

###############################################################################
class InvalidDBNameError(DumpError):

    ###########################################################################
    def __init__(self, return_code=None, error_log_line=None, last_namespace=None):
        super(InvalidDBNameError, self).__init__(return_code=return_code,
                                                 error_log_line=error_log_line,
                                                 last_namespace=last_namespace)
        self._message = ("Failed to mongodump because the name of your "
                         "database is invalid")

###############################################################################
class BadTypeError(RetriableDumpError):
    pass

###############################################################################
class ExhaustReceiveError(RetriableDumpError):
    pass

###############################################################################
class MongoctlConnectionError(RetriableDumpError):
    """
        Raised when mongoctl (used for dump) cannot connect to source
    """

###############################################################################
class CursorDoesNotExistError(RetriableDumpError):
    pass

###############################################################################
class DumpConnectivityError(RetriableDumpError):
    pass

###############################################################################
class DBClientCursorFailError(RetriableDumpError):
    pass

###############################################################################
class CollectionReadError(RetriableDumpError):
    pass


###############################################################################
class OplogOverflowError(RetriableDumpError):
    pass

########################################################################################################################
class OverlappingBackupError(RetriableError):
    pass

########################################################################################################################
class OverlappingMongodumpError(OverlappingBackupError):
    pass

###############################################################################
class ArchiveError(MBSError):
    """
        Base error for archive errors
    """
    def __init__(self, return_code=None, last_log_line=None):
        self._return_code = return_code
        self._last_log_line = last_log_line
        msg = "Failed to zip and compress your backup"
        details = "Failed to tar. Tar command returned a non-zero exit status %s" % return_code
        super(ArchiveError, self).__init__(msg=msg, details=details)

    ###########################################################################
    @property
    def return_code(self):
        return self._return_code

    @return_code.setter
    def return_code(self, val):
        self._return_code = val

    ###########################################################################
    @property
    def last_log_line(self):
        return self._last_log_line

    @last_log_line.setter
    def last_log_line(self, val):
        self._last_log_line = val

    ###########################################################################
    def to_document(self, display_only=False):
        doc = super(ArchiveError, self).to_document(display_only=display_only)
        doc["returnCode"] = self.return_code
        doc["lastLogLine"] = self.last_log_line

        return doc

###############################################################################
class NoSpaceLeftError(MBSError):
    """
    raised when there is no disk space left
    """


###############################################################################
class SourceDataSizeExceedsLimits(MBSError):
    """
        Raised when source data size exceeds the limit defined in the strategy
    """
    def __init__(self, data_size=None, max_size=None, database_name=None):
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
class TargetInaccessibleError(TargetError):
    def __init__(self, container_name=None, cause=None):
        msg = ("Cloud storage container %s is inaccessible or "
               "unidentifiable, potentially due to out-of-date "
               "target configuration.\n%s" % (container_name,
                                              cause))
        super(TargetInaccessibleError, self).__init__(msg=msg,
                                                      cause=cause)

###############################################################################
class NoSuchContainerError(TargetError):
    """
        Raised when the container does not exist
    """
    def __init__(self, container_name=None, cause=None):
        msg = ("No such Cloud storage container %s .\n%s" % (container_name, cause))
        super(NoSuchContainerError, self).__init__(msg=msg, cause=cause)

###############################################################################
class TargetConnectionError(TargetError, RetriableError):
    def __init__(self, container_name=None, cause=None):
        msg = ("Could not connect to cloud storage "
               "container '%s'" % container_name)
        super(TargetConnectionError, self).__init__(msg=msg, cause=cause)

###############################################################################
class TargetUploadError(TargetError):

    ###########################################################################
    def __init__(self, destination_path=None, container_name=None, cause=None):
        msg = ("Failed to to upload your backup to cloud storage "
               "container '%s'" % (container_name))
        super(TargetUploadError, self).__init__(msg=msg, cause=cause)


###############################################################################
class UploadedFileAlreadyExistError(TargetError):
    """
        Raised when the uploaded file already exists in container and
        overwrite_existing is set to False
    """

###############################################################################
class UploadedFileDoesNotExistError(TargetUploadError, RetriableError):

    ###########################################################################
    def __init__(self, destination_path=None, container_name=None):
        TargetUploadError.__init__(self, destination_path=destination_path,
                                   container_name=container_name)
        self._details = ("Failure during upload verification: File '%s' does"
                         "not exist in container '%s'" %
                        (destination_path, container_name))

###############################################################################
class UploadedFileSizeMatchError(TargetUploadError, RetriableError):

    ###########################################################################
    def __init__(self, destination_path=None, container_name=None, dest_size=None, file_size=None):
        TargetUploadError.__init__(self, destination_path=destination_path, container_name=container_name)
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
class BackupNotOnLocalhost(MBSError, RetriableError):
    """
        Raised when strategy.ensureLocalHost is set and dump runs on a host
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
class SnapshotDidNotSucceedError(MBSError, RetriableError):
    """
        Thrown when snapshot status becomes 'error'
    """

###############################################################################
#Ec2SnapshotDoesNotExistError
###############################################################################
class Ec2SnapshotDoesNotExistError(MBSError, RetriableError):
    """
        thrown when a snapshot does not exist anymore during ec2 snapshot check
        updates
    """

###############################################################################
class SnapshotDeleteError(BlockStorageSnapshotError, RetriableError):
    """
        Raised when there was an error while deleting a snapshot
    """

###############################################################################
# MongoLockError
###############################################################################
class MongoLockError(MBSError):
    """
        Raised when there is an fsynclock/fsyncunlock error
    """

###############################################################################
# MongoLockError
###############################################################################
class ServerAlreadyLockedError(MongoLockError, RetriableError):
    """
        Raised when attempting to lock an already locked server
    """

###############################################################################
# CbsIOError
###############################################################################
class CbsIOError(MBSError, RetriableError):
    """
    """

###############################################################################
# SuspendIOError
###############################################################################
class SuspendIOError(CbsIOError):
    """
        Raised when there is a suspend error
    """

###############################################################################
# ResumeIOError
###############################################################################
class ResumeIOError(CbsIOError):
    """
        Raised when there is a resume error
    """

###############################################################################
# VolumeError
###############################################################################
class VolumeError(MBSError):
    """
        Raised when there is a volume error
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
        raised by backup system when plan config is invalid
    """

###############################################################################
# UTILITY ERROR METHODS
###############################################################################
def is_connection_exception(exception):
    if isinstance(exception, ConnectionFailure):
        return True
    else:
        msg = utils.safe_stringify(exception)
        return ("timed out" in msg or "refused" in msg or "reset" in msg or
                "Broken pipe" in msg or "closed" in msg or "IncompleteRead" in msg)


###############################################################################
def is_exception_retriable(exception):
    return (isinstance(exception, RetriableError) or
            is_connection_exception(exception))

###############################################################################

def raise_if_not_retriable(exception):
    if is_exception_retriable(exception):
        logger.warn("Caught a retriable exception: %s" % exception)
    else:
        logger.debug("Re-raising a a NON-retriable exception: %s" % exception)
        raise

###############################################################################
def raise_if_not_ec2_retriable(exception):
    # retry on boto request limit and other ec2 errors
    msg = utils.safe_stringify(exception)
    if ((isinstance(exception, BotoServerError) and
         exception.status == 503) or "ConcurrentTagAccess" in msg):
        logger.warn("Caught a retriable exception: %s" % exception)
    else:
        raise_if_not_retriable(exception)

###############################################################################
def raise_exception():
    raise

###############################################################################
def swallow_exception():
    logger.exception("EXCEPTION")

###############################################################################
# Restore errors
###############################################################################


class RestoreError(MBSError):
    """
        Base error for dump errors
        IMPORTANT NOTE! note that all restore errors DOES NOT pass the cause since
        the cause is a CalledProcessError that contains the full un-censored
        dump command (which might contain username/password). It has been
        omitted to avoid logging credentials
    """
    ###########################################################################
    def __init__(self, return_code=None, last_log_line=None):
        msg = ("Failed to mongorestore")
        details = ("Failed to restore. restore command returned a non-zero "
                   "exit status %s.Check restore logs. Last restore log line: "
                   "%s" % (return_code, last_log_line))
        super(RestoreError, self).__init__(msg=msg, details=details)

###############################################################################
class ExtractError(MBSError):
    """
        Base error for archive errors
    """
    def __init__(self, cause=None):
        msg = "Failed to extract source backup"
        details = ("Failed to tar. Tar command returned a non-zero "
                   "exit status")
        super(ExtractError, self).__init__(msg=msg, details=details,
                                           cause=cause)

###############################################################################
class WorkspaceCreationError(MBSError, RetriableError):
    """
        happens when there is is a problem creating workspace for task
    """

###############################################################################
class BalancerActiveError(MBSError, RetriableError):
    pass

###############################################################################
# PlanGeneratorError
###############################################################################
class PlanGenerationError(MBSError):
    pass


###############################################################################
# BackupSweepError
###############################################################################
class BackupSweepError(MBSError):
    pass

###############################################################################
# BackupExpirationError
###############################################################################
class BackupExpirationError(MBSError):
    pass

###############################################################################
# MBSApiError class
###############################################################################
class MBSApiError(Exception):

    def __init__(self, message, status_code=None):
        Exception.__init__(self)
        self._message = message
        self._status_code = status_code or 400

    ###########################################################################
    @property
    def message(self):
        return self._message

    ###########################################################################
    @property
    def status_code(self):
        return self._status_code

    ###########################################################################
    def to_dict(self):
        return {
            "ok": 0,
            "error": self.message
        }

########################################################################################################################
# Error Utility functions
########################################################################################################################
def raise_dump_error(returncode, error_log_line, last_namespace=None):
    error_log_line = utils.safe_stringify(error_log_line)
    # encode error log line
    if (("Failed: error creating bson file" in error_log_line and
                 "no such file or directory" in error_log_line) or
        "contains a path separator" in error_log_line):
        error_type = BadCollectionNameError
    elif "10334" in error_log_line:
        if "BSONObj size: 0 (0x00000000)" in error_log_line:
            error_type = CorruptionError
        else:
            error_type = InvalidBSONObjSizeError
    elif "13338" in error_log_line:
        error_type = CappedCursorOverrunError
    elif "13280" in error_log_line:
        error_type = InvalidDBNameError
    elif "10320" in error_log_line:
        error_type = BadTypeError
    elif "Cannot connect" in error_log_line:
        error_type = MongoctlConnectionError
    elif "cursor didn't exist on server" in error_log_line:
        error_type = CursorDoesNotExistError
    elif "16465" in error_log_line:
        error_type = ExhaustReceiveError
    elif ("SocketException" in error_log_line or
          "socket error" in error_log_line or
          "transport error" in error_log_line or
          "no reachable servers" in error_log_line or
          "error connecting to db server" in error_log_line):
        error_type = DumpConnectivityError
    elif (("DBClientCursor" in error_log_line and "failed" in error_log_line) or
          "invalid cursor" in error_log_line or
          "Closed explicitly" in error_log_line):
        error_type = DBClientCursorFailError
    elif "index out of range" in error_log_line:
        error_type = IndexOutOfRangeDumpError
    elif "error reading collection" in error_log_line:
        error_type = CollectionReadError
    elif "oplog overflow" in error_log_line:
        error_type = OplogOverflowError

    # Generic retriable errors
    elif is_retriable_dump_error(returncode, error_log_line):
        error_type = RetriableDumpError
    else:
        error_type = DumpError

    raise error_type(returncode, error_log_line, last_namespace=last_namespace)

########################################################################################################################
def raise_archive_error(return_code, last_log_line):
    if "No space left on device" in last_log_line:
        raise NoSpaceLeftError("No disk space left on device")
    else:
        raise ArchiveError(return_code=return_code, last_log_line=last_log_line)

########################################################################################################################

RETRIABLE_DUMP_ERROR_PARTIALS = [
    "Err: EOF",
    "Err: no collection",
    "Err: no database",
    "Operation was interrupted"
]

########################################################################################################################
def is_retriable_dump_error(returncode, error_log_line):
    matches = filter(lambda p: p in error_log_line, RETRIABLE_DUMP_ERROR_PARTIALS)
    return matches and len(matches) > 0

########################################################################################################################
def to_mbs_error_code(error):
    if isinstance(error, MBSError):
        return error
    else:
        return MBSErrorWrapper(msg=utils.safe_stringify(error), cause=error)


