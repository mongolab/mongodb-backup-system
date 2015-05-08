__author__ = 'abdul'

import os
import shutil
import logging

from utils import ensure_dir, which, execute_command, execute_command_wrapper, listify
import errors
from subprocess import CalledProcessError
from target import multi_target_upload_file
from errors import MBSError
from mongo_uri_tools import mask_mongo_uri

###############################################################################
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

class BackupAssistant(object):
    """
    Object responsible for assisting backups with running certain commands on the box
    """
    ####################################################################################################################
    def __init__(self):
        pass

    ####################################################################################################################
    def create_task_workspace(self, task):
        """

        """
    ####################################################################################################################
    def delete_task_workspace(self, task):
        """
        """

    ####################################################################################################################
    def dump_backup(self, backup, uri, destination, options=None):
        pass

    ####################################################################################################################
    def tgz_backup(self, backup, dump_dir, file_name):
        pass

    ####################################################################################################################
    def upload_backup(self, backup, file_name, target, destination_path=None):
        pass

#########################################################################################################################
# LocalBackupAssistant
#########################################################################################################################
class LocalBackupAssistant(object):
    """
    Basic impl locally
    """
    ####################################################################################################################
    def __init__(self):
        pass

    ####################################################################################################################
    def create_task_workspace(self, task):
        """

        """
        # ensure task workspace
        try:
            ensure_dir(task.workspace)
        except Exception, e:
            raise errors.WorkspaceCreationError("Failed to create workspace: %s" % e)

    ####################################################################################################################
    def delete_task_workspace(self, task):
        """
        """
        workspace = task.workspace

        try:

            if os.path.exists(workspace):
                shutil.rmtree(workspace)
            else:
                logger.error("workspace dir %s does not exist!" % workspace)
        except Exception, e:
            logger.exception("Cleanup error for task '%s': %s" % (task.id,
                                                                  e))

    ####################################################################################################################
    def dump_backup(self, backup, uri, destination, options=None):
        mongoctl_exe = which("mongoctl")
        if not mongoctl_exe:
            raise MBSError("mongoctl exe not found in PATH")

        dump_cmd = [mongoctl_exe, "--noninteractive", "dump", uri, "-o", destination]

        if options:
            dump_cmd.extend(options)

        dump_cmd_display= dump_cmd[:]
        # mask mongo uri
        dump_cmd_display[3] = mask_mongo_uri(uri)

        logger.info("Running dump command: %s" % " ".join(dump_cmd_display))

        # execute dump command
        returncode = execute_command_wrapper(dump_cmd, cwd=backup.workspace)
        # TODO grab last dump line
        last_dump_line = ""
        # raise an error if return code is not 0
        if returncode:
            errors.raise_dump_error(returncode, last_dump_line)

    ####################################################################################################################
    def tgz_backup(self, backup, dump_dir, file_name):
        tar_exe = which("tar")

        tar_cmd = [tar_exe, "-cvzf", file_name, dump_dir]
        cmd_display = " ".join(tar_cmd)

        try:
            logger.info("Running tar command: %s" % cmd_display)
            execute_command(tar_cmd, cwd=backup.workspace)

        except CalledProcessError, e:
            if "No space left on device" in e.output:
                error_type = errors.NoSpaceLeftError
            else:
                error_type = errors.ArchiveError

            raise error_type(cmd_display, e.returncode, e.output, e)

    ####################################################################################################################
    def upload_backup(self, backup, file_name, target, destination_path=None):
        targets = listify(target)
        file_path = os.path.join(backup.workspace, file_name)

        uploaders = multi_target_upload_file(targets, file_path, destination_path=destination_path, metadata=metadata)

        errored_uploaders = filter(lambda uploader: uploader.error is not None,
                                   uploaders)

        if errored_uploaders:
            raise errored_uploaders[0].error

        target_references = map(lambda uploader: uploader.target_reference, uploaders)

        if isinstance(target, list):
            return target_references
        else:
            return target_references[0]
