__author__ = 'abdul'

import os
import shutil
import logging

from utils import ensure_dir, which, execute_command, execute_command_wrapper
import errors
from subprocess import CalledProcessError
from target import multi_target_upload_file

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
    def dump_command(self, backup, command):
        pass

    ####################################################################################################################
    def tar_gzip_command(self, backup, command):
        pass

    ####################################################################################################################
    def upload_dump(self, backup):
        pass

    ####################################################################################################################
    def upload_file(self, file_path, target, destination_path=None, overwrite_existing=True):
        pass

    ####################################################################################################################
    def multi_upload_file(self, all_targets, tar_file_path, destination_path=None,
                          overwrite_existing=True, metadata=None):
        pass

    ####################################################################################################################
    def ensure_dir(self, dir_path):
        pass





class LocalBackupAssistant(object):
    """
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
    def dump_command(self, dump_cmd, log_path):
        dump_cmd[0] = which("mongoctl")
        # execute dump command
        return execute_command_wrapper(dump_cmd, output_path=log_path)

    ####################################################################################################################
    def upload_file(self, file_path, target, destination_path=None, overwrite_existing=True):
        return target.put_file(file_path, destination_path=destination_path, overwrite_existing=overwrite_existing)

    ####################################################################################################################
    def multi_upload_file(self, all_targets, tar_file_path, destination_path=None,
                          overwrite_existing=True, metadata=None):
        uploaders = multi_target_upload_file(all_targets,
                                             tar_file_path,
                                             destination_path=destination_path,
                                             overwrite_existing=overwrite_existing,
                                             metadata=metadata)

        errored_uploaders = filter(lambda uploader: uploader.error is not None,
                                   uploaders)

        if errored_uploaders:
            raise errored_uploaders[0].error

        return map(lambda uploader: uploader.target_reference, uploaders)

    ####################################################################################################################
    def tar_gzip_command(self, path, filename):
        tar_exe = which("tar")
        working_dir = os.path.dirname(path)
        target_dirname = os.path.basename(path)

        tar_cmd = [tar_exe, "-cvzf", filename, target_dirname]
        cmd_display = " ".join(tar_cmd)

        try:
            logger.info("Running tar command: %s" % cmd_display)
            execute_command(tar_cmd, cwd=working_dir)

        except CalledProcessError, e:
            if "No space left on device" in e.output:
                error_type = errors.NoSpaceLeftError
            else:
                error_type = errors.ArchiveError

            raise error_type(cmd_display, e.returncode, e.output, e)

    ####################################################################################################################
    def upload_dump(self, backup):
        pass

    ####################################################################################################################
    def tail_command(self, path, lines=1):
        # read the last dump log line
        last_line_tail_cmd = [which('tail'), "-%s" % lines, path]
        return execute_command(last_line_tail_cmd)

    ####################################################################################################################
    def ensure_dir(self, dir_path):
        ensure_dir(dir_path)