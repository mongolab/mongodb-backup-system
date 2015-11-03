__author__ = 'abdul'

import os
import shutil
import logging

from utils import ensure_dir, which, execute_command, execute_command_wrapper, listify, list_dir_subdirs
import errors
from subprocess import CalledProcessError
from target import multi_target_upload_file
from errors import MBSError, ExtractError, RestoreError
from mongo_uri_tools import mask_mongo_uri
from base import MBSObject
from backup import Backup
from restore import Restore

###############################################################################
# Logger
###############################################################################
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

class BackupAssistant(MBSObject):
    """
    Object responsible for assisting backups with running certain commands on the box
    """

    ####################################################################################################################
    def create_task_workspace(self, task):
        """

        """
    ####################################################################################################################
    def delete_task_workspace(self, task):
        """
        """

    ####################################################################################################################
    def dump_backup(self, backup, uri, destination, log_file_name, options=None):
        pass

    ####################################################################################################################
    def upload_backup_log_file(self, backup, file_name, dump_dir, target, destination_path=None):
        pass

    ####################################################################################################################
    def tar_backup(self, backup, dump_dir, file_name):
        pass

    ####################################################################################################################
    def upload_backup(self, backup, file_name, target, destination_path=None):
        pass

    ####################################################################################################################
    def suspend_io(self, backup, mongo_connector, cloud_block_storage):
        pass

    ####################################################################################################################
    def resume_io(self, backup, mongo_connector, cloud_block_storage):
        pass

    ####################################################################################################################
    def download_restore_source_backup(self, restore):
        pass

    ####################################################################################################################
    def extract_restore_source_backup(self, restore):
        pass

    ####################################################################################################################
    def run_mongo_restore(self, restore, destination_uri, dump_dir, source_database_name,
                          log_file_name, dump_log_file_name, delete_old_users_file=None,
                          delete_old_admin_users_file=None,
                          options=None):
        pass

    ####################################################################################################################
    def is_connector_local_to_assistant(self, mongo_connector, backup):
        pass

    ####################################################################################################################
    def to_document(self, display_only=False):
        return {
            "_type": self.full_type_name
        }


########################################################################################################################
# LocalBackupAssistant
########################################################################################################################
class LocalBackupAssistant(BackupAssistant):
    """
    Basic impl locally
    """
    ####################################################################################################################
    def __init__(self):
        super(LocalBackupAssistant, self).__init__()
        self._temp_dir = None

    ####################################################################################################################
    @property
    def temp_dir(self):
        return self._temp_dir

    @temp_dir.setter
    def temp_dir(self, val):
        self._temp_dir = val

    ####################################################################################################################
    def create_task_workspace(self, task):
        """

        """
        # ensure task workspace
        try:
            workspace_dir = self.get_task_workspace_dir(task)
            ensure_dir(workspace_dir)
        except Exception, e:
            raise errors.WorkspaceCreationError("Failed to create workspace: %s" % e)

    ####################################################################################################################
    def delete_task_workspace(self, task):
        """
        """
        workspace = self.get_task_workspace_dir(task)

        try:

            if os.path.exists(workspace):
                shutil.rmtree(workspace)
            else:
                logger.error("workspace dir %s does not exist!" % workspace)
        except Exception, e:
            logger.exception("Cleanup error for task '%s': %s" % (task.id,
                                                                  e))

    ####################################################################################################################
    def dump_backup(self, backup, uri, destination, log_file_name, options=None):
        mongoctl_exe = which("mongoctl")
        if not mongoctl_exe:
            raise MBSError("mongoctl exe not found in PATH")

        dump_cmd = [mongoctl_exe, "--noninteractive", "dump", uri, "-o", destination]

        if options:
            dump_cmd.extend(options)

        dump_cmd_display = dump_cmd[:]
        # mask mongo uri
        dump_cmd_display[3] = mask_mongo_uri(uri)

        logger.info("Running dump command: %s" % " ".join(dump_cmd_display))

        workspace = self.get_task_workspace_dir(backup)
        log_path = os.path.join(workspace, destination, log_file_name)
        last_error_line = {"line": ""}

        def capture_last_error(line):
            if is_mongo_error_log_line(line):
                last_error_line["line"] = line
        # execute dump command
        return_code = execute_command_wrapper(dump_cmd, cwd=workspace, output_path=log_path,
                                             on_output=capture_last_error)

        # raise an error if return code is not 0
        if return_code:
            errors.raise_dump_error(return_code, last_error_line["line"])

    ####################################################################################################################
    def upload_backup_log_file(self, backup, file_name, dump_dir, target, destination_path=None):
        workspace = self.get_task_workspace_dir(backup)
        file_path = os.path.join(workspace, dump_dir, file_name)
        return target.put_file(file_path, destination_path=destination_path)

    ####################################################################################################################
    def tar_backup(self, backup, dump_dir, file_name):
        tar_exe = which("tar")

        tar_cmd = [tar_exe, "-cvzf", file_name, dump_dir]
        cmd_display = " ".join(tar_cmd)
        workspace = self.get_task_workspace_dir(backup)
        try:
            logger.info("Running tar command: %s" % cmd_display)
            execute_command(tar_cmd, cwd=workspace)
            self._delete_dump_dir(backup, dump_dir)
        except CalledProcessError, e:
            last_log_line = e.output.split("\n")[-1]
            errors.raise_archive_error(e.returncode, last_log_line)


    ####################################################################################################################
    def upload_backup(self, backup, file_name, target, destination_path=None):
        targets = listify(target)
        workspace = self.get_task_workspace_dir(backup)
        file_path = os.path.join(workspace, file_name)
        metadata = {
            "Content-Type": "application/x-compressed"
        }
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

    ####################################################################################################################
    def suspend_io(self, backup, mongo_connector, cloud_block_storage):
        cloud_block_storage.suspend_io()

    ####################################################################################################################
    def resume_io(self, backup, mongo_connector, cloud_block_storage):
        cloud_block_storage.resume_io()

    ####################################################################################################################
    def _delete_dump_dir(self, backup, dump_dir):
        workspace = self.get_task_workspace_dir(backup)
        dump_dir_path = os.path.join(workspace, dump_dir)
        # delete the temp dir
        logger.info("Deleting dump dir %s" % dump_dir_path)

        try:

            if os.path.exists(dump_dir_path):
                shutil.rmtree(dump_dir_path)
            else:
                logger.error("dump dir %s does not exist!" % dump_dir_path)
        except Exception, e:
            logger.error("Error while deleting dump dir for backup '%s': %s" %
                         (backup.id, e))

    ####################################################################################################################
    def download_restore_source_backup(self, restore):
        backup = restore.source_backup
        workspace = self.get_task_workspace_dir(restore)
        file_reference = backup.target_reference
        logger.info("Downloading restore '%s' dump tar file '%s'" %
                    (restore.id, file_reference.file_name))

        backup.target.get_file(file_reference, workspace)

    ####################################################################################################################
    def extract_restore_source_backup(self, restore):
        working_dir = self.get_task_workspace_dir(restore)
        file_reference = restore.source_backup.target_reference
        logger.info("Extracting tar file '%s'" % file_reference.file_name)

        tarx_cmd = [
            which("tar"),
            "-xf",
            file_reference.file_name
        ]

        logger.info("Running tar extract command: %s" % tarx_cmd)
        try:
            execute_command(tarx_cmd, cwd=working_dir)
        except CalledProcessError, cpe:
            logger.error("Failed to execute extract command: %s" % tarx_cmd)
            raise ExtractError(cause=cpe)

    ####################################################################################################################
    def run_mongo_restore(self, restore, destination_uri, dump_dir, source_database_name,
                          log_file_name, dump_log_file_name,
                          delete_old_users_file=None,
                          delete_old_admin_users_file=None,
                          options=None):

        if source_database_name:
            source_dir = os.path.join(dump_dir, source_database_name)
        else:
            source_dir = dump_dir

        workspace = self.get_task_workspace_dir(restore)
        # IMPORTANT delete dump log file so the restore command would not break
        dump_log_path = os.path.join(workspace, dump_dir, dump_log_file_name)
        if os.path.exists(dump_log_path):
            os.remove(dump_log_path)

        if delete_old_users_file or delete_old_admin_users_file:
            self._delete_restore_old_users_files(restore, source_dir, include_admin=delete_old_admin_users_file)

        working_dir = workspace
        log_path = os.path.join(workspace, log_file_name)

        restore_cmd = [
            which("mongoctl"),
            "restore",
            destination_uri,
            source_dir
        ]

        if options:
            restore_cmd.extend(options)

        restore_cmd_display = restore_cmd[:]

        restore_cmd_display[restore_cmd_display.index("restore") + 1] = mask_mongo_uri(destination_uri)

        logger.info("Running mongoctl restore command: %s" %
                    " ".join(restore_cmd_display))

        returncode = execute_command_wrapper(restore_cmd,
                                             output_path=log_path,
                                             cwd=working_dir)

        # read the last dump log line
        last_line_tail_cmd = [which('tail'), '-1', log_path]
        last_log_line = execute_command(last_line_tail_cmd)

        if returncode:
            raise RestoreError(returncode, last_log_line)

    ####################################################################################################################
    def _delete_restore_old_users_files(self, restore, restore_source_dir, include_admin=False):
        workspace = self.get_task_workspace_dir(restore)
        restore_source_path = os.path.join(workspace, restore_source_dir)

        db_dirs = list_dir_subdirs(restore_source_path)
        for db_dir in db_dirs:
            if db_dir == "admin" and not include_admin:
                continue
            db_dir_path = os.path.join(restore_source_path, db_dir)
            bson_file = os.path.join(db_dir_path, "system.users.bson")
            json_md_file = os.path.join(db_dir_path, "system.users.metadata.json")
            if os.path.exists(bson_file):
                logger.info("2.6 Restore workaround: Deleting old "
                            "system.users bson file '%s'" % bson_file)
                os.remove(bson_file)
            if os.path.exists(json_md_file):
                logger.info("2.6 Restore workaround: Deleting old system."
                            "users.metadata.json file '%s'" % json_md_file)
                os.remove(json_md_file)

    ####################################################################################################################
    def is_connector_local_to_assistant(self, mongo_connector, backup):
        return mongo_connector.is_local()

    ####################################################################################################################
    def get_task_workspace_dir(self, task):
        if isinstance(task, Backup):
            subdir = "backups"
        elif isinstance(task, Restore):
            subdir = "restores"
        else:
            raise Exception("Unknown task type")

        return os.path.join(self.temp_dir, subdir, str(task.id))


####################################################################################################################
def is_mongo_error_log_line(line):
    if line:
        line = line.lower()
        return "assertion:" in line or "runtime error:" in line or "failed:" in line
