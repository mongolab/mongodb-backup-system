__author__ = 'abdul'

from netutils import fetch_url_json
from errors import BackupSystemClientError
###############################################################################
# CONSTANTS
###############################################################################
DEFAULT_BS_URL = "http://localhost:9003"

BACKUP_SYSTEM_STATUS_RUNNING = "running"
BACKUP_SYSTEM_STATUS_STOPPING = "stopping"
BACKUP_SYSTEM_STATUS_STOPPED = "stopped"

###############################################################################
# BackupSystemClient
###############################################################################


class BackupSystemClient(object):

    ###########################################################################
    def __init__(self, backup_system_url=None):
        self._url = backup_system_url or DEFAULT_BS_URL

    ###########################################################################
    # CLIENT METHODS
    ###########################################################################
    def get_status(self):
        try:
            return self._execute_command("status")
        except IOError:
            return {
                "status": BACKUP_SYSTEM_STATUS_STOPPED
            }
        except Exception, e:
            msg = "Error while trying to get backup system status: %s" % e
            raise BackupSystemClientError(msg)

    ###########################################################################
    def stop_backup_system(self):
        return self._execute_command("stop")

    ###########################################################################
    def delete_backup(self, backup_id):
        return self._execute_command("delete-backup/%s" % backup_id)

    ###########################################################################
    def restore_backup(self, backup_id, destination_uri,
                       source_database_name=None):
        data = {
            "backupId": backup_id,
            "destinationUri": destination_uri
        }

        if source_database_name:
            data["sourceDatabaseName"] = source_database_name

        return self._execute_command("restore-backup", method="POST",
                                     data=data)

    ###########################################################################
    def get_destination_restore_status(self, destination_uri):
        params = {
            "destinationUri": destination_uri
        }

        return self._execute_command("get-destination-restore-status",
                                     method="GET", params=params)

    ###########################################################################
    # HELPERS
    ###########################################################################
    def _execute_command(self, command, params=None, data=None, method=None):
        url = self._command_url(command, params=params)
        return fetch_url_json(url=url, method=method, data=data)

    ###########################################################################
    def _command_url(self, command, params=None):
        url = self._url
        if not url.endswith("/"):
            url += "/"
        url += command

        if params:
            url += "?"
            for name,val in params.items():
                url += "%s=%s" % (name, val)
        return url

    ###########################################################################

