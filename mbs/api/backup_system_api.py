__author__ = 'abdul'

import logging
import traceback
import threading


from api_server import ApiServer
from api_utils import send_api_error, error_response, get_request_json, new_request_id, get_request_value


from flask.globals import request
from mbs.utils import document_pretty_string, get_local_host_name

from mbs.netutils import crossdomain
from functools import update_wrapper

from mbs.mbs import get_mbs

from mbs import persistence


from mbs import date_utils

###############################################################################
# LOGGER
###############################################################################
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

DEFAULT_NUM_WORKERS = 20

HOST_NAME = get_local_host_name()
###############################################################################
# BackupSystemApiServer
###############################################################################
class BackupSystemApiServer(ApiServer):


    ###########################################################################
    @property
    def backup_system(self):
        return get_mbs().backup_system

    ###########################################################################
    def get_backup(self, backup_id):
        try:
            backup = persistence.get_backup(backup_id)
            return str(backup)
        except Exception, e:
            msg = "Error while trying to get backup %s: %s" % (backup_id, e)
            logger.error(msg)
            logger.error(traceback.format_exc())
            send_api_error("get-backup", e)
            return error_response(msg)

    ###########################################################################
    def get_backup_database_names(self, backup_id):
        try:
            dbnames = self.backup_system.get_backup_database_names(backup_id)
            return document_pretty_string(dbnames)
        except Exception, e:
            msg = ("Error while trying to get backup database"
                   " names %s: %s" %(backup_id, e))
            logger.error(msg)
            logger.error(traceback.format_exc())
            send_api_error("get-backup-database-names", e)
            return error_response(msg)

    ###########################################################################
    def expire_backup(self, backup_id):
        try:
            exp_man = self.backup_system.backup_expiration_manager
            backup = persistence.get_backup(backup_id)
            result = exp_man.expire_backup(backup, force=True)
            return document_pretty_string(result)
        except Exception, e:
            msg = ("Error while trying to expire backup %s: %s" %
                   (backup_id, e))
            logger.error(msg)
            logger.error(traceback.format_exc())
            send_api_error("expire-backup", e)
            return error_response(msg)


    ###########################################################################
    def delete_backup_plan(self, plan_id):
        try:
            result = self.backup_system.remove_plan(plan_id)
            return document_pretty_string(result)
        except Exception, e:
            msg = ("Error while trying to delete backup plan %s: %s" %
                   (plan_id, e))
            logger.error(msg)
            logger.error(traceback.format_exc())
            send_api_error("delete-backup-plan", e)
            return error_response(msg)

    ###########################################################################
    def restore_backup(self):
        arg_json = get_request_json()
        backup_id = arg_json.get('backupId')
        destination_uri = arg_json.get('destinationUri')
        no_index_restore = arg_json.get('noIndexRestore')
        no_users_restore = arg_json.get('noUsersRestore')
        no_roles_restore = arg_json.get('noRolesRestore')
        tags = arg_json.get('tags')
        source_database_name = arg_json.get('sourceDatabaseName')
        try:
            bs = self.backup_system
            r = bs.schedule_backup_restore(backup_id,
                                           destination_uri,
                                           source_database_name=source_database_name,
                                           no_index_restore=no_index_restore,
                                           no_users_restore=no_users_restore,
                                           no_roles_restore=no_roles_restore,
                                           tags=tags)
            return str(r)
        except Exception, e:
            msg = "Error while trying to restore backup %s: %s" % (backup_id,
                                                                    e)
            logger.error(msg)
            logger.error(traceback.format_exc())
            send_api_error("restore-backup", e)
            return error_response(msg)


    ###########################################################################
    def get_destination_restore_status(self):
        destination_uri = request.args.get('destinationUri')
        try:
            status = self.backup_system.get_destination_restore_status(
                destination_uri)
            return document_pretty_string({
                "status": status
            })
        except Exception, e:
            msg = ("Error while trying to get restore status for"
                   " destination '%s': %s" % (destination_uri, e))
            logger.error(msg)
            logger.error(traceback.format_exc())
            send_api_error("get-destination-restore-status", e)

            return error_response(msg)

    ###########################################################################
    def build_flask_server(self, flask_server):

        # call super
        super(BackupSystemApiServer, self).build_flask_server(flask_server)

        # build custom
        @flask_server.after_request
        def add_default_response_headers(response):
            response.headers["request-id"] = get_current_request_id()
            response.headers["mbs-api-server"] = HOST_NAME
            return response

        # build get backup database names
        @flask_server.route('/get-backup-database-names',
                            methods=['GET'])
        @self.api_auth_service.auth("/get-backup-database-names")
        @crossdomain(origin='*')
        @self.mbs_endpoint
        def get_backup_database_names_request():
            backup_id = request.args.get('backupId')
            return self.get_backup_database_names(backup_id)

        # build delete backup method
        @flask_server.route('/expire-backup', methods=['GET'])
        @self.api_auth_service.auth("/expire-backup")
        @crossdomain(origin='*')
        @self.mbs_endpoint
        def expire_backup_request():
            backup_id = request.args.get('backupId')
            return self.expire_backup(backup_id)

        # build delete backup plan method
        @flask_server.route('/delete-backup-plan', methods=['GET'])
        @self.api_auth_service.auth("/delete-backup-plan")
        @crossdomain(origin='*')
        @self.mbs_endpoint
        def delete_backup_plan_request():
            plan_id = request.args.get('backupPlanId')
            return self.delete_backup_plan(plan_id)

        # build restore method
        @flask_server.route('/restore-backup', methods=['POST'])
        @self.api_auth_service.auth("/restore-backup")
        @crossdomain(origin='*')
        @self.mbs_endpoint
        def restore_backup_request():
            return self.restore_backup()

        # build get-destination-restore-status
        @flask_server.route('/get-destination-restore-status', methods=['GET'])
        @self.api_auth_service.auth("/get-destination-restore-status")
        @crossdomain(origin='*')
        @self.mbs_endpoint
        def get_destination_restore_status_request():
            return self.get_destination_restore_status()

    ####################################################################################################################
    def mbs_endpoint(self, f):
        def wrapped_function(*args, **kwargs):
            request_id = bind_new_request_id()
            backup_id = get_requested_backup_id()
            backup_id_str = "(backupId=%s)" % backup_id if backup_id else ""
            start_date = date_utils.date_now()
            queue_size = self._waitress_server.task_dispatcher.queue.qsize()
            logger.info("%s: NEW REQUEST (requestId=%s) %s [%s total requests queued]" % (
                request.path, request_id, backup_id_str, queue_size))

            result = f(*args, **kwargs)
            elapsed = date_utils.timedelta_total_seconds(date_utils.date_now() - start_date)

            logger.info("%s: FINISHED (requestId=%s) %s in %s seconds" % (request.path,
                                                                          request_id, backup_id_str, elapsed))

            return result

        return update_wrapper(wrapped_function, f)

########################################################################################################################
# HELPERS
########################################################################################################################
__local__ = threading.local()

def get_requested_backup_id():
    backup_id = get_request_value("backupId")
    if isinstance(backup_id, dict):
        backup_id = backup_id["$oid"]

    return backup_id

########################################################################################################################
def bind_new_request_id():
    request_id = new_request_id()
    set_current_request_id(request_id)
    return request_id

########################################################################################################################
def set_current_request_id(request_id):
    __local__.request_id = request_id

########################################################################################################################
def get_current_request_id():
    if hasattr(__local__, "request_id"):
        return __local__.request_id
