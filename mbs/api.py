__author__ = 'abdul'

import mbs_logging
import traceback

from threading import Thread

from flask import Flask
from flask.globals import request
from utils import document_pretty_string, parse_json
from errors import BackupSystemApiError
from netutils import crossdomain
from functools import update_wrapper

from waitress import serve
from mbs import get_mbs

import persistence

import os
import signal

###############################################################################
# BackupSystemApiServer
###############################################################################

###############################################################################
# LOGGER
###############################################################################
logger = mbs_logging.logger

class BackupSystemApiServer(Thread):

    ###########################################################################
    def __init__(self, port=9003):
        Thread.__init__(self)
        self._backup_system = None
        self._port = port
        self._api_auth_service = None
        self._flask_server = None
        self._http_server = None
        self._protocol = "http"
        self._ssl_options = None
        self._waitress_server = None

    ###########################################################################
    @property
    def flask_server(self):
        if not self._flask_server:
            flask_server = Flask(__name__, static_folder=None)
            self._build_flask_server(flask_server)
            self.api_auth_service.validate_server_auth(flask_server)
            self._flask_server = flask_server

        return self._flask_server

    ###########################################################################
    @property
    def api_auth_service(self):
        if not self._api_auth_service:
            self._api_auth_service = DefaultApiAuthService()
        return self._api_auth_service


    ###########################################################################
    @property
    def port(self):
        return self._port

    @port.setter
    def port(self, val):
        self._port = val

    ###########################################################################
    @property
    def protocol(self):
        return self._protocol

    @protocol.setter
    def protocol(self, val):
        self._protocol = val

    ###########################################################################
    @property
    def ssl_options(self):
        return self._ssl_options

    @ssl_options.setter
    def ssl_options(self, val):
        self._ssl_options = val
        
    ###########################################################################
    def stop_backup_system(self):
        logger.info("Backup System: Received a stop command")
        try:
            # stop the backup system
            self._backup_system.request_stop()
            return document_pretty_string({
                "ok": True
            })
        except Exception, e:
            msg = "Error while trying to stop backup system: %s" % e
            logger.error(msg)
            logger.error(traceback.format_exc())
            return document_pretty_string({"error": "can't stop"})

    ###########################################################################
    def status(self):
        logger.debug("Backup System: Received a status command")
        try:
            return document_pretty_string(self._backup_system._do_get_status())
        except Exception, e:
            msg = "Error while trying to get backup system status: %s" % e
            logger.error(msg)
            logger.error(traceback.format_exc())
            return document_pretty_string({"status": "error"})

    ###########################################################################
    def get_backup(self, backup_id):
        logger.info("Backup System: Received a get-backup command")
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
        logger.info("Backup System: Received a get-backup-database-names"
                    " command")
        try:
            dbnames = self._backup_system.get_backup_database_names(backup_id)
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
        logger.info("Backup System: Received a expire-backup command")
        try:
            exp_man = self._backup_system.backup_expiration_manager
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
        logger.info("Backup System: Received a delete-backup-plan command")
        try:
            result = self._backup_system.remove_plan(plan_id)
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
        tags = arg_json.get('tags')
        source_database_name = arg_json.get('sourceDatabaseName')
        logger.info("Backup System: Received a restore-backup command")
        try:
            bs = self._backup_system
            r = bs.schedule_backup_restore(backup_id,
                                           destination_uri,
                                           source_database_name=
                                           source_database_name,
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
        logger.info("Backup System: Received a "
                    "get-destination-restore-status command")
        try:
            status = self._backup_system.get_destination_restore_status(
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
    def _build_flask_server(self, flask_server):

        ########## build stop method
        @flask_server.route('/stop', methods=['GET'])
        @self.api_auth_service.auth("/stop")
        @crossdomain(origin='*')
        def stop_backup_system_request():
            return self.stop_backup_system()

        ########## build status method
        @flask_server.route('/status', methods=['GET'])
        @self.api_auth_service.auth("/status")
        @crossdomain(origin='*')
        def status_request():
            return self.status()

        ########## build get backup database names
        @flask_server.route('/get-backup-database-names',
                            methods=['GET'])
        @self.api_auth_service.auth("/get-backup-database-names")
        @crossdomain(origin='*')
        def get_backup_database_names_request():
            backup_id = request.args.get('backupId')
            return self.get_backup_database_names(backup_id)

        ########## build delete backup method
        @flask_server.route('/expire-backup', methods=['GET'])
        @self.api_auth_service.auth("/expire-backup")
        @crossdomain(origin='*')
        def expire_backup_request():
            backup_id = request.args.get('backupId')
            return self.expire_backup(backup_id)

        ########## build delete backup plan method
        @flask_server.route('/delete-backup-plan', methods=['GET'])
        @self.api_auth_service.auth("/delete-backup-plan")
        @crossdomain(origin='*')
        def delete_backup_plan_request():
            plan_id = request.args.get('backupPlanId')
            return self.delete_backup_plan(plan_id)

        ########## build restore method
        @flask_server.route('/restore-backup', methods=['POST'])
        @self.api_auth_service.auth("/restore-backup")
        @crossdomain(origin='*')
        def restore_backup_request():
            return self.restore_backup()

        ########## build get-destination-restore-status
        @flask_server.route('/get-destination-restore-status', methods=['GET'])
        @self.api_auth_service.auth("/get-destination-restore-status")
        @crossdomain(origin='*')
        def get_destination_restore_status_request():
            return self.get_destination_restore_status()


    ###########################################################################
    def run(self):
        app = self.flask_server
        logger.info("BackupSystemApiServer: Starting HTTPServer"
                    " (port=%s, protocol=%s)" % (self.port, self.protocol))

        serve(app, host='0.0.0.0', port=self.port, url_scheme=self.protocol,
              threads=10, _server=self.custom_waitress_create_server)

    ###########################################################################
    def stop_command_server(self):
        # This is how we stop waitress unfortunately
        try:
            self._waitress_server.task_dispatcher.shutdown(timeout=5)
            import asyncore
            asyncore.socket_map.clear()
        except Exception:
            traceback.print_exc()
    ###########################################################################
    # TODO Remove this once we have a better shutdown method
    def custom_waitress_create_server(
            self,
            application,
            map=None,
            _start=True,      # test shim
            _sock=None,       # test shim
            _dispatcher=None, # test shim
            **kw):
        import waitress.server
        self._waitress_server = waitress.server.create_server(
            application, map=map, _start=_start, _sock=_sock,
            _dispatcher=_dispatcher, **kw)

        return self._waitress_server

###############################################################################
# Api Auth Service
###############################################################################


class ApiAuthService(object):

    ###########################################################################
    def __init__(self):
        self._registered_paths = {}

    ###########################################################################
    def register_path(self, path):
        self._registered_paths[path] = True

    ###########################################################################
    def is_path_registered(self, path):
        return path in self._registered_paths

    ###########################################################################
    def auth(self, path):
        self.register_path(path)

        def decorator(f):
            def wrapped_function(*args, **kwargs):
                if not self.is_authenticated_request(path):
                    raise BackupSystemApiError("Need to authenticate")
                if not self.is_authorized_request(path):
                    raise BackupSystemApiError("Not authorized")
                return f(*args, **kwargs)
            return update_wrapper(wrapped_function, f)

        return decorator

    ###########################################################################
    def validate_server_auth(self, flask_server):
        for rule in flask_server.url_map.iter_rules():
            path = rule.rule
            if not self.is_path_registered(path):
                raise BackupSystemApiError("Un-registered path '%s' with "
                                           "auth service" % path)

    ###########################################################################
    def is_authenticated_request(self, path):
        """
        :param path:
        :return:
        """
        return True

    ###########################################################################
    def is_authorized_request(self, path):
        """

        :param path:
        :return: True if request is authorized to execute on the specified path
                / request
        """
        return True

###############################################################################
class DefaultApiAuthService(ApiAuthService):
    pass

###############################################################################
# HELPERS
###############################################################################
def error_response(message):
    return document_pretty_string({
        "error": message
    })

###############################################################################
def ok_response(ok=True):
    return document_pretty_string({
        "ok": ok
    })

###############################################################################
def send_api_error(end_point, exception):
    subject = "BackupSystemAPI Error"
    message = ("BackupSystemAPI Error on '%s'.\n\nStack Trace:\n%s" %
               (end_point, traceback.format_exc()))

    get_mbs().send_error_notification(subject, message, exception)

###############################################################################
def get_request_json():
    if request.data:
        return parse_json(request.data)