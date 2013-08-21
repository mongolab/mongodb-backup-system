__author__ = 'abdul'

import mbs_logging
import traceback

from threading import Thread

from flask import Flask
from flask.globals import request
from utils import document_pretty_string
from errors import BackupSystemApiError
from netutils import crossdomain
from functools import update_wrapper

from tornado.wsgi import WSGIContainer
from tornado.httpserver import HTTPServer
from tornado.ioloop import IOLoop

###############################################################################
# BackupSystemApiServer
###############################################################################

###############################################################################
# LOGGER
###############################################################################
logger = mbs_logging.logger

class BackupSystemApiServer(Thread):

    ###########################################################################
    def __init__(self):
        Thread.__init__(self)
        self._backup_system = None
        self._api_auth_service = None
        self._flask_server = None
        self._http_server = None
        self._protocol = None
        self._ssl_options = None

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
            self._backup_system._do_stop()
            # stop the api server
            self.stop()
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
        logger.info("Backup System: Received a status command")
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
            backup = self._backup_system.get_backup(backup_id)
            return str(backup)
        except Exception, e:
            msg = "Error while trying to get backup %s: %s" % (backup_id, e)
            logger.error(msg)
            logger.error(traceback.format_exc())
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
            return error_response(msg)

    ###########################################################################
    def delete_backup(self, backup_id):
        logger.info("Backup System: Received a delete-backup command")
        try:
            result = self._backup_system.delete_backup(backup_id)
            return document_pretty_string(result)
        except Exception, e:
            msg = ("Error while trying to delete backup %s: %s" %
                   (backup_id, e))
            logger.error(msg)
            logger.error(traceback.format_exc())
            return error_response(msg)

    ###########################################################################
    def restore_backup(self):
        arg_json = request.json
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
            return error_response(msg)

    ###########################################################################
    def stop_command_server(self):
        logger.info("Stopping command server")
        try:
            shutdown = request.environ.get('werkzeug.server.shutdown')
            if shutdown is None:
                raise RuntimeError('Not running with the Werkzeug Server')
            shutdown()
            return "success"
        except Exception, e:
            msg = "Error while trying to get backup system status: %s" % e
            logger.error(msg)
            logger.error(traceback.format_exc())
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
        @flask_server.route('/delete-backup', methods=['GET'])
        @self.api_auth_service.auth("/delete-backup")
        @crossdomain(origin='*')
        def delete_backup_request():
            backup_id = request.args.get('backupId')
            return self.delete_backup(backup_id)

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
        port = self._backup_system._api_port
        app = self.flask_server
        logger.info("BackupSystemApiServer: Starting HTTPServer"
                    " (port=%s, protocol=%s)" % (port, self.protocol))

        http_server = HTTPServer(WSGIContainer(app), protocol=self.protocol,
                                 ssl_options=self.ssl_options)
        http_server.listen(port)
        self._http_server = http_server
        IOLoop.instance().start()

    ###########################################################################
    def stop(self):

        logger.info("Stopping api server")
        try:
            IOLoop.instance().stop()
            return "success"
        except Exception, e:
            msg = "Error while trying to stop backup api server: %s" % e
            logger.error(msg)
            logger.error(traceback.format_exc())
            return error_response(msg)

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