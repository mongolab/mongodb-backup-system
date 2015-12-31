__author__ = 'abdul'

import logging
import traceback

from threading import Thread, Timer

from flask import Flask

from mbs.utils import document_pretty_string, object_type_name
from mbs.errors import MBSApiError
from mbs.netutils import crossdomain
from mbs_client.client import MBSClient


from waitress import serve
from mbs.mbs import get_mbs


from flask import jsonify
from auth_service import DefaultApiAuthService


########################################################################################################################
# LOGGER
########################################################################################################################
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

DEFAULT_NUM_WORKERS = 20

########################################################################################################################
# BackupSystemApiServer
########################################################################################################################
class ApiServer(Thread):

    ####################################################################################################################
    def __init__(self, port=9003):
        Thread.__init__(self)
        self._port = port
        self._api_auth_service = None
        self._flask_server = None
        self._http_server = None
        self._protocol = "http"
        self._ssl_options = None
        self._num_workers = DEFAULT_NUM_WORKERS
        self._waitress_server = None
        self._local_client = None

    ####################################################################################################################
    @property
    def flask_server(self):
        if not self._flask_server:
            flask_server = Flask(__name__, static_folder=None)
            self.build_flask_server(flask_server)
            self.api_auth_service.validate_server_auth(flask_server)
            self._flask_server = flask_server

        return self._flask_server

    ####################################################################################################################
    @property
    def name(self):
        return object_type_name(self)

    ####################################################################################################################
    @property
    def api_auth_service(self):
        if not self._api_auth_service:
            self._api_auth_service = DefaultApiAuthService()
        return self._api_auth_service

    ####################################################################################################################
    @property
    def port(self):
        return self._port

    @port.setter
    def port(self, val):
        self._port = val

    ####################################################################################################################
    @property
    def protocol(self):
        return self._protocol

    @protocol.setter
    def protocol(self, val):
        self._protocol = val

    ####################################################################################################################
    @property
    def ssl_options(self):
        return self._ssl_options

    @ssl_options.setter
    def ssl_options(self, val):
        self._ssl_options = val

    ####################################################################################################################
    @property
    def num_workers(self):
        return self._num_workers

    @num_workers.setter
    def num_workers(self, val):
        self._num_workers = val

    ####################################################################################################################
    @property
    def local_client(self):
        if self._local_client is None:
            self._local_client = MBSClient(api_url="http://0.0.0.0:%s" % self.port)
        return self._local_client

    ####################################################################################################################
    def status(self):
        return {
            "status": "running",
            "versionInfo": get_mbs().get_version_info()
        }

    ####################################################################################################################
    def build_flask_server(self, flask_server):

        @flask_server.errorhandler(MBSApiError)
        def handle_invalid_usage(error):
            response = jsonify(error.to_dict())
            response.status_code = error.status_code
            return response

        # build stop method
        @flask_server.route('/stop', methods=['GET'])
        @self.api_auth_service.auth("/stop")
        @crossdomain(origin='*')
        def stop_api_server_request():
            logger.info("Received a stop command")
            return document_pretty_string(self.stop_api_server())

        # build status method
        @flask_server.route('/status', methods=['GET'])
        @self.api_auth_service.auth("/status")
        @crossdomain(origin='*')
        def status_request():
            logger.info("Received a status command")
            return document_pretty_string(self.status())

    ####################################################################################################################
    def run(self):
        app = self.flask_server
        logger.info("%s: Starting HTTPServer (port=%s, protocol=%s)" % (self.name, self.port, self.protocol))

        serve(app, host='0.0.0.0', port=self.port, url_scheme=self.protocol,
              threads=self.num_workers, _server=self.custom_waitress_create_server)

    ####################################################################################################################
    def stop_api_server(self):

        Timer(2, self._do_stop).start()
        return {
            "ok": 1
        }

    ####################################################################################################################
    def _do_stop(self):
        try:
            # This is how we stop waitress unfortunately
            self._waitress_server.task_dispatcher.shutdown(timeout=5)
            import asyncore
            asyncore.socket_map.clear()

        except Exception:
            traceback.print_exc()

    ####################################################################################################################
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

