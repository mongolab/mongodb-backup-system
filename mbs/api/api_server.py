__author__ = 'abdul'

import logging
import traceback

from flask import Flask

from mbs.utils import document_pretty_string, object_type_name, get_local_host_name
from mbs.errors import MBSApiError
from mbs.netutils import crossdomain
from mbs_client.client import MBSClient
from mbs.notification import NotificationPriority

from mbs.mbs import get_mbs


from flask import jsonify
from auth_service import DefaultApiAuthService

import gunicorn.app.base
from gunicorn.six import iteritems

########################################################################################################################
# LOGGER
########################################################################################################################
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

DEFAULT_NUM_WORKERS = 4

########################################################################################################################
# BackupSystemApiServer
########################################################################################################################
class ApiServer(object):

    ####################################################################################################################
    def __init__(self, port=9003):

        self._port = port
        self._api_auth_service = None
        self._flask_server = None
        self._http_server = None
        self._protocol = "http"
        self._ssl_options = None
        self._num_workers = DEFAULT_NUM_WORKERS
        self._local_client = None
        self._debug_mode = False

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
    def debug_mode(self):
        return self._debug_mode

    @debug_mode.setter
    def debug_mode(self, val):
        self._debug_mode = val

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

        # build status method
        @flask_server.route('/status', methods=['GET'])
        @self.api_auth_service.auth("/status")
        @crossdomain(origin='*')
        def status_request():
            logger.info("Received a status command")
            return document_pretty_string(self.status())

    ####################################################################################################################
    def start(self):
        try:
            app = self.flask_server
            logger.info("%s: Starting HTTPServer (port=%s, protocol=%s, workers=%s)" %
                        (self.name, self.port, self.protocol, self.num_workers))

            options = {
                "bind": "0.0.0.0:%s" % self.port,
                "workers": self.num_workers,
                "worker_class": "gevent" if not self.debug_mode else "sync",
                "proxy_protocol": self.protocol == "https"
            }
            MbsApiGunicornApplication(app, options).run()

        except Exception, ex:
            logger.exception("Api Server crashed")
            sbj = "Api Server %s on %s crashed" % (self.name, get_local_host_name())

            get_mbs().notifications.send_event_notification(sbj, sbj, priority=NotificationPriority.CRITICAL)


########################################################################################################################
# Custom MbsApiGunicornApplication
########################################################################################################################


class MbsApiGunicornApplication(gunicorn.app.base.BaseApplication):
    def __init__(self, app, options=None):
        self.options = options or {}
        self.application = app
        super(MbsApiGunicornApplication, self).__init__()

    def load_config(self):
        config = dict([(key, value) for key, value in iteritems(self.options)
                       if key in self.cfg.settings and value is not None])
        for key, value in iteritems(config):
            self.cfg.set(key.lower(), value)

    def load(self):
        return self.application

