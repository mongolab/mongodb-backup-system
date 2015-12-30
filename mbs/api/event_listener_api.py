__author__ = 'abdul'

import logging

from base import ApiServer
from mbs.netutils import crossdomain


###############################################################################
# LOGGER
###############################################################################
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

DEFAULT_NUM_WORKERS = 20

###############################################################################
# BackupSystemApiServer
###############################################################################
class BackupEventListenerApiServer(ApiServer):

    ###########################################################################
    def handle_backup_event(self):
        pass

    ###########################################################################
    def build_flask_server(self, flask_server):

        # call super
        super(BackupEventListenerApiServer, self).build_flask_server(flask_server)

        # build restore method
        @flask_server.route('/handle-backup-event', methods=['POST'])
        @self.api_auth_service.auth("/handle-backup-event")
        @crossdomain(origin='*')
        def handle_backup_event_request():
            return self.handle_backup_event()

