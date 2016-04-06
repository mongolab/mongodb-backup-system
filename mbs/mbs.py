__author__ = 'abdul'


import os
import version
import mbs_config as config
import logging

import threading

from collection import MBSObjectCollection, MBSTaskCollection
from makerpy.maker import resolve_class, Maker

from type_bindings import TYPE_BINDINGS
from indexes import MBS_INDEXES
from errors import MBSError

from utils import read_config_json, resolve_function, resolve_path, multiprocess_local
from mongo_utils import mongo_connect, get_client_connection_id

from backup import Backup
from restore import Restore

from plan import BackupPlan
from audit import AuditReport

from encryption import Encryptor


###############################################################################
# LOGGER
###############################################################################
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

###############################################################################
DEFAULT_BACKUP_TEMP_DIR_ROOT = "~/backup_temp"
DEFAULT_TEMPLATE_DIR_ROOT = \
    os.path.join(os.path.dirname(__file__), "notification", "templates")

###############################################################################
# MBS
###############################################################################
class MBS(object):

    ###########################################################################
    def __init__(self, config):

        # init config and database
        self._config = config
        self._mbs_db_client = None

        self._type_bindings = self._get_type_bindings()

        # make the maker
        self._maker =  Maker(type_bindings=self._type_bindings)

        #  notifications
        self._notifications = None

        # init object collections
        self._backup_collection = None
        self._plan_collection = None
        self._deleted_plan_collection = None
        self._audit_collection = None
        self._restore_collection = None

        # load backup system/engines lazily
        self._backup_system = None
        self._api_server = None

        # listens for backup events coming through rest
        self._backup_event_listener = None

        self._api_client = None


        self._engines = None

        # init the encryptor
        self._encryptor = self._get_encryptor()

        #
        self._backup_source_builder = None
        self._default_backup_assistant = None
        self._temp_dir = resolve_path(DEFAULT_BACKUP_TEMP_DIR_ROOT)

        self._event_colllection = None
        self._event_listener_collection = None
        self._event_queue = None

    ###########################################################################
    @property
    def temp_dir(self):
        return self._temp_dir

    @temp_dir.setter
    def temp_dir(self, temp_dir):
        self._temp_dir = resolve_path(temp_dir)

    ###########################################################################
    def _get_type_bindings(self):
        return TYPE_BINDINGS

    ###########################################################################
    def ensure_mbs_indexes(self):
        # Ensures indexes in the background thread
        def do_ensure_indexes():
            database = self.database
            logger.debug("Ensuring mongodb-backup-system indexes")
            for coll_name, coll_indexes in self._get_indexes().items():
                coll = database[coll_name]
                for c_index in coll_indexes:
                    logger.debug("Ensuring index %s on collection '%s'" %
                                 (c_index, coll_name))
                    kwargs = c_index.get("args") or {}
                    kwargs["background"] = True
                    coll.ensure_index(c_index["index"], **kwargs)

        threading.Thread(target=do_ensure_indexes).start()

    ###########################################################################
    def _get_indexes(self):
        return MBS_INDEXES

    ###########################################################################
    def _get_config_value(self, name):
        return self._config.get(name)

    ###########################################################################
    def _get_database_uri(self):
        return self._get_config_value("databaseURI")


    ###########################################################################
    @property
    def mbs_db_client(self):
        if not self._mbs_db_client:
            self._mbs_db_client = mongo_connect(self._get_database_uri(), w=1)
            connection_id = get_client_connection_id(self._mbs_db_client)
            logger.info("Successfully connected to mbs database (mongo connection id %s)" % connection_id)

        return self._mbs_db_client

    ###########################################################################
    @property
    def database(self):
        return self.mbs_db_client.get_default_database()

    ###########################################################################
    @property
    def backup_collection(self):
        if not self._backup_collection:
            bc = MBSTaskCollection(self.database["backups"],
                                   clazz=Backup,
                                   type_bindings=self._type_bindings)
            self._backup_collection = bc

        return self._backup_collection

    ###########################################################################
    @property
    def restore_collection(self):
        if not self._restore_collection:
            rc = MBSTaskCollection(self.database["restores"],
                                   clazz=Restore,
                                   type_bindings=self._type_bindings)
            self._restore_collection = rc

        return self._restore_collection

    ###########################################################################
    @property
    def plan_collection(self):

        if not self._plan_collection:
            pc = MBSObjectCollection(self.database["plans"], clazz=BackupPlan,
                                     type_bindings=self._type_bindings)
            self._plan_collection = pc

        return self._plan_collection

    ###########################################################################
    @property
    def event_queue(self):
        event_queue_conf = self._get_config_value("eventQueue")
        if not self._event_queue and event_queue_conf:
            self._event_queue = self._maker.make(event_queue_conf)

        return self._event_queue

    ###########################################################################
    @property
    def deleted_plan_collection(self):
        if not self._deleted_plan_collection:
            dpc = MBSObjectCollection(self.database["deleted-plans"],
                                      clazz=BackupPlan,
                                      type_bindings=self._type_bindings)
            self._deleted_plan_collection = dpc

        return self._deleted_plan_collection

    ###########################################################################
    @property
    def audit_collection(self):
        if not self._audit_collection:
            ac = MBSObjectCollection(self.database["audits"], clazz=AuditReport,
                                     type_bindings=self._type_bindings)

            self._audit_collection = ac

        return self._audit_collection

    ###########################################################################
    @property
    def engines(self):
        if not self._engines:
            self._engines = self._read_engines()
        return self._engines

    ###########################################################################
    def get_default_engine(self):
        return self.engines[0]

    ###########################################################################
    def _read_engines(self):
        engines_conf = self._get_config_value("engines")
        if not engines_conf:
            raise MBSError("No 'engines' configured")

        engines = self._maker.make(engines_conf)
        return engines


    ###########################################################################
    def get_engine(self, engine_id):
        engine = filter(lambda eng: eng.id == engine_id, self.engines)
        if engine:
            return engine[0]
        else:
            raise MBSError("No such engine '%s'" % engine_id)

    ###########################################################################
    @property
    def backup_system(self):
        backup_system_conf = self._get_config_value("backupSystem")
        if not self._backup_system and backup_system_conf:
            self._backup_system = self._maker.make(backup_system_conf)

        return self._backup_system


    ###########################################################################
    @property
    def api_server(self):

        if not self._api_server:
            api_server_conf = self._get_config_value("apiServer")
            if api_server_conf:
                self._api_server = self._maker.make(api_server_conf)

        return self._api_server

    ###########################################################################
    @property
    def api_client(self):

        if not self._api_client:
            api_client_conf = self._get_config_value("apiClient")
            if api_client_conf:
                self._api_client = self._maker.make(api_client_conf)
            else:
                import mbs_client.client
                self._api_client = mbs_client.client.backup_system_client()

        return self._api_client

    ###########################################################################
    @property
    def backup_event_listener(self):

        if not self._backup_event_listener:
            listener_server_conf = self._get_config_value("backupEventListener")
            if listener_server_conf:
                self._backup_event_listener = self._maker.make(listener_server_conf)

        return self._backup_event_listener

    ###########################################################################
    @property
    def notifications(self):
        if not self._notifications:
            notifications_conf = self._get_config_value("notifications")
            self._notifications = self.maker.make(notifications_conf)

        return self._notifications

    ###########################################################################
    @property
    def default_backup_assistant(self):
        import backup_assistant
        assistant_conf = self._get_config_value("defaultBackupAssistant")
        if not self._default_backup_assistant:
            if assistant_conf:
                self._default_backup_assistant = self._maker.make(assistant_conf)
            else:
                self._default_backup_assistant = backup_assistant.LocalBackupAssistant()

            # set temp dir for local assistants
            if isinstance(self._default_backup_assistant, backup_assistant.LocalBackupAssistant):
                self._default_backup_assistant.temp_dir = self.temp_dir

        return self._default_backup_assistant

    ###########################################################################
    def _get_encryptor(self):
        encryptor_conf = self._get_config_value("encryptor")
        if encryptor_conf:
            return self._maker.make(encryptor_conf)
        else:
            # return default encryption class
            return Encryptor()

    ###########################################################################
    @property
    def encryptor(self):
        return self._encryptor

    ###########################################################################
    @property
    def maker(self):
        return self._maker

    ###########################################################################
    @property
    def backup_source_builder(self):

        sb = self._backup_source_builder
        if not sb:
            from backup_source_builder import DefaultBackupSourceBuilder
            sb = DefaultBackupSourceBuilder()
            self._backup_source_builder = sb
        return sb

    ###########################################################################
    @property
    def dump_line_filter_function(self):
        func_name = self._get_config_value("dumpLineFilterFunction")
        if func_name:
            return resolve_function(func_name)

    ###########################################################################
    @property
    def mongoctl_config_root(self):
        return self._get_config_value("mongoctlConfigRoot")

    ###########################################################################
    def get_version_info(self):
        return {
            "mbs": version.get_mbs_version()
        }

    ###########################################################################
    def resolve_notification_template_path(self, path, *roots):
        if path.startswith(os.path.sep) and os.path.isfile(path):
            return path

        if len(roots) == 0:
            roots = [DEFAULT_TEMPLATE_DIR_ROOT]

        def search_root(root):
            for root_, dirs, files in os.walk(root):
                if path in files:
                    return os.path.join(root_, path)
            return None

        for root in roots:
            if os.path.sep not in path:
                template = search_root(root)
                if template is not None:
                    return template
            else:
                template = os.path.join(root, path)
                if os.path.isfile(template):
                    return template

        raise ValueError('Template {} not found'.format(path))


###############################################################################
# MBS Singleton
###############################################################################
def get_mbs():
    mbs_singleton = multiprocess_local().get("mbs_singleton")
    if not mbs_singleton:
        mbs_config = read_config_json("mbs", config.MBS_CONF_PATH)
        mbs_type = mbs_config.get("_type")
        mbs_class = MBS
        if mbs_type:
            mbs_class = resolve_class(mbs_type)

        mbs_singleton = mbs_class(mbs_config)
        multiprocess_local()["mbs_singleton"] = mbs_singleton
    return mbs_singleton


