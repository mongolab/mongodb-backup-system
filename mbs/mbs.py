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

from utils import read_config_json, resolve_function
from mongo_utils import mongo_connect

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
# MBS
###############################################################################
class MBS(object):

    ###########################################################################
    def __init__(self, config):

        # init config and database
        self._config = config
        self._database = None

        self._type_bindings = self._get_type_bindings()

        # make the maker
        self._maker =  Maker(type_bindings=self._type_bindings)

        # init notification handler
        self._notification_handler = self.get_notification_handler()

        # init object collections
        self._backup_collection = None
        self._plan_collection = None
        self._deleted_plan_collection = None
        self._audit_collection = None
        self._restore_collection = None

        # load backup system/engines lazily
        self._backup_system = None
        self._api_server = None

        self._api_client = None


        self._engines = None

        # init the encryptor
        self._encryptor = self._get_encryptor()

        #
        self._backup_source_builder = None

        self._default_backup_assistant = None

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
    def database(self):
        if not self._database:
            # use w=1
            self._database = mongo_connect(self._get_database_uri(), w=1)

        return self._database

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
        for engine in engines:
            engine.notification_handler = self._notification_handler

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
    def notification_handler(self):
        return self._notification_handler

    ###########################################################################
    def get_notification_handler(self):
        handler_conf = self._get_config_value("notificationHandler")
        return self._maker.make(handler_conf)

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

        return self._default_backup_assistant

    ###########################################################################
    def send_notification(self, subject, message):
        nh = self.notification_handler
        if nh:
            nh.send_notification(subject, message)

    ###########################################################################
    def send_error_notification(self, subject, message, exception):
        nh = self.notification_handler
        if nh:
            nh.send_error_notification(subject, message, exception)

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

###############################################################################
# MBS Singleton
###############################################################################
mbs_singleton = None

def get_mbs():
    global mbs_singleton
    if not mbs_singleton:
        mbs_config = read_config_json("mbs", config.MBS_CONF_PATH)
        mbs_type = mbs_config.get("_type")
        mbs_class = MBS
        if mbs_type:
            mbs_class = resolve_class(mbs_type)

        mbs_singleton = mbs_class(mbs_config)
    return mbs_singleton


