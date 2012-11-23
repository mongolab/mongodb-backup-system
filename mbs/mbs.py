__author__ = 'abdul'

import urllib

from makerpy.object_collection import ObjectCollection
from makerpy.maker import resolve_class, Maker
from type_bindings import TYPE_BINDINGS
from errors import MBSException

from utils import read_config_json
from mongo_utils import mongo_connect

from backup import Backup
from plan import BackupPlan
from audit import AuditReport, GlobalAuditor, PlanAuditor
from engine import BackupEngine
from manager import PlanManager

###############################################################################
MBS_CONFIG = "~/.mbs/mbs.config"

###############################################################################
# MBS
###############################################################################
class MBS(object):

    ###########################################################################
    def __init__(self, config):

        # init config and database
        self._config = config
        self._database = mongo_connect(self._get_database_uri())
        type_bindings = self._get_type_bindings()

        # make the maker
        self._maker =  Maker(type_bindings=type_bindings)

        # init notification handler
        self._notification_handler = self.get_notification_handler()

        # init object collections
        bc = ObjectCollection(self._database["backups"],
                              clazz=Backup,
                              type_bindings=type_bindings)
        self._backup_collection = bc

        pc = ObjectCollection(self._database["plans"],
                              clazz=BackupPlan,
                              type_bindings=type_bindings)
        self._plan_collection = pc

        ac = ObjectCollection(self._database["audits"],
                              clazz=AuditReport,
                              type_bindings=type_bindings)

        self._audit_collection = ac
        # load manager/engines lazily
        self._plan_manager = None


        self._engines = None

    ###########################################################################
    def _get_type_bindings(self):
        return TYPE_BINDINGS

    ###########################################################################
    def _get_config_value(self, name):
        return self._config.get(name)

    ###########################################################################
    def _get_database_uri(self):
        return self._get_config_value("databaseURI")

    ###########################################################################
    @property
    def backup_collection(self):
        return self._backup_collection

    ###########################################################################
    @property
    def audit_collection(self):
        return self._audit_collection

    ###########################################################################
    @property
    def plan_collection(self):
        return self._plan_collection

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
            raise MBSException("No 'engines' configured")

        engines = self._maker.make(engines_conf)
        for engine in engines:
            engine.backup_collection = self.backup_collection
            engine.notification_handler = self._notification_handler

        return engines


    ###########################################################################
    def get_engine(self, engine_id):
        engine = filter(lambda eng: eng.id == engine_id, self.engines)
        if engine:
            return engine[0]
        else:
            raise MBSException("No such engine '%s'" % engine_id)

    ###########################################################################
    @property
    def plan_manager(self):
        manager_conf = self._get_config_value("planManager")
        if not self._plan_manager and manager_conf:
            self._plan_manager = self._maker.make(manager_conf)
            self._plan_manager.plan_collection = self.plan_collection
            self._plan_manager.backup_collection = self.backup_collection
            self._plan_manager.audit_collection = self.audit_collection
            self._plan_manager.notification_handler = \
                                                    self._notification_handler

        return self._plan_manager

    ###########################################################################
    def get_notification_handler(self):
        handler_conf = self._get_config_value("notificationHandler")
        return self._maker.make(handler_conf)

###############################################################################
# MBS Singleton
###############################################################################
mbs_singleton = None

def get_mbs():
    global mbs_singleton
    if not mbs_singleton:
        mbs_config = read_config_json("mbs", MBS_CONFIG)
        mbs_type = mbs_config.get("_type")
        mbs_class = MBS
        if mbs_type:
            mbs_class = resolve_class(mbs_type)

        mbs_singleton = mbs_class(mbs_config)
    return mbs_singleton


