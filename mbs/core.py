__author__ = 'abdul'

from makerpy.object_collection import ObjectCollection
from makerpy.maker import resolve_class, Maker
from type_bindings import TYPE_BINDINGS

from utils import read_config_json, mongo_connect
from backup import Backup
from plan import BackupPlan
from audit import AuditReport, GlobalAuditor, PlanAuditor
from engine import BackupEngine
from manager import PlanManager

###############################################################################
MBS_CONFIG = "~/.mbs/mbs.config"

###############################################################################
# MBSCore
###############################################################################
class MBSCore(object):

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


        # init plan manager
        self._plan_manager = PlanManager(self.plan_collection,
                                         self.backup_collection,
                                          notification_handler=
                                           self._notification_handler)
        self._audit_collection = ac

        # init global editor
        self._global_auditor = GlobalAuditor(self._audit_collection,
                                             notification_handler=
                                              self._notification_handler)
        plan_auditor = PlanAuditor(self.plan_collection,
                                   self.backup_collection)

        self._global_auditor.register_auditor(plan_auditor)

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
    def plan_collection(self):
        return self._plan_collection

    ###########################################################################
    def create_backup_engine(self, engine_id, **kwargs):
        return BackupEngine(engine_id, self.backup_collection,
                            notification_handler=self._notification_handler,
                            **kwargs)

    ###########################################################################
    @property
    def plan_manager(self):
        return self._plan_manager

    ###########################################################################
    @property
    def global_auditor(self):
        return self._global_auditor

    ###########################################################################
    def get_notification_handler(self):
        handler_conf = self._get_config_value("notificationHandler")
        if handler_conf:
            return self._maker.make(handler_conf)

###############################################################################
# MBS Core Singleton
###############################################################################
mbs_core = None

def get_mbs_core():
    global mbs_core
    if not mbs_core:
        mbs_config = read_config_json("mbs", MBS_CONFIG)
        core_type = mbs_config.get("coreType")
        core_class = MBSCore
        if core_type:
            core_class = resolve_class(core_type)

        mbs_core = core_class(mbs_config)
    return mbs_core


