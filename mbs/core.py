__author__ = 'abdul'

from makerpy.object_collection import ObjectCollection
from makerpy.maker import resolve_class

from utils import read_config_json, mongo_connect
from backup import Backup
from plan import BackupPlan
from audit import AuditEntry

#package_coll = ObjectCollection(database['packages'],
 #   clazz=Package,
  #  type_bindings=type_bindings)

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

        # init object collections
        type_bindings = self._get_type_bindings()
        bc = ObjectCollection(self._database["backups"],
                              clazz=Backup,
                              type_bindings=type_bindings)
        self._backup_collection = bc

        pc = ObjectCollection(self._database["plans"],
                              clazz=BackupPlan,
                              type_bindings=type_bindings)
        self._plan_collection = pc

        ac = ObjectCollection(self._database["audits"],
                              clazz=AuditEntry,
                              type_bindings=type_bindings)

        self._audit_collection = ac


    ###########################################################################
    def _get_type_bindings(self):
        return self._get_config_value("typeBindings")

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


