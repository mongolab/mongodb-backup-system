__author__ = 'abdul'

from base import MBSObject
from utils import safe_format
# Contains classes for backup naming schemes

###############################################################################
# BackupNamingScheme
###############################################################################
class BackupNamingScheme(MBSObject):

    ###########################################################################
    def generate_name(self, backup, **kwargs):
        pass

###############################################################################
# DefaultBackupNamingScheme
###############################################################################
class DefaultBackupNamingScheme(BackupNamingScheme):

    ###########################################################################
    def __init__(self):
        BackupNamingScheme.__init__(self)

    ###########################################################################
    def generate_name(self, backup, **kwargs):
        return "%s" % backup.id

    ###########################################################################
    def to_document(self, display_only=False):
        return {
            "_type": "DefaultBackupNamingScheme"
        }

###############################################################################
# TemplateBackupNamingScheme
###############################################################################
class TemplateBackupNamingScheme(BackupNamingScheme):

    ###########################################################################
    def __init__(self, template=None):
        BackupNamingScheme.__init__(self)
        self._template = template

    ###########################################################################
    def generate_name(self, backup, **kwargs):
        return safe_format(self.template, backup=backup, **kwargs)

    ###########################################################################
    @property
    def template(self):
        return self._template

    @template.setter
    def template(self, template):
        self._template = template

    ###########################################################################
    def to_document(self, display_only=False):
        return {
            "_type": "TemplateBackupNamingScheme",
            "template": self.template
        }