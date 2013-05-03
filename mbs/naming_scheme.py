__author__ = 'abdul'

from base import MBSObject

# Contains classes for backup naming schemes

###############################################################################
# BackupNamingScheme
###############################################################################
class BackupNamingScheme(MBSObject):

    ###########################################################################
    def __init__(self):
        pass

    ###########################################################################
    def generate_name(self, backup):
        pass

###############################################################################
# DefaultBackupNamingScheme
###############################################################################
class DefaultBackupNamingScheme(BackupNamingScheme):

    ###########################################################################
    def __init__(self):
        BackupNamingScheme.__init__(self)

    ###########################################################################
    def generate_name(self, backup):
        return "%s" % backup.id

###############################################################################
# TemplateBackupNamingScheme
###############################################################################
class TemplateBackupNamingScheme(BackupNamingScheme):

    ###########################################################################
    def __init__(self, template=None):
        BackupNamingScheme.__init__(self)
        self._template = template

    ###########################################################################
    def generate_name(self, backup):
        return self.template.format(backup=backup)

    ###########################################################################
    @property
    def template(self):
        return self._template

    @template.setter
    def template(self, template):
        self._template = template

    ###########################################################################
