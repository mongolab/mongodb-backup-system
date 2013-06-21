__author__ = 'abdul'


from source import MongoSource

###############################################################################
# BackupSourceBuilder
###############################################################################


class BackupSourceBuilder(object):
    ###########################################################################
    def __init__(self):
        pass

    ###########################################################################
    def build_backup_source(self, uri):
        """
        :param uri: a backup source uri (could be a mongodb uri or a
            custom uri)
        :return: BackupSource object
        """

###############################################################################
# DefaultBackupSourceBuilder
###############################################################################


class DefaultBackupSourceBuilder(BackupSourceBuilder):
    ###########################################################################
    def __init__(self):
        pass

    ###########################################################################
    def build_backup_source(self, uri):
        return MongoSource(uri=uri)
