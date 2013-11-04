__author__ = 'abdul'

from target import BackupTarget
from threading import Thread

import mbs_logging

###############################################################################
# LOGGER
###############################################################################
logger = mbs_logging.logger

###############################################################################
# MirroredTarget
###############################################################################
class MirroredTarget(BackupTarget):
    """
     A composite target of a primary and mirrors. Uploads files to the primary
     and all mirrors but only deletes/fetches from primary.
    """

    ###########################################################################
    def __init__(self):
        BackupTarget.__init__(self)
        self._primary_target = None
        self._mirror_targets = None

    ###########################################################################
    @property
    def primary_target(self):
        return self._primary_target

    @primary_target.setter
    def primary_target(self, val):
        self._primary_target = val

    ###########################################################################
    @property
    def mirror_targets(self):
        return self._mirror_targets

    @mirror_targets.setter
    def mirror_targets(self, val):
        self._mirror_targets = val

    ###########################################################################
    @property
    def container_name(self):
        return self.primary_target.container_name

    ###########################################################################
    def put_file(self, file_path, destination_path=None,
                 overwrite_existing=False):

        logger.info("MirroredTarget: Starting mirrored target upload for file "
                    "'%s'" % file_path)
        mirror_uploaders = []
        # first kick off the mirror uploads
        for mirror_target in self.mirror_targets:
            uploader = MirrorUploader(mirror_target,
                                      file_path,
                                      destination_path=destination_path,
                                      overwrite_existing=overwrite_existing)
            mirror_uploaders.append(uploader)
            logger.info("MirroredTarget: Starting uploader for mirror: %s" %
                        mirror_target)
            uploader.start()

        logger.info("MirroredTarget: uploading to the primary target: %s" %
                    self.primary_target)

        result = self.primary_target.put_file(file_path,
                                              destination_path=
                                              destination_path,
                                              overwrite_existing=
                                              overwrite_existing)

        logger.info("MirroredTarget: Upload '%s' to primary target %s finished"
                    " successfully" % (file_path, self.primary_target))

        logger.info("MirroredTarget: Waiting for all mirror uploaders to "
                    "finish")
        # wait for all mirror uploaders to finish
        for uploader in mirror_uploaders:
            logger.info("MirroredTarget: Waiting for mirror uploader for to "
                        "finish: %s" % uploader.mirror_target)
            uploader.join()
            if uploader.error:
                logger.info("MirroredTarget: Mirror uploader %s for %s to "
                            "finished with an error. Failing and raising" %
                            (uploader.mirror_target, file_path))
                raise uploader.error
            else:
                logger.info("MirroredTarget: Mirror uploader %s for %s to "
                            "finished successfully! Target ref: %s" %
                            (file_path, uploader.mirror_target,
                             uploader.target_reference))

        logger.info("MirroredTarget: SUCCESSFULLY uploaded '%s'!" % file_path)

        return result

    ###########################################################################
    def _fetch_file_info(self, destination_path):
        """
            Override by s3 specifics

        """
        return self.primary_target._fetch_file_info(destination_path)

    ###########################################################################
    def get_file(self, file_reference, destination):
        return self.primary_target.get_file(file_reference, destination)

    ###########################################################################
    def do_delete_file(self, file_reference):
        self.primary_target.do_delete_file(file_reference)

    ###########################################################################
    def to_document(self, display_only=False):
        return {
            "_type": "MirroredTarget",
            "primaryTarget": self.primary_target.to_document(
                display_only=display_only),
            "mirrorTargets": map(lambda target: target.to_document(
                display_only=display_only), self.mirror_targets)
        }

    ###########################################################################
    def validate(self):
        errors = []

        if not self.primary_target:
            errors.append("Missing 'primaryTarget' property")

        if not self.mirror_targets:
            errors.append("mirrorTargets cannot be empty")

        return errors

###############################################################################
# MirrorUploader class
###############################################################################


class MirrorUploader(Thread):
###############################################################################
    def __init__(self, mirror_target, file_path, **upload_kargs):
        Thread.__init__(self)
        self._mirror_target = mirror_target
        self._upload_kargs = upload_kargs
        self._target_reference = None
        self._file_path = file_path
        self._error = None

    ###########################################################################
    def run(self):
        try:
            tr = self._mirror_target.put_file(self._file_path,
                                              **self._upload_kargs)
            self._target_reference = tr
        except Exception, ex:
            self._error = ex

    ###########################################################################
    @property
    def mirror_target(self):
        return self._mirror_target

    ###########################################################################
    @property
    def target_reference(self):
        return self._target_reference

    ###########################################################################
    @property
    def error(self):
        return self._error

    ###########################################################################
    def completed(self):
        return self.target_reference is not None or self.error is not None



