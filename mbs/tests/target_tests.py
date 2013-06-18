import hashlib
import math

from tempfile import NamedTemporaryFile

from mock import patch, Mock

import mbs.target

from . import BaseTest


###############################################################################
# TargetTest
###############################################################################
class TargetTest(BaseTest):

    ###########################################################################
    def test_multi_part_put(self):
        hash_ = hashlib.md5()
        mp_upload_mock = Mock(**{'upload_part_from_file.side_effect':
                                 lambda data, i: hash_.update(data.read()),
                                 'complete_upload': Mock()})
        with NamedTemporaryFile() as dump, \
             open('/dev/urandom', 'rb') as random_data, \
             patch.object(mbs.target, 'MAX_SPLIT_SIZE', 1024), \
             patch.object(mbs.target.S3BucketTarget,
                          '_get_bucket',
                          Mock(return_value=Mock(
                                **{'initiate_multipart_upload.return_value':
                                   mp_upload_mock}))):
            dump.write(random_data.read(10000))
            target = self.maker.make({'_type': 'S3BucketTarget'})
            target._multi_part_put(dump.name, 'com.foo.bar', 10000)

            self.assertEqual(mp_upload_mock.upload_part_from_file.call_count,
                             math.ceil(10000/1024))
            self.assertTrue(mp_upload_mock.complete_upload.called)
            self.assertEqual(hash_.hexdigest(), self.md5(dump.name))
