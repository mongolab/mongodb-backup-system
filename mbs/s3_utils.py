"""
NOTE: on a failed request for a bucket in another region, you can get the correct
      region using the 'x-amz-region' header returned in the error response
"""

__author__ = 'greg'

import os

from boto.exception import S3ResponseError
from boto.s3.connection import S3Connection
from boto.regioninfo import load_regions


_S3_ENDPOINT_LOOKUP = load_regions()['s3']


###############################################################################
class _BotoConnectionRequestHook(object):
    def __init__(self, existing_hook=None):
        self.request = None
        self.response = None
        self.error = False

        self.existing_hook = existing_hook

    def handle_request_data(self, request, response, error=None):
        self.request = request
        self.response = response
        self.error = error

        if self.existing_hook is not None:
            return self.existing_hook(request, response, error)
        return None

    @property
    def region(self):
        if self.response is None:
            raise RuntimeError('region can only be accessed after '
                               '%s.handl_request_data is called' %
                               (self.__class__.__name__))
        return dict(self.response.getheaders())['x-amz-bucket-region']


###############################################################################
def get_connection_for_bucket(api_key_id, api_secret_key, bucket_name,
                              **kwargs):
    """Get a connection to the endpoint associated with a bucket's region

    :param str api_key_id: the account api key id
    :param str api_secret_key: the account secret key
    :param str bucket_name: the bucket name we want a connection for
    :param dict **kwargs: any options to forward on to the boto connection
    :return: a connection, the bucket, and region identifier or None on error
    :rtype: S3Connection, str or None
    :raises: S3ResponseError
    """
    con = S3Connection(api_key_id, api_secret_key, **kwargs)
    con.set_request_hook(_BotoConnectionRequestHook(con.request_hook))

    bucket = None
    try:
        bucket = con.get_bucket(bucket_name)
    except S3ResponseError:
        pass

    if (bucket is not None):
        # NOTE: if bucket retrieval is successful, but the region we return is
        # *not* us-east-1, then we are using signature version 2. we might want
        # to upgrade here rather than just marching forward.
        region = con.request_hook.region
        con.request_hook = con.request_hook.existing_hook
        return con, bucket, region

    region = con.request_hook.region
    con = get_connection(api_key_id, api_secret_key, region)

    return con, con.get_bucket(bucket_name), region


###############################################################################
def get_connection(api_key_id, api_secret_key, region=None, **kwargs):
    """Get a connection with the option to select endpoint by region name

    NOTE: you can specify the endpoint via the "host" option or you can use the
          region kwarg and the appropriate endpoint will be picked

    NOTE: the region (or correct host endpoint) must be specified if signature
          version 4 is desired as the signing key is scoped to the region.

    :param str api_key_id: the account api key id
    :param str api_secret_key: the account secret key
    :param str region: the region name (not the constraint)
    :param dict **kwargs: any options to forward on to the boto connection
    :return: a connection
    :rtype: S3Connection
    :raises: ValueError
    :raises: S3ResponseError
    """
    endpoint = None
    try:
        kwargs.update({'host': _S3_ENDPOINT_LOOKUP[region]})
    except KeyError:
        raise ValueError('unkonwn region: %s' % (region))
    return S3Connection(api_key_id, api_secret_key, **kwargs)


