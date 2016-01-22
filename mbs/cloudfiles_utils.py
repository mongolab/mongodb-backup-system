__author__ = 'abdul'

from netutils import fetch_url
from utils import random_string, safe_stringify
from date_utils import date_to_seconds, date_plus_seconds, date_now
from hashlib import sha1

import hmac

###############################################################################
def get_download_url(container, file_path):

    temp_key = random_temp_shared_key()
    storage_url = "https://%s/%s" % (container.conn.connection_args[0], 
                                     container.conn.connection_args[2])
    set_account_temp_url_key(storage_url, container.conn.token, temp_key)

    expire_date = date_plus_seconds(date_now(), 300)

    return get_temp_url(container.name, "GET", storage_url, temp_key,
                        file_path, expire_date)


###############################################################################
def set_account_temp_url_key(storage_url,  auth_token, temp_key):


    headers = {
        "X-Auth-Token": auth_token,
        "X-Account-Meta-Temp-Url-Key": temp_key

    }

    try:
        return fetch_url(storage_url, headers=headers, method="POST")
    except Exception, e:
        if "204" in safe_stringify(e):
            pass
        else:
            raise


###############################################################################
def get_temp_url(container_name, method, storage_url,
                 temp_key, file_path, expire_date):

    exp_seconds = date_to_seconds(expire_date)
    split = storage_url.split("/v1/")
    base_url = split[0]
    file_url_path = "/v1/" + split[1]+ "/" + container_name + "/" + file_path



    hmac_body = method + "\n" + str(exp_seconds) + "\n" + file_url_path
    h = hmac.new(temp_key, msg=hmac_body, digestmod=sha1)
    tempUrlSig = h.hexdigest()
    return base_url + file_url_path + "?temp_url_sig=" + tempUrlSig + \
           "&temp_url_expires=" + str(exp_seconds)


###############################################################################
def random_temp_shared_key():
    return random_string(8)

