import hashlib


###############################################################################
def truthy(val):
    val = str(val).lower()
    if val in ['1', 'true', 'on', 'yes']:
        return True
    if falsey(val):
        return False
    raise ValueError('{} is neither truthy nor falsey'.format(val))


###############################################################################
def falsey(val):
    val = str(val).lower()
    if val in ['0', 'false', 'off', 'no']:
        return True
    if truthy(val):
        return False
    raise ValueError('{} is neither truthy nor falsey'.format(val))


###############################################################################
def md5(path):
    md5 = hashlib.md5()
    with open(path, 'rb') as file_:
        data = file_.read(8192)
        while data:
            md5.update(data)
            data = file_.read(8192)
        return md5.hexdigest()

