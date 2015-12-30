__author__ = 'abdul'

from functools import update_wrapper

from api_utils import raise_forbidden_error
########################################################################################################################
# Api Auth Service
########################################################################################################################


class ApiAuthService(object):

    ####################################################################################################################
    def __init__(self):
        self._registered_paths = {}

    ####################################################################################################################
    def register_path(self, path):
        self._registered_paths[path] = True

    ####################################################################################################################
    def is_path_registered(self, path):
        return path in self._registered_paths

    ####################################################################################################################
    def auth(self, path):
        self.register_path(path)

        def decorator(f):
            def wrapped_function(*args, **kwargs):
                if not self.is_authenticated_request(path):
                    raise_forbidden_error("Need to authenticate")
                if not self.is_authorized_request(path):
                    raise_forbidden_error("Not authorized")
                return f(*args, **kwargs)
            return update_wrapper(wrapped_function, f)

        return decorator

    ####################################################################################################################
    def validate_server_auth(self, flask_server):
        for rule in flask_server.url_map.iter_rules():
            path = rule.rule
            if not self.is_path_registered(path):
                raise Exception("Un-registered path '%s' with auth service" %
                                path)

    ####################################################################################################################
    def is_authenticated_request(self, path):
        """
        :param path:
        :return:
        """
        return True

    ####################################################################################################################
    def is_authorized_request(self, path):
        """

        :param path:
        :return: True if request is authorized to execute on the specified path
                / request
        """
        return True

########################################################################################################################
class DefaultApiAuthService(ApiAuthService):
    pass