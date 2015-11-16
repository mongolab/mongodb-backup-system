import abc
import json
import logging
import os

from .template import NotificationTemplate
from ..mbs import get_mbs


# XXX: ideally, we would move all message components here (e.g., subject, from,
#      to, ...) and just pass messages to the notification handlers


###############################################################################
# LOGGER
###############################################################################
logger = logging.getLogger(__name__)


###############################################################################
# AbstractNotificationMessage
###############################################################################
class AbstractNotificationMessage(object):
    __metaclass__ = abc.ABCMeta

    ###########################################################################
    @abc.abstractmethod
    def get_message(self, context=None):
        pass


###############################################################################
# NotificationMessage
###############################################################################
class NotificationMessage(AbstractNotificationMessage):

    ###########################################################################
    def __init__(self, message=None):
        self._message = message

    ###########################################################################
    @property
    def message(self):
        return self._message

    @message.setter
    def message(self, message):
        self._message = message

    ###########################################################################
    def get_message(self, context=None):
        return self._message


###############################################################################
# TemplateNotificationMessage
###############################################################################
class TemplateNotificationMessage(AbstractNotificationMessage):
    TYPES = {
        'FILE': 'file',
        'STRING': 'string',
        'MUSTACHE': 'mustache',
    }

    ###########################################################################
    def __init__(self, template=None, type_=None):
        self._template = template
        self._type = type_

    ###########################################################################
    @property
    def template(self):
        return self._template

    @template.setter
    def template(self, template):
        self._template = template

    ###########################################################################
    @property
    def type(self):
        return self._type

    @type.setter
    def type(self, type):
        self._type = type

    ###########################################################################
    def get_message(self, context):
        if self.type is None:
            raise RuntimeError('type not set')

        message = None

        if self._type == self.__class__.TYPES['FILE']:
            message = NotificationTemplate.render_path(self._template, context)
        elif self._type == self.__class__.TYPES['STRING']:
            message = NotificationTemplate.render_string(self._template, context)
        else:
            message = \
                NotificationTemplate.render_string(
                    self._template, context, self._type)

        return message


###############################################################################
# get_messages
###############################################################################
_MESSAGES = None
_DEFAULT_MESSAGES_PATH = os.path.join(os.path.dirname(__file__), 'messages.json')

def _load_messages(path=None):
    global _MESSAGES

    if path is None:
        path = _DEFAULT_MESSAGES_PATH

    _mbs = get_mbs()

    _MESSAGES = {
        k: _mbs.maker.make(v) 
        for k, v in json.loads(open(path).read()).iteritems()
    }

def get_messages():
    if _MESSAGES is None:
        _load_messages()
    return _MESSAGES


