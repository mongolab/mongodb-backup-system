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
    def get_message(self, context=None):
        if self.type is None:
            raise RuntimeError('type not set')
        if context is None:
            context = {}

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
# EmailNotification
###############################################################################
class EmailNotification(AbstractNotificationMessage):

    ###########################################################################
    def __init__(self, subject_template=None, body_template=None):
        self._subject_template = subject_template
        self._body_template = body_template

    ###########################################################################
    @property
    def subject_template(self):
        return self._subject_template

    @subject_template.setter
    def subject_template(self, template):
        self._subject_template = template

    ###########################################################################
    @property
    def body_template(self):
        return self._body_template

    @body_template.setter
    def body_template(self, template):
        self._body_template = template

    ###########################################################################
    def get_message(self, context=None):
        """
        return body message, however
        :param context:
        :return:
        """
        raise Exception("only get_subject_message() or get_body_message() allowed")

    ###########################################################################
    def get_subject_message(self, context=None):
        st = self._to_notfication_message_object(self.subject_template)
        return st.get_message(context=context)

    ###########################################################################
    def get_body_message(self, context=None):
        bt = self._to_notfication_message_object(self.body_template)
        return bt.get_message(context=context)

    ###########################################################################
    def _to_notfication_message_object(self, val):
        if isinstance(val, basestring):
            return TemplateNotificationMessage(template=str(val), type_="string")
        elif isinstance(val, AbstractNotificationMessage):
            return val
        else:
            raise Exception("Unexpected value type: %s" % type(val))


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


