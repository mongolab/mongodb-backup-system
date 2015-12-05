import os
import logging
import string
import threading

import pystache

from ..mbs import get_mbs


###############################################################################
# LOGGER
###############################################################################
logger = logging.getLogger(__name__)


###############################################################################
# NotificationTemplate
###############################################################################
class NotificationTemplate(object):
    _lock = threading.RLock()
    _parsed_templates = {}
    _renderers = {}

    ###########################################################################
    @classmethod
    def register(cls, extension, _cls):
        cls._renderers[extension] = _cls

    ###########################################################################
    @classmethod
    def _parse_template(cls, template):
        return string.Template(template)

    ###########################################################################
    @classmethod
    def _get_template(cls, template):
        parsed_template = None
        with cls._lock:
            try:
                parsed_template = cls._parsed_templates[template]
            except KeyError:
                parsed_template = \
                    cls._parsed_templates[template] = \
                    cls._parse_template(template)
        return parsed_template

    ###########################################################################
    @classmethod
    def render_path(cls, template_path, context):
        renderer = os.path.basename(template_path).rsplit('.', 1)[-1].lower()
        try:
            return cls._renderers[renderer].render_string(
                open(get_mbs().resolve_notification_template_path(
                    template_path)).read(), context, renderer)
        except KeyError:
            raise ValueError('no renderer registered for %s', renderer)

    ###########################################################################
    @classmethod
    def render_string(cls, template_string, context, renderer=None):
        if renderer is None:
            template = cls._get_template(template_string)
            return template.substitute(context)
        try:
            return cls._renderers[renderer].render_string(
                template_string, context)
        except KeyError:
            raise ValueError('no renderer registered for %s', renderer)

NotificationTemplate.register('string', NotificationTemplate)


###############################################################################
# MustacheNotificationTemplate
###############################################################################
class MustacheNotificationTemplate(NotificationTemplate):
    _lock = threading.RLock()
    _parsed_templates = {}

    ###########################################################################
    @classmethod
    def _parse_template(self, template):
        return pystache.parse(template.decode('utf-8'))

    ###########################################################################
    @classmethod
    def render_string(cls, template_string, context, _=None):
        renderer = pystache.Renderer()
        template = cls._get_template(template_string)
        return renderer.render(template, context)

NotificationTemplate.register('mustache', MustacheNotificationTemplate)

