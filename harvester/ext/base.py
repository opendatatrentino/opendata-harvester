from collections import namedtuple

plugin_option = namedtuple('plugin_option', 'name,type,default,doc')


class PluginBase(object):
    options = []  # (name, type, default, doc)

    def __init__(self, url, conf=None):
        self.url = url
        self.conf = conf or {}

    def __getstate__(self):
        # Return state for pickling.
        # At unpicklet time, __dict__ will be set to the returned
        # value. Also note that __init__ will **not** be called!
        return {'url': self.url, 'conf': self.conf}

    def __repr__(self):
        return '{0}({1!r}, conf={2!r})'.format(
            self.__class__.__name__, self.url, self.conf)

    def __unicode__(self):
        return unicode(self.url)

    @classmethod
    def get_options(cls):
        """
        Get a list of options accepted by this plugin.

        By default, configuration options will be inherited from classes
        along the whole inheritance path of the current plugin.

        If you really want to remove an option, just set its type to
        ``None`` in the plugin ``options``, eg::

            options = [('my_option', None)]

        :return: a list of ``plugin_option`` named tuples.
        """

        options = {}

        for subobj in reversed(cls.mro()):
            if getattr(subobj, 'options', None) is not None:
                for item in subobj.options:
                    plgo = plugin_option(*(item + (None,) * (4 - len(item))))
                    if plgo.type is None:
                        options.pop(plgo.name, None)
                    else:
                        options[plgo.name] = plgo
        return options
