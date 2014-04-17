"""
Miscellaneous utilities
"""

import urlparse

from stevedore.extension import ExtensionManager


def get_plugin(plugin_type, url, options):
    """
    Get an instance of the selected plugin, getting its name
    from the URL and passing "options" as configuration.

    :param plugin_type:
        The plugin type. Will prepend ``harvester.ext.`` to
        the passed-in string.
    :param url: the plugin URL, like ``name://host``, ``name``
        or ``name+scheme://host``
    :param options: options passed on the command line
        (list of strings like ``key=value`` or ``type:key=value``)
    """

    def _to_bool(s):
        s = s.lower()
        if s in ['0', 'false', 'off', 'no']:
            return False
        if s in ['1', 'true', 'on', 'yes']:
            return True
        raise ValueError("Not a boolean: {0}".format(s))

    type_converters = {
        'int': int,
        'bool': _to_bool,
        'str': lambda x: x,
    }

    crawler_mgr = ExtensionManager('harvester.ext.{0}'.format(plugin_type))

    name, url = parse_plugin_url(url)

    try:
        ep = crawler_mgr[name]
    except KeyError:
        raise ValueError("Invalid plugin name: {0}" .format(name))

    conf = {}
    if options is not None:
        for option in options:
            key, value = [x.strip() for x in option.split('=', 1)]
            if ':' in key:
                k_type, key = key.split(':', 1)
                value = type_converters[k_type](value)
            conf[key] = value

    return ep.plugin(url, conf)


def parse_plugin_url(url):
    """
    >>> parse_plugin_url("name")
    'name', None

    >>> parse_plugin_url('name+url')
    'name', 'url'

    >>> parse_plugin_url('name://url')
    'name', 'name://url'

    >>> parse_plugin_url('name+scheme://url')
    'name', 'scheme://url'

    >>> parse_plugin_url('name://url+other')
    'name', 'name://url+other'
    """

    plus_pos = url.find('+')
    css_pos = url.find('://')  # colon-slash-slash

    if plus_pos >= 0:
        # We have a ``+``
        if css_pos < 0 or css_pos > plus_pos:
            # We don't have a ``://`` or it's after the ``+``
            # ``name+scheme://url`` or ``name+something``
            return url[:plus_pos], url[plus_pos+1:]

    if css_pos >= 0:
        # The :// is before any +, we use the scheme as name
        return url[:css_pos], url

    # We don't have neither of ``+`` or ``://``
    return url, None
