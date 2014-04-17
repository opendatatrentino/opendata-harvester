"""
Miscellaneous utilities
"""

import logging
import sys

from stevedore.extension import ExtensionManager


POWERLINE_STYLE = True


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


try:
    from termcolor import colored
except ImportError:
    def colored(text, *a, **kw):
        return text


def _colorer(*args, **kwargs):
    return lambda x: colored(x, *args, **kwargs)


class ColorLogFormatter(logging.Formatter):
    level_colors = {
        logging.DEBUG: ('white', 'blue', 'cyan'),
        logging.INFO: ('white', 'green', 'green'),
        logging.WARNING: ('white', 'yellow', 'yellow'),
        logging.ERROR: ('white', 'red', 'red'),
        logging.CRITICAL: ('yellow', 'red', 'red'),
    }

    def _get_exc_info(self, record):
        if record.exc_info:
            # Cache the traceback text to avoid converting it multiple times
            # (it's constant anyway)
            if not record.exc_text:
                record.exc_text = self.formatException(record.exc_info)
        if record.exc_text:
            try:
                return unicode(record.exc_text)
            except UnicodeError:
                # Sometimes filenames have non-ASCII chars, which can lead
                # to errors when s is Unicode and record.exc_text is str
                # See issue 8924.
                # We also use replace for when there are multiple
                # encodings, e.g. UTF-8 for the filesystem and latin-1
                # for a script. See issue 13232.
                return record.exc_text.decode(sys.getfilesystemencoding(),
                                              'replace')
        return None

    def format(self, record):
        """Format logs nicely"""

        fgcolor, bgcolor, msgcolor = \
            self.level_colors.get(record.levelno, 'white')
        levelname = colored(' {0:<6} '.format(record.levelname),
                            fgcolor, 'on_' + bgcolor)
        if POWERLINE_STYLE:
            levelname += colored(u'\ue0b0', bgcolor)

        message = colored(record.getMessage(), msgcolor)

        s = ' '.join((levelname, message))

        exc_info = self._get_exc_info(record)
        if exc_info is not None:
            if s[-1:] != "\n":
                s = s + "\n"
            s += colored(exc_info, 'grey', attrs=['bold'])

        return s
