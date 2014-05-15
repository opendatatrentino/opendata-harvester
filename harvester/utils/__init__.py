"""
Miscellaneous utilities.
"""

from collections import namedtuple
import hashlib
import json
import logging
import re
import sys
import warnings

from stevedore.extension import ExtensionManager
from termcolor import colored
from unidecode import unidecode

from .flattening import *  # noqa
from .xml_data_extraction import *  # noqa


# todo: find a nicer way to make this configurable, eg. from
#       some environment variable..
POWERLINE_STYLE = True


plugin_option = namedtuple('plugin_option', 'name,type,default,doc')


def convert_string(type_, string):
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

    if isinstance(type_, type):
        return type_(string)

    if callable(type_):
        return type_(string)

    if type_ in type_converters:
        return type_converters[type_](string)

    raise ValueError("Invalid type: {0!r}".format(type_))


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

    crawler_mgr = ExtensionManager('harvester.ext.{0}'.format(plugin_type))

    name, url = parse_plugin_url(url)

    try:
        ep = crawler_mgr[name]
    except KeyError:
        raise ValueError("Invalid plugin name: {0}" .format(name))

    # First, parse options passed from the command line
    # We store a mapping of "key name" : [(type, value), ..]

    conf_options = {}
    if options is not None:
        for option in options:
            key, value = [x.strip() for x in option.split('=', 1)]
            if ':' in key:
                k_type, key = key.split(':', 1)
                value = convert_string(k_type, value)
            else:
                k_type = None
            conf_options[key] = (k_type, value)

    # Now, we start reading actual configuration required
    # by the plugin

    def _get_options(opts):
        for item in opts:
            # Make sure we have all the four fields -- fill missing
            # ones with None
            yield plugin_option(*(item + (None,) * (4 - len(item))))

    conf = {}
    for opt_def in _get_options(ep.plugin.options):
        if opt_def.name in conf_options:
            # Take type, value from the passed-in value
            type_, value = conf_options.pop(opt_def.name)

            # If type is not specified on the command line,
            # use the default type for this option.
            if type_ is None:
                type_ = opt_def.type

            # Perform type conversion
            value = convert_string(type_, value)
            conf[opt_def.name] = value

        else:
            # Option was not specified -- use default
            conf[opt_def.name] = opt_def.default

    # Now trigger warnings for any option left around..

    for name in conf_options:
        warnings.warn(
            "Unknown configuration option {0!r} passed to plugin {1!r}"
            .format(name, ep.plugin),
            UserWarning)

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


# try:
#     from termcolor import colored
# except ImportError:
#     def colored(text, *a, **kw):
#         return text


# def _colorer(*args, **kwargs):
#     return lambda x: colored(x, *args, **kwargs)


class ColorLogFormatter(logging.Formatter):
    level_colors = {
        logging.DEBUG: 'cyan',
        logging.INFO: 'green',
        logging.WARNING: 'yellow',
        logging.ERROR: 'red',
        logging.CRITICAL: 'red',
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

        color = self.level_colors.get(record.levelno, 'white')

        levelname = colored(' {0:<6} '.format(record.levelname),
                            color, attrs=['reverse'])

        if POWERLINE_STYLE:
            levelname += colored(u'\ue0b0', color, 'on_white')

        loggername = colored(' {0} '.format(record.name), 'red', 'on_white')

        if POWERLINE_STYLE:
            loggername += colored(u'\ue0b0', 'white')

        message = colored(record.getMessage(), color)

        s = ' '.join((''.join((levelname, loggername)), message))

        exc_info = self._get_exc_info(record)
        if exc_info is not None:
            if s[-1:] != "\n":
                s = s + "\n"
            s += colored(exc_info, 'grey', attrs=['bold'])

        return s


def slugify(text):
    """
    Convert a string of text to a more compact representation,
    suitable to be used in a URL.

    The unidecode library will be used to transliterate non-ascii
    letters too..

    >>> slugify('Hello, world!')
    'hello-world'
    """
    if not isinstance(text, unicode):
        text = text.decode('utf-8')
    text = unidecode(text)
    text = text.lower()
    return re.sub(r'[^a-z0-9]+', '-', text).strip('-')


def decode_faulty_json(text):
    """
    Attempt to decode json containing newlines inside strings,
    which is invalid for the JSON standard.
    """
    text = text.replace('\n', ' ').replace('\r', '')
    return json.loads(text)


def get_robohash_url(text):
    h = hashlib.sha1(text).hexdigest()
    return 'http://robohash.org/{0}.png?set=set1&bgset=bg1'.format(h)


def normalize_case(text):
    """
    Normalize case of some (all-{upper|lower}case) text.
    Uses a wordlist to determine which words need capitalization.
    """

    # todo: figure out a smarter way :)
    SPECIAL_CASE_WORDS = [
        'Trento', 'Provincia',
    ]
    text = text.lower()
    for word in SPECIAL_CASE_WORDS:
        text.replace(word.lower(), word)
    return text.capitalize()


def lazy_property(fn):
    attr_name = '_lazy_' + fn.__name__

    def getter(self):
        if not hasattr(self, attr_name):
            setattr(self, attr_name, fn(self))
        return getattr(self, attr_name)

    def setter(self, value):
        setattr(self, attr_name, value)

    def deleter(self):
        delattr(self, attr_name)

    return property(fget=getter, fset=setter, fdel=deleter, doc=fn.__doc__)
