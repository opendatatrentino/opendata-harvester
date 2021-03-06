"""
Miscellaneous utilities.

.. todo:: Refactor this, to provide better distinction between
          options passed from the command line and options specified
          directly from Python library calls.

          In the latter case, we just need to validate options (?)
          and set default values.
"""

from collections import namedtuple
import hashlib
import json
import logging
import re
import socket
import sys
import warnings

from stevedore.extension import ExtensionManager
from termcolor import colored
from unidecode import unidecode
import eventlite

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


def get_plugin_class(plugin_type, name):
    """
    Get a plugin class, based on plugin type and name.
    This is mostly a wrapper around stevedore's ExtensionManager.

    :param plugin_type:
        The plugin type, without the ``harvester.ext.`` prefix

    :param name:
        The plugin name, as configured in the setuptools' entry_points

    :return:
        The plugin class
    """

    extmgr = ExtensionManager('harvester.ext.{0}'.format(plugin_type))
    try:
        ep = extmgr[name]
    except KeyError:
        raise ValueError("Invalid plugin name: {0}".format(name))
    return ep.plugin


def get_plugin_options(plugin):
    warnings.warn('get_plugin_options() is deprecated: '
                  'use plugin.get_options() instead',
                  DeprecationWarning)
    return plugin.get_options()


def plugin_options_from_cmdline(options):
    """
    Convert a list of options from command-line arguments
    into a format suitable for passing to plugin constructor.
    """

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
    return conf_options


def prepare_plugin_options(plugin_class, options):
    """
    Prepare plugin options by extracting / converting supported
    values from a plugin.
    """

    opt_schema = plugin_class.get_options()

    if options is None:
        options = {}

    conf = {}
    for opt_def in opt_schema.itervalues():
        if opt_def.name in options:
            # Take type, value from the passed-in value
            type_, value = options.pop(opt_def.name)

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

    for name in options:
        warnings.warn(
            "Unknown configuration option {0!r} passed to plugin {1!r}"
            .format(name, plugin_class),
            UserWarning)

    return conf


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

    name, url = parse_plugin_url(url)
    plugin_class = get_plugin_class(plugin_type, name)

    # First, parse options passed from the command line
    # We store a mapping of "key name" : [(type, value), ..]

    if isinstance(options, (list, tuple)):
        options = plugin_options_from_cmdline(options)

    # Now, we start reading actual configuration required
    # by the plugin

    conf = prepare_plugin_options(plugin_class, options)

    return plugin_class(url, conf)


def get_storage(url, options):
    return get_plugin('storage', url, options)


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


def to_ordinal(number):
    """Return the "ordinal" representation of a number"""

    assert isinstance(number, int)

    sr = str(number)  # string representation
    ld = sr[-1]  # last digit

    try:
        # Second to last digit
        stld = sr[-2]
    except IndexError:
        stld = None

    if stld != '1':
        if ld == '1':
            return sr + 'st'
        if ld == '2':
            return sr + 'nd'
        if ld == '3':
            return sr + 'rd'

    return sr + 'th'


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


def check_tcp_port(host, port, timeout=3):
    """
    Try connecting to a given TCP port.

    :param host: Host to connect to
    :param port: TCP port to connect to
    :param timeout: Connection timeout, in seconds
    :return: True if the port is open, False otherwise.
    """

    s = socket.socket()
    try:
        s.settimeout(timeout)
        s.connect((host, port))
    except socket.error:
        return False
    else:
        s.close()
        return True


def get_storage_direct(url, options=None):
    """
    Get storage directly, trusting the passed-in options.
    """
    name, url = parse_plugin_url(url)
    plugin_class = get_plugin_class('storage', name)
    return plugin_class(url, options)


def get_storage_from_arg(arg):
    """
    Get a storage instance from an argument to a function.

    This is needed for functions that may be called via
    an external tool that doesn't allow passing object instances
    directly.
    """

    from harvester.ext.storage.base import BaseStorage

    if isinstance(arg, BaseStorage):
        return arg

    if isinstance(arg, basestring):
        return get_storage_direct(arg, options={})

    return get_storage_direct(
        arg['url'], options=arg.get('conf', None))


# ------------------------------------------------------------
# JobControl integration
# ------------------------------------------------------------


class ProgressReport(object):
    def __init__(self, name, current, total, status_line=None):
        self.group_name = name
        self.current = current
        self.total = total
        self.status_line = status_line


def report_progress(name, cur, tot, status=None):
    eventlite.emit(ProgressReport(name, cur, tot, status))


def handle_progress_report_events(*a, **kw):
    from jobcontrol.globals import current_app

    if len(a) and isinstance(a[0], ProgressReport):
        report = a[0]
        current_app.report_progress(
            report.group_name, report.current, report.total,
            report.status_line)


def jobcontrol_integration():
    """Returns a context manager providing jobcontrol integration stuff"""
    return eventlite.handler(handle_progress_report_events)
