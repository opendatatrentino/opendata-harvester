import logging
from cliff.command import Command
from cliff.lister import Lister
from stevedore.extension import ExtensionManager


class PluginLister(Lister):
    plugin_namespace = 'harvester.ext.crawlers'
    logger = logging.getLogger(__name__)

    def _get_repr(self, obj):
        return '.'.join((obj.__module__, obj.__name__))

    def take_action(self, parsed_args):
        mgr = ExtensionManager(self.plugin_namespace)
        rows = sorted([(ext.name, self._get_repr(ext.plugin)) for ext in mgr])
        return (('Name', 'Class'), rows)


class ListCrawlers(PluginLister):
    """list available crawler plugins"""
    plugin_namespace = 'harvester.ext.crawlers'


class ListStorages(PluginLister):
    """list available storage plugins"""
    plugin_namespace = 'harvester.ext.storage'


class ListConverters(PluginLister):
    """list available converter plugins"""
    plugin_namespace = 'harvester.ext.converters'


class ListImporters(PluginLister):
    """list available importer plugins"""
    plugin_namespace = 'harvester.ext.importers'


def get_plugin(plugin_type, url, options):
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
    }

    crawler_mgr = ExtensionManager('harvester.ext.{0}'.format(plugin_type))

    name, url = url.split('+', 1) if '+' in url else (url, None)

    try:
        ep = crawler_mgr[name]
    except KeyError:
        raise ValueError("Invalid crawler name: {0}" .format(name))

    conf = {}
    for option in options:
        key, value = [x.strip() for x in option.split('=', 1)]
        if ':' in key:
            k_type, key = key.split(':', 1)
            value = type_converters[k_type](value)
        conf[key] = value

    return ep.plugin(url, conf)


class Crawl(Command):
    """download data from a source, using a crawler plugin"""

    logger = logging.getLogger(__name__)

    def get_parser(self, prog_name):
        parser = super(Crawl, self).get_parser(prog_name)
        parser.add_argument('--crawler', help='crawler plugin to be used')
        parser.add_argument('--crawler-options', action='append')
        parser.add_argument('--storage', help='storage plugin to be used')
        parser.add_argument('--storage-options', action='append')
        return parser

    def take_action(self, parsed_args):
        crawler = get_plugin(
            'crawlers', parsed_args.crawler,
            parsed_args.crawler_options)
        storage = get_plugin(
            'storages', parsed_args.storage,
            parsed_args.storage_options)

        crawler.fetch_data(storage)


class Convert(Command):
    """clean raw data for import to somewhere"""

    logger = logging.getLogger(__name__)

    def get_parser(self, prog_name):
        parser = super(Convert, self).get_parser(prog_name)
        parser.add_argument('--converter', help='converter name')
        parser.add_argument('--converter-options', action='append')
        parser.add_argument('--input', help='input storage url')
        parser.add_argument('--input-options', action='append')
        parser.add_argument('--output', help='output storage url')
        parser.add_argument('--output-options', action='append')
        return parser

    def take_action(self, parsed_args):
        # We need a converter plugin, an input and an output
        converter = get_plugin(
            'converter', parsed_args.converter,
            parsed_args.converter_options)
        storage_in = get_plugin(
            'storage', parsed_args.input,
            parsed_args.input_options)
        storage_out = get_plugin(
            'storage', parsed_args.output,
            parsed_args.output_options)

        converter.convert(storage_in, storage_out)


class Import(Command):
    """import crawled data to somewhere"""

    logger = logging.getLogger(__name__)

    def get_parser(self, prog_name):
        parser = super(Import, self).get_parser(prog_name)
        parser.add_argument('--storage', help='storage plugin to be used')
        parser.add_argument('--storage-options', action='append')
        parser.add_argument('--importer', help='importer plugin to be used')
        parser.add_argument('--importer-options', action='append')
        return parser

    def take_action(self, parsed_args):
        storage = get_plugin(
            'storages', parsed_args.storage,
            parsed_args.storage_options)
        importer = get_plugin(
            'importers', parsed_args.importer,
            parsed_args.importer_options)

        importer.sync_data(storage)
