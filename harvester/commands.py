import logging

from cliff.command import Command
from cliff.lister import Lister
from stevedore.extension import ExtensionManager

from .utils import get_plugin


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


class Crawl(Command):
    """download data from a source, using a crawler plugin"""

    logger = logging.getLogger(__name__)

    def get_parser(self, prog_name):
        parser = super(Crawl, self).get_parser(prog_name)
        parser.add_argument('--crawler', help='crawler plugin to be used')
        parser.add_argument('--crawler-option', action='append')
        parser.add_argument('--storage', help='storage plugin to be used')
        parser.add_argument('--storage-option', action='append')
        return parser

    def take_action(self, parsed_args):
        crawler = get_plugin(
            'crawlers', parsed_args.crawler,
            parsed_args.crawler_option)
        storage = get_plugin(
            'storage', parsed_args.storage,
            parsed_args.storage_option)

        crawler.fetch_data(storage)


class Convert(Command):
    """clean raw data for import to somewhere"""

    logger = logging.getLogger(__name__)

    def get_parser(self, prog_name):
        parser = super(Convert, self).get_parser(prog_name)
        parser.add_argument('--converter', help='converter name')
        parser.add_argument('--converter-option', action='append')
        parser.add_argument('--input', help='input storage url')
        parser.add_argument('--input-option', action='append')
        parser.add_argument('--output', help='output storage url')
        parser.add_argument('--output-option', action='append')
        return parser

    def take_action(self, parsed_args):
        # We need a converter plugin, an input and an output
        converter = get_plugin(
            'converters', parsed_args.converter,
            parsed_args.converter_option)
        storage_in = get_plugin(
            'storage', parsed_args.input,
            parsed_args.input_option)
        storage_out = get_plugin(
            'storage', parsed_args.output,
            parsed_args.output_option)

        converter.convert(storage_in, storage_out)


class Import(Command):
    """import crawled data to somewhere"""

    logger = logging.getLogger(__name__)

    def get_parser(self, prog_name):
        parser = super(Import, self).get_parser(prog_name)
        parser.add_argument('--storage', help='storage plugin to be used')
        parser.add_argument('--storage-option', action='append')
        parser.add_argument('--importer', help='importer plugin to be used')
        parser.add_argument('--importer-option', action='append')
        return parser

    def take_action(self, parsed_args):
        storage = get_plugin(
            'storage', parsed_args.storage,
            parsed_args.storage_option)
        importer = get_plugin(
            'importers', parsed_args.importer,
            parsed_args.importer_option)

        importer.sync_data(storage)
