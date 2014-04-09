import logging
from cliff.command import Command
from cliff.lister import Lister
from stevedore.extension import ExtensionManager


class ListCrawlers(Lister):
    """list available crawler plugins"""

    logger = logging.getLogger(__name__)

    def take_action(self, parsed_args):
        mgr = ExtensionManager('harvester.ext.crawlers')
        return (
            ('Name', 'Object'),
            sorted([
                (ext.name, repr(ext.plugin))
                for ext in mgr
            ]))


class ListStorages(Lister):
    """list available storage plugins"""

    logger = logging.getLogger(__name__)

    def take_action(self, parsed_args):
        mgr = ExtensionManager('harvester.ext.storage')
        return (
            ('Name', 'Object'),
            sorted([
                (ext.name, repr(ext.plugin))
                for ext in mgr
            ]))


class Crawl(Command):
    """download data from a source, using a crawler plugin"""

    logger = logging.getLogger(__name__)

    def get_parser(self, prog_name):
        parser = super(Crawl, self).get_parser(prog_name)
        parser.add_argument('--crawler', help='crawler plugin to be used')
        parser.add_argument('--storage', help='storage plugin to be used')
        return parser

    def _split_plugin_url(self, url):
        """
        Plugin urls are  in the form::

            <plugin>+<url>

        or just::

            <plugin>
        """
        if '+' in url:
            return url.split('+', 1)
        return url, None

    def take_action(self, parsed_args):
        crawler_mgr = ExtensionManager('harvester.ext.crawlers')
        storage_mgr = ExtensionManager('harvester.ext.storage')

        crawler_name, crawler_url = self._split_plugin_url(parsed_args.crawler)
        try:
            crawler_ep = crawler_mgr[crawler_name]
        except KeyError:
            raise ValueError("Invalid crawler name: {0}"
                             .format(crawler_name))
        crawler_class = crawler_ep.plugin

        storage_name, storage_url = self._split_plugin_url(parsed_args.storage)
        try:
            storage_ep = storage_mgr[storage_name]
        except KeyError:
            raise ValueError("Invalid storage name: {0}"
                             .format(storage_name))
        storage_class = storage_ep.plugin

        self.logger.debug("Using crawler: {0!r}".format(crawler_class))
        self.logger.debug("Using storage: {0!r}".format(storage_class))

        crawler = crawler_class(crawler_url)
        storage = storage_class(storage_url)

        crawler.fetch_data(storage)
