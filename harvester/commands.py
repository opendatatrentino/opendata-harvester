import logging
# from cliff.command import Command
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
