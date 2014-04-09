import logging
import sys

from cliff.app import App
from cliff.commandmanager import CommandManager


class HarvesterApp(App):
    logger = logging.getLogger(__name__)

    def __init__(self):
        super(HarvesterApp, self).__init__(
            description='Harvester application CLI',
            version='0.1',
            command_manager=CommandManager('harvester.commands'))


def main(argv=sys.argv[1:]):
    myapp = HarvesterApp()
    return myapp.run(argv)


if __name__ == '__main__':
    sys.exit(main())
