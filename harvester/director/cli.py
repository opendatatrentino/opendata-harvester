import logging
import sys

from cliff.commandmanager import CommandManager

from harvester.utils.cli import CustomBaseApp


class HarvestDirector(CustomBaseApp):
    logger = logging.getLogger(__name__)

    def __init__(self):
        super(HarvestDirector, self).__init__(
            description='Harvester director CLI',
            version='0.1',
            command_manager=CommandManager('harvester.director.commands'))


def main(argv=sys.argv[1:]):
    myapp = HarvestDirector()
    return myapp.run(argv)


if __name__ == '__main__':
    sys.exit(main())
