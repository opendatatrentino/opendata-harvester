import logging
import sys

from cliff.app import App
from cliff.commandmanager import CommandManager

from .utils import ColorLogFormatter


class HarvesterApp(App):
    logger = logging.getLogger(__name__)

    def __init__(self):
        super(HarvesterApp, self).__init__(
            description='Harvester application CLI',
            version='0.1',
            command_manager=CommandManager('harvester.commands'))

    def configure_logging(self):
        """
        Create logging handlers for any log output.
        Modified version to set custom formatter for console
        """
        root_logger = logging.getLogger('')
        root_logger.setLevel(logging.DEBUG)

        # Set up logging to a file
        if self.options.log_file:
            file_handler = logging.FileHandler(
                filename=self.options.log_file,
            )
            formatter = logging.Formatter(self.LOG_FILE_MESSAGE_FORMAT)
            file_handler.setFormatter(formatter)
            root_logger.addHandler(file_handler)

        # Always send higher-level messages to the console via stderr
        console = logging.StreamHandler(self.stderr)
        console_level = {0: logging.WARNING,
                         1: logging.INFO,
                         2: logging.DEBUG,
                         }.get(self.options.verbose_level, logging.DEBUG)
        console.setLevel(console_level)
        # formatter = logging.Formatter(self.CONSOLE_MESSAGE_FORMAT)
        formatter = ColorLogFormatter()  # This one is nicer!
        console.setFormatter(formatter)
        root_logger.addHandler(console)
        return


def main(argv=sys.argv[1:]):
    myapp = HarvesterApp()
    return myapp.run(argv)


if __name__ == '__main__':
    sys.exit(main())
