import logging

from cliff.app import App

from harvester.utils import ColorLogFormatter


class CustomBaseApp(App):
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
