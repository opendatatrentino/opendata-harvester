import logging

from cliff.command import Command


class RunServer(Command):
    logger = logging.getLogger(__name__)

    def get_parser(self, prog_name):
        parser = super(RunServer, self).get_parser(prog_name)
        parser.add_argument('--host', help='Override listen host')
        parser.add_argument('--port', help='Override listen port')
        parser.add_argument(
            '--debug-mode', action='store_true', default=False,
            help='Enable debugging (reload + web-based debugger)')
        return parser

    def take_action(self, parsed_args):
        self.logger.info('Starting server')
        from harvester.director.web import app

        # Note that host/port will default to values from settings
        # in case they are not set from command line..
        app.run(
            host=parsed_args.host,
            port=int(parsed_args.port) if parsed_args.port else None,
            debug=parsed_args.debug_mode or None)


class CeleryWorker(Command):
    logger = logging.getLogger(__name__)

    def take_action(self, parsed_args):
        self.logger.info('Starting celery worker')
        from harvester.director.tasks import worker
        worker.worker_main(['celery-worker'])  # todo: pass in extra arguments
