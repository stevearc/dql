""" Simple SQL-like query language for dynamo. """
from __future__ import print_function
import os

import argparse
import logging.config

from .cli import DQLClient
from .engine import Engine, FragmentEngine

__version__ = "0.5.27"


LOG_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {"brief": {"format": "%(message)s"}},
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "brief",
            "stream": "ext://sys.stdout",
        }
    },
    "root": {"level": "ERROR", "handlers": ["console"]},
    "loggers": {"dql": {"level": "INFO", "propagate": True}},
}


def main():
    """ Start the DQL client. """
    parse = argparse.ArgumentParser(description=main.__doc__)
    parse.add_argument("-c", "--command", help="Run this command and exit")
    region = os.environ.get("AWS_REGION", "us-west-1")
    parse.add_argument(
        "-r",
        "--region",
        default=region,
        help="AWS region to connect to (default %(default)s)",
    )
    parse.add_argument(
        "-H",
        "--host",
        default=None,
        help="Host to connect to if using a local instance " "(default %(default)s)",
    )
    parse.add_argument(
        "-p",
        "--port",
        default=8000,
        type=int,
        help="Port to connect to " "(default %(default)d)",
    )
    parse.add_argument(
        "--version", action="store_true", help="Print the version and exit"
    )
    args = parse.parse_args()

    if args.version:
        print(__version__)
        return

    logging.config.dictConfig(LOG_CONFIG)
    cli = DQLClient()
    cli.initialize(region=args.region, host=args.host, port=args.port)

    if args.command:
        command = args.command.strip()
        try:
            cli.run_command(command)
            if cli.engine.partial:
                cli.run_command(";")
        except KeyboardInterrupt:
            pass
    else:
        cli.start()
