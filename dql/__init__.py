""" Simple SQL-like query language for dynamo. """
import os

import argparse

from .cli import DQLClient
from .engine import Engine, FragmentEngine

__version__ = '0.5.9'


def main():
    """ Start the DQL client. """
    parse = argparse.ArgumentParser(description=main.__doc__)
    parse.add_argument('-c', '--command', help="Run this command and exit")
    region = os.environ.get('AWS_REGION', 'us-west-1')
    parse.add_argument('-r', '--region', default=region,
                       help="AWS region to connect to (default %(default)s)")
    parse.add_argument('-H', '--host', default=None,
                       help="Host to connect to if using a local instance "
                       "(default %(default)s)")
    parse.add_argument('-p', '--port', default=8000, type=int,
                       help="Port to connect to "
                       "(default %(default)d)")

    args = parse.parse_args()

    cli = DQLClient()
    cli.initialize(region=args.region, host=args.host, port=args.port)

    if args.command:
        command = args.command.strip()
        if not command.endswith(';'):
            command += ' ;'
        try:
            cli.run_command(command)
        except KeyboardInterrupt:
            pass
    else:
        cli.start()
