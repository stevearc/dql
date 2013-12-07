""" Interative DQL client """
import boto.dynamodb2
import boto.exception
import cmd
import functools
import shlex
import subprocess
import traceback
from boto.dynamodb2.layer1 import DynamoDBConnection
from boto.dynamodb2.results import ResultSet
try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict  # pylint: disable=F0401
from pyparsing import ParseException

from .engine import Engine


def repl_command(fxn):
    """
    Decorator for cmd methods

    Parses arguments from the arg string and passes them to the method as *args
    and **kwargs.

    """
    @functools.wraps(fxn)
    def wrapper(self, arglist):
        """Wraps the command method"""
        args = []
        kwargs = {}
        if arglist:
            for arg in shlex.split(arglist):
                if '=' in arg:
                    split = arg.split('=')
                    kwargs[split[0]] = split[1]
                else:
                    args.append(arg)
        return fxn(self, *args, **kwargs)
    return wrapper


def connect(region, host='localhost', port=8000, access_key=None,
            secret_key=None):
    """ Create a DynamoDB connection """
    if region == 'local':
        return DynamoDBConnection(
            host=host,
            port=port,
            is_secure=False,
            aws_access_key_id='',
            aws_secret_access_key='')
    else:
        return boto.dynamodb2.connect_to_region(
            region,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key
        )


class DQLREPL(cmd.Cmd):

    """
    Interactive commandline interface

    Attributes
    ----------
    running : bool
        True while session is active, False after quitting
    ddb : :class:`boto.dynamodb2.layer1.DynamoDBConnection`
    engine : :class:`dql.engine.Engine`

    """
    running = False
    ddb = None
    engine = None
    _access_key = None
    _secret_key = None

    def initialize(self, region='us-west-1', host='localhost', port=8000,
                   access_key=None, secret_key=None):
        """ Set up the repl for execution """
        self._access_key = access_key
        self._secret_key = secret_key
        self.prompt = region + '> '
        self.ddb = connect(region, host, port, access_key, secret_key)
        self.engine = Engine(self.ddb)

    def start(self):
        """ Start running the interactive session (blocking) """
        self.running = True
        while self.running:
            try:
                self.cmdloop()
            except KeyboardInterrupt:
                print
            except boto.exception.JSONResponseError as e:
                try:
                    print e.body['Message']
                except KeyError:
                    print e
            except ParseException as e:
                print " " * (e.loc + len(self.prompt)) + "^"
                print e
            except:
                traceback.print_exc()

    def help_help(self):
        """Print the help text for help"""
        print "List commands or print details about a command"

    def do_shell(self, arglist):
        """ Run a shell command """
        print subprocess.check_output(shlex.split(arglist))

    @repl_command
    def do_ls(self, table=None):
        """ List all tables or print details of one table """
        if table is None:
            fields = OrderedDict([('Name', 'name'),
                                  ('Status', 'status'),
                                  ('Read', 'read_throughput'),
                                  ('Write', 'write_throughput')])
            tables = self.engine.describe_all()
            # Calculate max width of all items for each column
            sizes = [1 + max([len(str(getattr(t, f))) for t in tables] +
                             [len(title)]) for title, f in fields.iteritems()]
            # Print the header
            for size, title in zip(sizes, fields):
                print title.ljust(size),
            print
            # Print each table row
            for table in tables:
                for size, field in zip(sizes, fields.values()):
                    print str(getattr(table, field)).ljust(size),
                print
        else:
            print self.engine.describe(table, refresh=True).pformat()

    @repl_command
    def do_use(self, region, host='localhost', port=8000):
        """
        Switch the AWS region

        You may also specify 'use local host=localhost port=8000' to use the
        DyanmoDB Local service

        """
        self.prompt = region + '> '
        self.ddb = connect(region, host, port, self._access_key,
                           self._secret_key)
        self.engine.connection = self.ddb

    def default(self, command):
        command = command.strip()
        if not command:
            print
            return
        results = self.engine.execute(command)
        if isinstance(results, ResultSet):
            for result in results:
                print(20 * '-')
                for key, val in result.items():
                    print("{0}: {1:<.100}".format(key, repr(val)))
        else:
            print results

    @repl_command
    def do_EOF(self):  # pylint: disable=C0103
        """Exit"""
        return self.onecmd('exit')

    @repl_command
    def do_exit(self):
        """Exit"""
        self.running = False
        print
        return True

    def emptyline(self):
        pass
