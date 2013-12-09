""" Interative DQL client """
import os

import boto.dynamodb2
import boto.exception
import cmd
import functools
import shlex
import subprocess
import traceback
from boto.dynamodb2.layer1 import DynamoDBConnection
from boto.dynamodb2.results import ResultSet
from pyparsing import ParseException

from .engine import FragmentEngine
from .help import (ALTER, COUNT, CREATE, DELETE, DROP, DUMP, INSERT, SCAN,
                   SELECT, UPDATE)


try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict  # pylint: disable=F0401


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


class DQLClient(cmd.Cmd):

    """
    Interactive commandline interface

    Attributes
    ----------
    running : bool
        True while session is active, False after quitting
    ddb : :class:`boto.dynamodb2.layer1.DynamoDBConnection`
    engine : :class:`dql.engine.FragmentEngine`

    """
    running = False
    ddb = None
    engine = None
    region = None
    _access_key = None
    _secret_key = None
    _coding = False
    _scope = {}

    def initialize(self, region='us-west-1', host='localhost', port=8000,
                   access_key=None, secret_key=None):
        """ Set up the repl for execution """
        self._access_key = access_key
        self._secret_key = secret_key
        self.region = region
        self.ddb = connect(region, host, port, access_key, secret_key)
        self.engine = FragmentEngine(self.ddb)

    def start(self):
        """ Start running the interactive session (blocking) """
        self.running = True
        while self.running:
            self.update_prompt()
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
                print self.engine.pformat_exc(e)
            except:
                traceback.print_exc()
            self.engine.reset()

    def postcmd(self, stop, line):
        self.update_prompt()
        return stop

    def update_prompt(self):
        """ Update the prompt """
        if self._coding:
            self.prompt = '>>> '
        elif self.engine.partial:
            self.prompt = len(self.region) * ' ' + '> '
        else:
            self.prompt = self.region + '> '

    def do_shell(self, arglist):
        """ Run a shell command """
        print subprocess.check_output(shlex.split(arglist))

    @repl_command
    def do_file(self, filename):
        """ Read and execute a .dql file """
        with open(filename, 'r') as infile:
            self._run_cmd(infile.read(), pdql=filename.lower().endswith('.py'))

    def complete_file(self, text, line, *_):
        """ Autocomplete DQL file lookup """
        leading = line[len('file '):]
        curpath = os.path.join(os.path.curdir, leading)

        def isdql(parent, filename):
            """ Check if a file is .dql or a dir """
            return (not filename.startswith('.') and
                    (os.path.isdir(os.path.join(parent, filename)) or
                     filename.lower().endswith('.dql') or
                     filename.lower().endswith('.py')))

        def addslash(path):
            """ Append a slash if a file is a directory """
            if path.lower().endswith('.dql') or path.lower().endswith('.py'):
                return path + ' '
            else:
                return path + '/'
        if not os.path.exists(curpath) or not os.path.isdir(curpath):
            curpath = os.path.dirname(curpath)
        return [addslash(f) for f in os.listdir(curpath) if f.startswith(text)
                and isdql(curpath, f)]

    @repl_command
    def do_code(self):
        """ Switch to executing python code """
        self._coding = True

    @repl_command
    def do_endcode(self):
        """ Stop executing python code """
        self._coding = False

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
            print self.engine.describe(table, refresh=True,
                                       metrics=True).pformat()

    def complete_ls(self, text, *_):
        """ Autocomplete for ls """
        return [t + ' ' for t in self.engine.cached_descriptions if
                t.startswith(text)]

    @repl_command
    def do_use(self, region, host='localhost', port=8000):
        """
        Switch the AWS region

        You may also specify 'use local host=localhost port=8000' to use the
        DynamoDB Local service

        """
        self.region = region
        self.ddb = connect(region, host, port, self._access_key,
                           self._secret_key)
        self.engine.connection = self.ddb

    def default(self, command):
        if self._coding:
            exec command in self._scope
        else:
            self._run_cmd(command)

    def _run_cmd(self, command, pdql=False):
        """ Run a DQL command """
        if pdql:
            results = self.engine.execute_pdql(command)
        else:
            results = self.engine.execute(command, scope=self._scope)
        if isinstance(results, ResultSet):
            for result in results:
                print(20 * '-')
                for key, val in result.items():
                    print("{0}: {1:<.100}".format(key, repr(val)))
        elif results is not None:
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
        self.default('')

    def help_help(self):
        """Print the help text for help"""
        print "List commands or print details about a command"

    def help_alter(self):
        """ Print the help text for ALTER """
        print ALTER

    def help_count(self):
        """ Print the help text for COUNT """
        print COUNT

    def help_create(self):
        """ Print the help text for CREATE """
        print CREATE

    def help_delete(self):
        """ Print the help text for DELETE """
        print DELETE

    def help_drop(self):
        """ Print the help text for DROP """
        print DROP

    def help_dump(self):
        """ Print the help text for DUMP """
        print DUMP

    def help_insert(self):
        """ Print the help text for INSERT """
        print INSERT

    def help_scan(self):
        """ Print the help text for SCAN """
        print SCAN

    def help_select(self):
        """ Print the help text for SELECT """
        print SELECT

    def help_update(self):
        """ Print the help text for UPDATE """
        print UPDATE
