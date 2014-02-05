""" Interative DQL client """
import os

import boto.dynamodb2
import boto.exception
import cmd
import functools
import inspect
import json
import shlex
import subprocess
import traceback
from boto.dynamodb2.layer1 import DynamoDBConnection
from boto.dynamodb2.results import ResultSet
from boto.regioninfo import RegionInfo
from pyparsing import ParseException

from .engine import FragmentEngine
from .help import (ALTER, COUNT, CREATE, DELETE, DROP, DUMP, INSERT, SCAN,
                   SELECT, UPDATE)
from .output import (ColumnFormat, ExpandedFormat, SmartFormat,
                     get_default_display, less_display, stdout_display)


try:
    from collections import OrderedDict
except ImportError:  # pragma: no cover
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
                    split = arg.split('=', 1)
                    kwargs[split[0]] = split[1]
                else:
                    args.append(arg)
        return fxn(self, *args, **kwargs)
    return wrapper


def connect(region, host='localhost', port=8000, access_key=None,
            secret_key=None):
    """ Create a DynamoDB connection """
    if region == 'local':
        region = RegionInfo(name='local', endpoint=host,
                            connection_cls=DynamoDBConnection)
        return DynamoDBConnection(
            region=region,
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

DISPLAYS = {
    'stdout': stdout_display,
    'less': less_display,
}
RDISPLAYS = dict(((v, k) for k, v in DISPLAYS.iteritems()))


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
    formatter = None
    display = None
    _access_key = None
    _secret_key = None
    _coding = False
    _conf_dir = None

    def initialize(self, region='us-west-1', host='localhost', port=8000,
                   access_key=None, secret_key=None):
        """ Set up the repl for execution """
        # Tab-complete names with a '-' in them
        import readline
        delims = set(readline.get_completer_delims())
        if '-' in delims:
            delims.remove('-')
            readline.set_completer_delims(''.join(delims))

        self._conf_dir = os.path.join(os.environ.get('HOME', '.'), '.config')
        self._access_key = access_key
        self._secret_key = secret_key
        self.region = region
        self.ddb = connect(region, host, port, access_key, secret_key)
        self.engine = FragmentEngine(self.ddb)
        conf = self.load_config()
        display_name = conf.get('display')
        if display_name is not None:
            self.display = DISPLAYS[display_name]
        else:
            self.display = get_default_display()
        self.formatter = SmartFormat(pagesize=conf.get('pagesize', 1000),
                                     width=conf.get('width', 80))
        for line in conf.get('autorun', []):
            exec line in self.engine.scope

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
                if e.error_message is not None:
                    print e.error_message
                else:
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
        proc = subprocess.Popen(shlex.split(arglist),
                                stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT)
        print proc.communicate()[0]

    def save_config_value(self, key, value):
        """ Save your configuration settings to a file """
        if not os.path.exists(self._conf_dir):
            os.makedirs(self._conf_dir)
        conf_file = os.path.join(self._conf_dir, 'dql.json')
        conf = self.load_config()
        conf[key] = value
        with open(conf_file, 'w') as ofile:
            json.dump(conf, ofile, indent=2)

    def load_config(self):
        """ Load your configuration settings from a file """
        conf_file = os.path.join(self._conf_dir, 'dql.json')
        if not os.path.exists(conf_file):
            return {}
        with open(conf_file, 'r') as ifile:
            return json.load(ifile)

    @repl_command
    def do_width(self, width=None):
        """ Get or set the width of the formatted output """
        if width is not None:
            self.formatter.width = int(width)
            self.save_config_value('width', int(width))
        print self.formatter.width
        print self.formatter.width * '-'

    @repl_command
    def do_pagesize(self, pagesize=None):
        """ Get or set the page size of the query output """
        if pagesize is None:
            print self.formatter.pagesize
        else:
            self.formatter.pagesize = int(pagesize)
            self.save_config_value('pagesize', int(pagesize))

    @repl_command
    def do_display(self, display=None):
        """ Get or set the type of display to use when printing results """
        if display is None:
            for key, val in DISPLAYS.iteritems():
                if val == self.display:
                    print '* %s' % key
                else:
                    print '  %s' % key
        else:
            self.display = DISPLAYS[display]
            self.save_config_value('display', display)

    def complete_display(self, text, *_):
        """ Autocomplete for display """
        return [t + ' ' for t in DISPLAYS if t.startswith(text)]

    @repl_command
    def do_x(self, smart='false'):
        """
        Toggle expanded display format

        You can set smart formatting with 'x smart'
        """
        if smart.lower() in ('smart', 'true'):
            self.formatter = SmartFormat(width=self.formatter.width,
                                         pagesize=self.formatter.pagesize)
            print "Smart format enabled"
        elif isinstance(self.formatter, ExpandedFormat):
            self.formatter = ColumnFormat(width=self.formatter.width,
                                          pagesize=self.formatter.pagesize)
            print "Expanded format disabled"
        else:
            self.formatter = ExpandedFormat(width=self.formatter.width,
                                            pagesize=self.formatter.pagesize)
            print "Expanded format enabled"

    @repl_command
    def do_file(self, filename):
        """ Read and execute a .dql file """
        with open(filename, 'r') as infile:
            self._run_cmd(infile.read())

    def complete_file(self, text, line, *_):
        """ Autocomplete DQL file lookup """
        leading = line[len('file '):]
        curpath = os.path.join(os.path.curdir, leading)

        def isdql(parent, filename):
            """ Check if a file is .dql or a dir """
            return (not filename.startswith('.') and
                    (os.path.isdir(os.path.join(parent, filename)) or
                     filename.lower().endswith('.dql')))

        def addslash(path):
            """ Append a slash if a file is a directory """
            if path.lower().endswith('.dql'):
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
                                  ('Read', 'total_read_throughput'),
                                  ('Write', 'total_write_throughput')])
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
        print "Default: '%s'" % command
        if self._coding:
            exec command in self.engine.scope
        else:
            self._run_cmd(command)

    def _run_cmd(self, command):
        """ Run a DQL command """
        results = self.engine.execute(command)
        if isinstance(results, ResultSet) or inspect.isgenerator(results):
            has_more = True
            while has_more:
                with self.display() as ostream:
                    has_more = self.formatter.write(results, ostream)
                if has_more:
                    raw_input("Press return for next %d results:" %
                              self.formatter.pagesize)
        elif results is not None:
            print results

    @repl_command
    def do_EOF(self):  # pylint: disable=C0103
        """Exit"""
        if self._coding:
            print
            return self.onecmd('endcode')
        else:
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
