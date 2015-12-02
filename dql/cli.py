""" Interative DQL client """
import os
from fnmatch import fnmatch

import botocore
import cmd
import functools
import json
import shlex
import six
import subprocess
import traceback
from pyparsing import ParseException

from .engine import FragmentEngine
from .help import (ALTER, ANALYZE, CREATE, DELETE, DROP, DUMP, INSERT, SCAN,
                   SELECT, UPDATE, OPTIONS, EXPLAIN)
from .monitor import Monitor
from .output import (ColumnFormat, ExpandedFormat, SmartFormat,
                     get_default_display, less_display, stdout_display)


try:
    from collections import OrderedDict
except ImportError:  # pragma: no cover
    from ordereddict import OrderedDict  # pylint: disable=F0401

# From http://docs.aws.amazon.com/general/latest/gr/rande.html#ddb_region
REGIONS = [
    'us-east-1',
    'us-west-2',
    'us-west-1',
    'eu-west-1',
    'eu-central-1',
    'ap-southeast-1',
    'ap-southeast-2',
    'ap-northeast-1',
    'sa-east-1',
]


def indent(string, prefix='  '):
    """ Indent a paragraph of text """
    return '\n'.join([prefix + line for line in string.split('\n')])


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


DISPLAYS = {
    'stdout': stdout_display,
    'less': less_display,
}
FORMATTERS = {
    'smart': SmartFormat,
    'expanded': ExpandedFormat,
    'column': ColumnFormat,
}
DEFAULT_CONFIG = {
    'width': 100,
    'pagesize': 100,
    'display': get_default_display(),
    'format': 'smart',
    'allow_select_scan': False,
}


def get_enum_key(key, choices):
    """ Get an enum by prefix or equality """
    if key in choices:
        return key
    keys = [k for k in choices if k.startswith(key)]
    if len(keys) == 1:
        return keys[0]


class DQLClient(cmd.Cmd):

    """
    Interactive commandline interface.

    Attributes
    ----------
    running : bool
        True while session is active, False after quitting
    engine : :class:`dql.engine.FragmentEngine`

    """

    running = False
    conf = None
    engine = None
    formatter = None
    display = None
    session = None
    _conf_dir = None
    _local_endpoint = None

    def initialize(self, region='us-west-1', host=None, port=8000,
                   config_dir=None, session=None):
        """ Set up the repl for execution. """
        # Tab-complete names with a '-' in them
        import readline
        delims = set(readline.get_completer_delims())
        if '-' in delims:
            delims.remove('-')
            readline.set_completer_delims(''.join(delims))

        self._conf_dir = (config_dir or
                          os.path.join(os.environ.get('HOME', '.'), '.config'))
        self.session = session or botocore.session.get_session()
        self.engine = FragmentEngine()
        if host is not None:
            self._local_endpoint = (host, port)
        self.engine.connect(region, session=self.session, host=host, port=port,
                            is_secure=(host is None))

        self.conf = self.load_config()
        for key, value in six.iteritems(DEFAULT_CONFIG):
            self.conf.setdefault(key, value)
        self.display = DISPLAYS[self.conf['display']]
        formatter = self.conf['format']
        self.formatter = FORMATTERS[formatter](
            pagesize=self.conf['pagesize'], width=self.conf['width'])

    def start(self):
        """ Start running the interactive session (blocking) """
        self.running = True
        while self.running:
            self.update_prompt()
            try:
                self.cmdloop()
            except KeyboardInterrupt:
                six.print_()
            except botocore.exceptions.BotoCoreError as e:
                six.print_(e)
            except ParseException as e:
                six.print_(self.engine.pformat_exc(e))
            except:
                traceback.print_exc()
            self.engine.reset()

    def postcmd(self, stop, line):
        self.update_prompt()
        return stop

    def update_prompt(self):
        """ Update the prompt """
        prefix = ''
        if self._local_endpoint is not None:
            prefix += "(%s:%d) " % self._local_endpoint
        prefix += self.engine.region
        if self.engine.partial:
            self.prompt = len(prefix) * ' ' + '> '
        else:
            self.prompt = prefix + '> '

    def do_shell(self, arglist):
        """ Run a shell command """
        proc = subprocess.Popen(shlex.split(arglist),
                                stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT)
        six.print_(proc.communicate()[0])

    def save_config(self):
        """ Save the conf file """
        if not os.path.exists(self._conf_dir):
            os.makedirs(self._conf_dir)
        conf_file = os.path.join(self._conf_dir, 'dql.json')
        with open(conf_file, 'w') as ofile:
            json.dump(self.conf, ofile, indent=2)

    def load_config(self):
        """ Load your configuration settings from a file """
        conf_file = os.path.join(self._conf_dir, 'dql.json')
        if not os.path.exists(conf_file):
            return {}
        with open(conf_file, 'r') as ifile:
            return json.load(ifile)

    @repl_command
    def do_opt(self, *args, **kwargs):
        """ Get and set options """
        args = list(args)
        if not args:
            largest = 0
            for key in self.conf:
                largest = max(largest, len(key))
            for key, value in six.iteritems(self.conf):
                six.print_("%s : %s" % (key.rjust(largest), value))
            return
        option = args.pop(0)
        if not args and not kwargs:
            method = getattr(self, "getopt_" + option, None)
            if method is None:
                self.getopt_default(option)
            else:
                method()
        else:
            method = getattr(self, "opt_" + option, None)
            if method is None:
                six.print_("Unrecognized option %r" % option)
            else:
                method(*args, **kwargs)
                self.save_config()

    def help_opt(self):
        """ Print the help text for options """
        six.print_(OPTIONS)

    def getopt_default(self, option):
        """ Default method to get an option """
        if option not in self.conf:
            six.print_("Unrecognized option %r" % option)
            return
        six.print_("%s: %s" % (option, self.conf[option]))

    def complete_opt(self, text, line, begidx, endidx):
        """ Autocomplete for options """
        tokens = line.split()
        if len(tokens) == 1:
            if text:
                return
            else:
                option = ''
        else:
            option = tokens[1]
        if len(tokens) == 1 or (len(tokens) == 2 and text):
            return [name[4:] + ' ' for name in dir(self)
                    if name.startswith('opt_' + text)]
        method = getattr(self, 'complete_opt_' + option, None)
        if method is not None:
            return method(text, line, begidx, endidx)

    def opt_width(self, width):
        """ Set value for width option """
        self.conf['width'] = width = int(width)
        self.formatter.width = width

    def opt_pagesize(self, pagesize):
        """ Get or set the page size of the query output """
        self.conf['pagesize'] = pagesize = int(pagesize)
        self.formatter.pagesize = pagesize

    def _print_enum_opt(self, option, choices):
        """ Helper for enum options """
        for key in choices:
            if key == self.conf[option]:
                six.print_('* %s' % key)
            else:
                six.print_('  %s' % key)

    def opt_display(self, display):
        """ Set value for display option """
        key = get_enum_key(display, DISPLAYS)
        if key is not None:
            self.conf['display'] = key
            self.display = DISPLAYS[key]
            six.print_("Set display %r" % key)
        else:
            six.print_("Unknown display %r" % display)

    def getopt_display(self):
        """ Get value for display option """
        self._print_enum_opt('display', DISPLAYS)

    def complete_opt_display(self, text, *_):
        """ Autocomplete for display option """
        return [t + ' ' for t in DISPLAYS if t.startswith(text)]

    def opt_format(self, format):
        """ Set value for format option """
        key = get_enum_key(format, FORMATTERS)
        if key is not None:
            self.conf['format'] = key
            self.formatter = FORMATTERS[key](width=self.conf['width'],
                                             pagesize=self.conf['pagesize'])
            six.print_("Set format %r" % key)
        else:
            six.print_("Unknown format %r" % format)

    def getopt_format(self):
        """ Get value for format option """
        self._print_enum_opt('format', FORMATTERS)

    def complete_opt_format(self, text, *_):
        """ Autocomplete for format option """
        return [t + ' ' for t in FORMATTERS if t.startswith(text)]

    def opt_allow_select_scan(self, allow):
        """ Set option allow_select_scan """
        allow = allow.lower() in ('true', 't', 'yes', 'y')
        self.conf['allow_select_scan'] = allow
        self.engine.allow_select_scan = allow

    def complete_opt_allow_select_scan(self, text, *_):
        """ Autocomplete for allow_select_scan option """
        return [t for t in ('true', 'false', 'yes', 'no')
                if t.startswith(text.lower())]

    @repl_command
    def do_watch(self, *args):
        """ Watch Dynamo tables consumed capacity """
        tables = set()
        if not self.engine.cached_descriptions:
            self.engine.describe_all()
        all_tables = list(self.engine.cached_descriptions)
        for arg in args:
            for table in all_tables:
                if fnmatch(table, arg):
                    tables.add(table)
        mon = Monitor(self.engine, tables)
        mon.start()

    def complete_watch(self, text, *_):
        """ Autocomplete for watch """
        return [t + ' ' for t in self.engine.cached_descriptions if
                t.startswith(text)]

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
        return [addslash(f) for f in os.listdir(curpath)
                if f.startswith(text) and isdql(curpath, f)]

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
            sizes = [1 +
                     max([len(str(getattr(t, f))) for t in tables] +
                         [len(title)]) for title, f in six.iteritems(fields)]
            # Print the header
            for size, title in zip(sizes, fields):
                six.print_(title.ljust(size), end='')
            six.print_()
            # Print each table row
            for table in tables:
                for size, field in zip(sizes, fields.values()):
                    six.print_(str(getattr(table, field)).ljust(size), end='')
                six.print_()
        else:
            six.print_(self.engine.describe(table, refresh=True,
                                            metrics=True).pformat())

    def complete_ls(self, text, *_):
        """ Autocomplete for ls """
        return [t + ' ' for t in self.engine.cached_descriptions if
                t.startswith(text)]

    @repl_command
    def do_local(self, host='localhost', port=8000):
        """
        Connect to a local DynamoDB instance. Use 'local off' to disable.

        > local
        > local host=localhost port=8001
        > local off

        """
        port = int(port)
        if host == 'off':
            self._local_endpoint = None
        else:
            self._local_endpoint = (host, port)
        self.onecmd('use %s' % self.engine.region)

    @repl_command
    def do_use(self, region):
        """
        Switch the AWS region

        > use us-west-1
        > use us-east-1

        """
        if self._local_endpoint is not None:
            host, port = self._local_endpoint  # pylint: disable=W0633
            self.engine.connect(region, session=self.session, host=host,
                                port=port, is_secure=False)
        else:
            self.engine.connect(region, session=self.session)

    def complete_use(self, text, *_):
        """ Autocomplete for use """
        return [t + ' ' for t in REGIONS if t.startswith(text)]

    def default(self, command):
        self._run_cmd(command)

    def completedefault(self, text, line, *_):
        """ Autocomplete table names in queries """
        tokens = line.split()
        try:
            before = tokens[-2]
            complete = before.lower() in ('from', 'update', 'table', 'into')
            if tokens[0].lower() == 'dump':
                complete = True
            if complete:
                return [t + ' ' for t in self.engine.cached_descriptions if
                        t.startswith(text)]
        except KeyError:
            pass

    def _run_cmd(self, command):
        """ Run a DQL command """
        results = self.engine.execute(command)
        if results is None:
            pass
        elif isinstance(results, six.string_types):
            six.print_(results)
        else:
            has_more = True
            while has_more:
                with self.display() as ostream:
                    has_more = self.formatter.write(results, ostream)
                if has_more:
                    raw_input("Press return for next %d results:" %
                              self.formatter.pagesize)
        print_count = 0
        total = None
        for (command, capacity) in self.engine.consumed_capacities:
            total += capacity
            six.print_(command)
            six.print_(indent(str(capacity)))
            print_count += 1
        if print_count > 1:
            six.print_('TOTAL')
            six.print_(indent(str(total)))

    @repl_command
    def do_EOF(self):  # pylint: disable=C0103
        """Exit"""
        return self.onecmd('exit')

    @repl_command
    def do_exit(self):
        """Exit"""
        self.running = False
        six.print_()
        return True

    def run_command(self, command):
        """ Run a command passed in from the command line with -c """
        self.display = DISPLAYS['stdout']
        self.conf['pagesize'] = 0
        self.onecmd(command)

    def emptyline(self):
        self.default('')

    def help_help(self):
        """Print the help text for help"""
        six.print_("List commands or print details about a command")

    def help_alter(self):
        """ Print the help text for ALTER """
        six.print_(ALTER)

    def help_analyze(self):
        """ Print the help text for ALTER """
        six.print_(ANALYZE)

    def help_create(self):
        """ Print the help text for CREATE """
        six.print_(CREATE)

    def help_delete(self):
        """ Print the help text for DELETE """
        six.print_(DELETE)

    def help_drop(self):
        """ Print the help text for DROP """
        six.print_(DROP)

    def help_dump(self):
        """ Print the help text for DUMP """
        six.print_(DUMP)

    def help_explain(self):
        """ Print the help text for EXPLAIN """
        six.print_(EXPLAIN)

    def help_insert(self):
        """ Print the help text for INSERT """
        six.print_(INSERT)

    def help_scan(self):
        """ Print the help text for SCAN """
        six.print_(SCAN)

    def help_select(self):
        """ Print the help text for SELECT """
        six.print_(SELECT)

    def help_update(self):
        """ Print the help text for UPDATE """
        six.print_(UPDATE)
