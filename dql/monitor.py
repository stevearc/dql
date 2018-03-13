""" Utilities for monitoring the consumed capacity of tables """
from __future__ import print_function
from future.utils import iteritems

import time

from datetime import datetime

from .util import getmaxyx


try:
    import curses
    CURSES_SUPPORTED = True
except ImportError:
    CURSES_SUPPORTED = False


class Monitor(object):
    """ Tool for monitoring the consumed capacity of many tables """

    def __init__(self, engine, tables):
        self.engine = engine
        self.win = None
        self._tables = tables
        self._refresh_rate = 30
        self._max_width = 80

    def start(self):
        """ Start the monitor """
        if CURSES_SUPPORTED:
            curses.wrapper(self.run)
        else:
            print("Your system does not have curses installed. "
                  "Cannot use 'watch'")

    def run(self, stdscr):
        """ Initialize curses and refresh in a loop """
        self.win = stdscr
        curses.curs_set(0)
        stdscr.timeout(0)
        curses.init_pair(1, curses.COLOR_CYAN, curses.COLOR_BLACK)
        curses.init_pair(2, curses.COLOR_GREEN, curses.COLOR_BLACK)
        curses.init_pair(3, curses.COLOR_YELLOW, curses.COLOR_BLACK)
        curses.init_pair(4, curses.COLOR_RED, curses.COLOR_BLACK)
        while True:
            self.refresh(True)
            now = time.time()
            while time.time() - now < self._refresh_rate:
                time.sleep(0.1)
                self.refresh(False)

    def _calc_min_width(self, table):
        """ Calculate the minimum allowable width for a table """
        width = len(table.name)
        cap = table.consumed_capacity['__table__']
        width = max(width, 4 + len("%.1f/%d" % (cap['read'],
                                                table.read_throughput)))
        width = max(width, 4 + len("%.1f/%d" % (cap['write'],
                                                table.write_throughput)))
        for index_name, cap in iteritems(table.consumed_capacity):
            if index_name == '__table__':
                continue
            index = table.global_indexes[index_name]
            width = max(width, 4 + len(index_name + "%.1f/%d" %
                                       (cap['read'], index.read_throughput)))
            width = max(width, 4 + len(index_name + "%.1f/%d" %
                                       (cap['write'], index.write_throughput)))
        return width

    def _progress_bar(self, width, percent, left='', right='', fill='|'):
        """ Get the green/yellow/red pieces of a text + bar display """
        text = left + (width - len(left) - len(right)) * ' ' + right
        cutoff = int(round(percent * width))
        text = text[:cutoff].replace(' ', fill) + text[cutoff:]
        low_cutoff = int(round(0.7 * width))
        med_cutoff = int(round(0.9 * width))
        if percent < 0.7:
            yield 2, text[:cutoff]
            yield 0, text[cutoff:]
        elif percent < 0.9:
            yield 2, text[:low_cutoff]
            yield 3, text[low_cutoff:cutoff]
            yield 0, text[cutoff:]
        else:
            yield 2, text[:low_cutoff]
            yield 3, text[low_cutoff:med_cutoff]
            yield 4, text[med_cutoff:cutoff]
            yield 0, text[cutoff:]

    def _add_throughput(self, y, x, width, op, title, available, used):
        """ Write a single throughput measure to a row """
        percent = float(used) / available
        self.win.addstr(y, x, '[')
        # Because we have disabled scrolling, writing the lower right corner
        # character in a terminal can throw an error (this is inside the curses
        # implementation). If that happens (and it will only ever happen here),
        # we should just catch it and continue.
        try:
            self.win.addstr(y, x + width - 1, ']')
        except curses.error:
            pass
        x += 1
        right = "%.1f/%d:%s" % (used, available, op)
        pieces = self._progress_bar(width - 2, percent, title, right)
        for color, text in pieces:
            self.win.addstr(y, x, text, curses.color_pair(color))
            x += len(text)

    def refresh(self, fetch_data):
        """ Redraw the display """
        self.win.erase()
        height, width = getmaxyx()
        if curses.is_term_resized(height, width):
            self.win.clear()
            curses.resizeterm(height, width)
        y = 1  # Starts at 1 because of date string
        x = 0
        columns = []
        column = []
        for table in self._tables:
            desc = self.engine.describe(table, fetch_data, True)
            line_count = 1 + 2 * len(desc.consumed_capacity)
            if (column or columns) and line_count + y > height:
                columns.append(column)
                column = []
                y = 1
            y += line_count
            column.append(desc)

        columns.append(column)
        y = 1

        # Calculate the min width of each column
        column_widths = []
        for column in columns:
            column_widths.append(max(map(self._calc_min_width, column)))
        # Find how many columns we can support
        while len(columns) > 1 and \
                sum(column_widths) > width - len(columns) + 1:
            columns.pop()
            column_widths.pop()
        effective_width = width - len(columns) + 1
        # Iteratively expand columns until we fit the width or they are all max
        while sum(column_widths) < effective_width and \
                any((w < self._max_width for w in column_widths)):
            smallest = min(column_widths)
            i = column_widths.index(smallest)
            column_widths[i] += 1

        status = datetime.now().strftime('%H:%M:%S')
        status += " %d tables" % len(self._tables)
        num_displayed = sum(map(len, columns))
        if num_displayed < len(self._tables):
            status += " (%d visible)" % num_displayed
        self.win.addstr(0, 0, status[:width])

        for column, col_width in zip(columns, column_widths):
            for table in column:
                cap = table.consumed_capacity['__table__']
                self.win.addstr(y, x, table.name, curses.color_pair(1))
                self._add_throughput(y + 1, x, col_width, 'R', '',
                                     table.read_throughput, cap['read'])
                self._add_throughput(y + 2, x, col_width, 'W', '',
                                     table.write_throughput, cap['write'])
                y += 3
                for index_name, cap in iteritems(table.consumed_capacity):
                    if index_name == '__table__':
                        continue
                    index = table.global_indexes[index_name]
                    self._add_throughput(y, x, col_width, 'R', index_name,
                                         index.read_throughput, cap['read'])
                    self._add_throughput(y + 1, x, col_width, 'W', index_name,
                                         index.write_throughput, cap['write'])
                    y += 2
            x += col_width + 1
            y = 1

        self.win.refresh()
