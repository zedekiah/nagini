#!/usr/bin/python
# -*- coding: utf8 -*-
from __future__ import absolute_import, print_function, unicode_literals

import shutil
import subprocess
from optparse import OptionParser
from os import listdir
from os.path import isdir, join

EXECUTIONS_DIR = None


def clear_nagini_data(execution):
    global EXECUTIONS_DIR
    shutil.rmtree(join(EXECUTIONS_DIR, str(execution), 'nagini_data'))


if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option('--executions-dir', type='string', dest='executions_dir')
    parser.add_option('-d', '--delete', type='string', dest='delete',
                      default=None, help="Execution id to delete")
    parser.add_option('-D', '--delete-all', action='store_true', default=False,
                      dest='delete_all', help='Clear all execution data')

    options, args = parser.parse_args()

    if options.executions_dir:
        EXECUTIONS_DIR = options.executions_dir
    else:
        parser.error('You must set --executions-dir option')

    executions = []
    for execution in listdir(options.executions_dir):
        nagini_data = join(options.executions_dir, execution, 'nagini_data')
        if isdir(nagini_data):
            du_output = subprocess.check_output(["du", "-s", nagini_data])
            size = int(du_output.split("\t")[0])
            if size > 0:
                executions.append((execution, size))

    executions.sort(key=lambda i: i[1])

    if not executions:
        print('No nagini data directories found')
        exit(0)

    max_length = max([len(x[0]) for x in executions])

    if options.delete:
        clear_nagini_data(options.delete)
    elif options.delete_all:
        for execution in executions:
            clear_nagini_data(execution[0])
    else:
        for execution in executions:
            print(('{0:<%s} {1}' % (max_length + 4)).format(*execution))
