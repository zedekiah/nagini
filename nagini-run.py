#!/usr/bin/python2
# -*- coding: utf8 -*-
# Script that run remote flows
from nagini.utility import flatten
from optparse import OptionParser
from tempfile import mkstemp
import json
import sys
import os


def fill_queue(job, queue=None):
    if queue is None:
        queue = []

    queue.append(job)
    requires = flatten(job.requires())
    if requires:
        for parent in requires:
            fill_queue(parent, queue)

    return reversed(queue)


def output_to_input_props(path):
    with open(path) as fd:
        content = json.load(fd)

    with open(path, "w") as fd:
        fd.write("")
        for name, value in content.iteritems():
            fd.write("{0}={1}\n".format(name, value))


if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option("--pythonpath", type="string",
                      default=None, dest="pythonpath")

    options, args = parser.parse_args()

    if options.pythonpath:
        sys.path.insert(0, options.pythonpath)

    module_name, root_job_name = args[0].split(":")

    module = __import__(module_name, fromlist=[root_job_name])
    root_job = getattr(module, root_job_name)()

    jobs_queue = fill_queue(root_job)
    print "Job queue:", jobs_queue

    # with TemporaryFile(mode="r")
    _, input_prop_file = mkstemp(prefix="nagini-run-props-")
    for job in jobs_queue:
        _, output_prop_file = mkstemp(prefix="nagini-run-props-")
        os.environ["JOB_PROP_FILE"] = input_prop_file
        os.environ["JOB_OUTPUT_PROP_FILE"] = output_prop_file
        print "JOB_PROP_FILE", os.environ["JOB_PROP_FILE"]
        print "JOB_OUTPUT_PROP_FILE", os.environ["JOB_OUTPUT_PROP_FILE"]
        print "Launcher: job", job
        print "Launcher: about to job.execute()"

        job.execute()

        output_to_input_props(output_prop_file)
        os.remove(input_prop_file)
        input_prop_file = output_prop_file
