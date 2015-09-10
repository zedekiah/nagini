# -*- coding: utf8 -*-
import json
import os


class Properties(dict):
    def load(self):
        if "JOB_PROP_FILE" in os.environ:
            with open(os.environ["JOB_PROP_FILE"]) as fd:
                for line in fd:
                    line = line.strip()
                    if not line.startswith("#"):
                        name, value = line.split("=", 1)
                        dict.__setitem__(self, name, value)
        else:
            print "No JOB_PROP_FILE"

    def dump(self):
        if "JOB_OUTPUT_PROP_FILE" in os.environ:
            with open(os.environ["JOB_OUTPUT_PROP_FILE"], "w") as fd:
                # json.dump(self.output_props, fd)
                json.dump(self, fd)
        else:
            print "No JOB_OUTPUT_PROP_FILE"


def load_properties():
    props = {}
    if "JOB_PROP_FILE" in os.environ:
        with open(os.environ["JOB_PROP_FILE"]) as fd:
            for line in fd:
                line = line.strip()
                if not line.startswith("#"):
                    name, value = line.split("=", 1)
                    props[name] = value.replace(r"\:", ":")
    # else:
    #     print "No JOB_PROP_FILE"
    return props


def save_properties(props):
    if "JOB_OUTPUT_PROP_FILE" in os.environ:
        with open(os.environ["JOB_OUTPUT_PROP_FILE"], "w") as fd:
            json.dump(props, fd)
    else:
        print "No JOB_OUTPUT_PROP_FILE"
