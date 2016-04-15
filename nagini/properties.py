# -*- coding: utf8 -*-
import json
import os
import re


class Properties(dict):
    FLOAT_RE = r'^\d+\.\d*$'

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

    def get_list(self, name, val_func='auto'):
        """Parse value with key=name and transform it to list.
        If val_vun

        :param str name:
        :param func val_func:
        :return:
        """
        value = self.get(name, '').strip().strip(',')
        if value:
            value_list = [v.strip() for v in value.split(',')]
            if val_func == 'auto':
                return [Properties._parse_value(v) for v in value_list]
            elif val_func:
                return [val_func(v) for v in value_list]
            else:
                return value_list
        else:
            return []

    @staticmethod
    def _parse_value(val):
        """Return proper typed value

        :param str val: value as a string
        :rtype: str|unicode|int|float|long
        """
        if re.match(Properties.FLOAT_RE, val):
            return float(val)
        elif val.isdigit():  # auto int and long
            return int(val)
        else:
            try:
                return val.decode('utf8')
            except UnicodeDecodeError:
                return val


def load_properties():
    props = Properties()
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
