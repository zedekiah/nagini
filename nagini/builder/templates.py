# -*- coding: utf8 -*-
from os.path import dirname, abspath, join, exists
from jinja2 import Template

if exists(abspath(join(dirname(__file__), "../../data"))):
    _templates_dir = abspath(join(dirname(__file__), "../../data"))
else:
    _templates_dir = "/usr/share/nagini"


def render_template(filename, **kwargs):
    with open(join(_templates_dir, filename)) as fd:
        template = Template(fd.read())
    return template.render(**kwargs)
