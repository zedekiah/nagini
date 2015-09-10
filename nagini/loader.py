# -*- coding: utf8 -*-
from os.path import basename


def remove_ext(string, ext=None):
    if ext:
        if not ext.startswith("."):
            ext = "." + ext
        if string.endswith(ext):
            return string[:-len(ext)]
        else:
            return string
    else:
        return string.rsplit(".", 1)[0]


def path_to_import(path):
    return remove_ext(path, "py").replace("/", ".").strip(".")


def load_module(module_path):
    module_name = remove_ext(basename(module_path), "py")
    return __import__(path_to_import(module_path), fromlist=[module_name])
