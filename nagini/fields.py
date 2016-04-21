# -*- coding: utf8 -*-
from nagini.utility import parse_list
import json


class BaseField(object):
    def __init__(self, default=None, require=True, *args, **kwargs):
        self.default = default
        self.require = require

    def to_python(self, value):
        """Return default if value is None otherwise return
        python typed value like int or list.

        :param str value:
        """
        if value is None:
            return self.default
        else:
            return self._to_python(value)

    def _to_python(self, value):
        """Return pythonic typed value

        :param str value: string value
        """
        return value


class StringField(BaseField):
    pass


class UnicodeField(BaseField):
    def __init__(self, default=None, require=True, encoding='utf8'):
        super(UnicodeField, self).__init__(default, require)
        self.encoding = encoding

    def _to_python(self, value):
        return value.decode(self.encoding)


class IntField(BaseField):
    def _to_python(self, value):
        return int(value)


class FloatField(BaseField):
    def _to_python(self, value):
        return float(value)


class ListField(BaseField):
    def __init__(self, default=None, require=True, val_func='auto'):
        super(ListField, self).__init__(default, require)
        self.val_func = val_func

    def _to_python(self, value):
        return parse_list(value, self.val_func)


class JsonField(BaseField):
    def _to_python(self, value):
        return json.loads(value)
