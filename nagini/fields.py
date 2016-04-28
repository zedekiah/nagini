# -*- coding: utf8 -*-
from nagini.utility import parse_list
from datetime import datetime
import json
import re


class BaseField(object):
    name = None

    def __init__(self, default=None, require=True, prop_name=None,
                 *args, **kwargs):
        self.default = default
        self.require = require
        self.name = prop_name

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


class RegexpField(BaseField):
    def __init__(self, regexp, default=None, require=True, prop_name=None):
        super(RegexpField, self).__init__(default, require, prop_name)
        self.regexp = regexp

    def _to_python(self, value):
        if not re.match(self.regexp, value):
            raise ValueError(
                'Value "%s" not match pattern "%s" for property "%s"' %
                (value, self.regexp, self.name)
            )
        return value


class DateField(BaseField):
    def __init__(self, fmt='%Y-%m-%d', default=None, require=True,
                 prop_name=None):
        super(DateField, self).__init__(default, require, prop_name)
        self.fmt = fmt

    def _to_python(self, value):
        return datetime.strptime(self.fmt, value).date()


class DateTimeField(BaseField):
    def __init__(self, fmt='%Y-%m-%d %H:%M:%S', default=None, require=True,
                 prop_name=None):
        super(DateTimeField, self).__init__(default, require, prop_name)
        self.fmt = fmt

    def _to_python(self, value):
        return datetime.strptime(self.fmt, value)


class StringMonthField(RegexpField):
    def __init__(self, regexp=r'^20\d{2}-(0?[1-9]|1[012])$',
                 default=None, require=True, prop_name=None):
        super(StringMonthField, self).__init__(regexp=regexp, default=default,
                                               require=require,
                                               prop_name=prop_name)


class UnicodeField(BaseField):
    def __init__(self, default=None, require=True, encoding='utf8',
                 prop_name=None):
        super(UnicodeField, self).__init__(default, require, prop_name)
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
    def __init__(self, default=None, require=True, prop_name=None,
                 val_func='auto'):
        super(ListField, self).__init__(default, require, prop_name)
        self.val_func = val_func

    def _to_python(self, value):
        return parse_list(value, self.val_func)


class JsonField(BaseField):
    def _to_python(self, value):
        return json.loads(value)
