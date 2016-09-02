# -*- coding: utf8 -*-
from nagini import BaseJob
from nagini.properties import props
from nagini.fields import (StringField, IntField, FloatField, RegexpField,
                           JsonField, DateField, DateTimeField,
                           StringMonthField, ListField, UnicodeField,
                           BooleanField)
import unittest
from tempfile import NamedTemporaryFile
from contextlib import contextmanager
from datetime import date, datetime
from textwrap import dedent


@contextmanager
def make_test_file():
    with NamedTemporaryFile() as fd:
        fd.write(dedent('''\
        string_field=some text
        int_field=123
        float_field=1.33
        regexp_field=some string
        json_field={"var"\: 1}
        date_field=2015-01-23
        datetime_field=2015-01-23 17\:02\:00
        str_month_field=2015-01
        list_field=1,2,3,4
        unicode_field=привет
        boolean_field=yes'''))
        fd.file.flush()
        yield fd.name


@contextmanager
def load_test_env(field_class, *args, **kwargs):
    """

    :param field_class: fields class
    :param args: field args
    """
    with make_test_file() as filename:
        props.load(filename)
        job_class = make_job(field_class, *args, **kwargs)
        yield job_class()


def make_job(test_field_class, *args, **kwargs):
    class TestJob(BaseJob):
        test_field = test_field_class(*args, **kwargs)

    return TestJob


class FieldsTest(unittest.TestCase):
    def test_string_field(self):
        with load_test_env(StringField, prop_name='string_field') as job:
            self.assertEqual(job.test_field, 'some text')

    def test_default_value_field(self):
        with load_test_env(StringField, default='default') as job:
            self.assertEqual(job.test_field, 'default')

    def test_int_field(self):
        with load_test_env(IntField, prop_name='int_field') as job:
            self.assertEqual(job.test_field, 123)

    def test_float_field(self):
        with load_test_env(FloatField, prop_name='float_field') as job:
            self.assertEqual(job.test_field, 1.33)

    def test_regexp_field(self):
        re = 'some \w+'
        with load_test_env(RegexpField, re, prop_name='regexp_field') as job:
            self.assertEqual(job.test_field, 'some string')

    def test_json_field(self):
        with load_test_env(JsonField, prop_name='json_field') as job:
            self.assertEqual(job.test_field, {'var': 1})

    def test_date_field(self):
        with load_test_env(DateField, prop_name='date_field') as job:
            self.assertEqual(job.test_field, date(2015, 1, 23))

    def test_datetime_field(self):
        with load_test_env(DateTimeField, prop_name='datetime_field') as job:
            self.assertEqual(job.test_field, datetime(2015, 1, 23, 17, 2, 0))

    def test_str_month_field(self):
        name = 'str_month_field'
        with load_test_env(StringMonthField, prop_name=name) as job:
            self.assertEqual(job.test_field, '2015-01')

    def test_list_field(self):
        with load_test_env(ListField, prop_name='list_field') as job:
            self.assertEqual(job.test_field, [1, 2, 3, 4])

    def test_unicode_field(self):
        with load_test_env(UnicodeField, prop_name='unicode_field') as job:
            self.assertEqual(job.test_field, u'привет')

    def test_boolean_field(self):
        with load_test_env(BooleanField, prop_name='boolean_field') as job:
            self.assertEqual(job.test_field, True)
