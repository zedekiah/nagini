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
        month_list_field=2015-01,2015-04
        unicode_field=привет
        boolean_field=yes'''))
        fd.file.flush()
        yield fd.name


@contextmanager
def load_test_env(field):
    """

    :param field: fields instance
    """
    with make_test_file() as filename:
        props.load(filename)
        job_class = make_job(field)
        yield job_class()


def make_job(field):
    class TestJob(BaseJob):
        test_field = field

    return TestJob


class FieldsTest(unittest.TestCase):
    def test_string_field(self):
        with load_test_env(StringField(prop_name='string_field')) as job:
            self.assertEqual(job.test_field, 'some text')
            job.test_field = 'nagini'
            self.assertEqual(props['string_field'], 'nagini')

    def test_default_value_field(self):
        with load_test_env(StringField(default='default')) as job:
            self.assertEqual(job.test_field, 'default')
            self.assertEqual(props['test_field'], 'default')

    def test_int_field(self):
        with load_test_env(IntField(prop_name='int_field')) as job:
            self.assertEqual(job.test_field, 123)
            job.test_field = 456
            self.assertEqual(props['int_field'], '456')

    def test_float_field(self):
        with load_test_env(FloatField(prop_name='float_field')) as job:
            self.assertEqual(job.test_field, 1.33)
            job.test_field = 17.02
            self.assertEqual(props['float_field'], '17.02')

    def test_regexp_field(self):
        re = 'some \w+'
        with load_test_env(RegexpField(re, prop_name='regexp_field')) as job:
            self.assertEqual(job.test_field, 'some string')
            job.test_field = 'another string'
            self.assertEqual(props['regexp_field'], 'another string')

    def test_json_field(self):
        with load_test_env(JsonField(prop_name='json_field')) as job:
            self.assertEqual(job.test_field, {'var': 1})
            job.test_field = [{'another_var': 17}]
            self.assertEqual(props['json_field'], '[{"another_var": 17}]')

    def test_date_field(self):
        with load_test_env(DateField(prop_name='date_field')) as job:
            self.assertEqual(job.test_field, date(2015, 1, 23))
            job.test_field = date(2012, 11, 4)
            self.assertEqual(props['date_field'], '2012-11-04')

    def test_datetime_field(self):
        with load_test_env(DateTimeField(prop_name='datetime_field')) as job:
            self.assertEqual(job.test_field, datetime(2015, 1, 23, 17, 2, 0))
            job.test_field = datetime(2016, 12, 18, 14, 44, 2)
            self.assertEqual(props['datetime_field'], '2016-12-18 14:44:02')

    def test_str_month_field(self):
        name = 'str_month_field'
        with load_test_env(StringMonthField(prop_name=name)) as job:
            self.assertEqual(job.test_field, '2015-01')

    def test_list_field(self):
        with load_test_env(ListField(prop_name='list_field')) as job:
            self.assertEqual(job.test_field, [1, 2, 3, 4])
            job.test_field = [10, 15]
            self.assertEqual(props['list_field'], '10,15')

    def test_typed_list_field(self):
        field = ListField(prop_name='month_list_field',
                          value_type=DateField('%Y-%m'))
        with load_test_env(field) as job:
            self.assertEqual(job.test_field,
                             [date(2015, 1, 1), date(2015, 4, 1)])
            job.test_field = [date(2016, 1, 1), date(2016, 4, 1)]
            self.assertEqual(props['month_list_field'], '2016-01,2016-04')

    def test_unicode_field(self):
        with load_test_env(UnicodeField(prop_name='unicode_field')) as job:
            self.assertEqual(job.test_field, u'привет')
            job.test_field = u'нагайна'
            self.assertEqual(props['unicode_field'], 'нагайна')

    def test_boolean_field(self):
        with load_test_env(BooleanField(prop_name='boolean_field')) as job:
            self.assertEqual(job.test_field, True)
            job.test_field = False
            self.assertEqual(props['boolean_field'], 'False')

    def test_deep_inheritance_field_check(self):
        class Base(BaseJob):
            string_field = StringField()

        class Derived(Base):
            pass

        with make_test_file() as filename:
            props.load(filename)
            job = Derived()

            self.assertEqual(job.string_field, 'some text')

    def test_deep_inheritance_default_field_check(self):
        class Base(BaseJob):
            test_field = StringField(default='default_value')

        class Derived(Base):
            pass

        with make_test_file() as filename:
            props.load(filename)
            job = Derived()
            self.assertEqual(props['test_field'], 'default_value')
            self.assertEqual(job.test_field, 'default_value')

    def test_missing_value_exception(self):
        class Job(BaseJob):
            missing_field = StringField()

        with make_test_file() as filename:
            props.load(filename)

            self.assertRaises(KeyError, Job)

    def test_clearing_props_on_load(self):
        with make_test_file() as filename:
            props.load(filename)
            props['key_that_must_disappear'] = 'value'
            props.load(filename)
            self.assertNotIn('key_that_must_disappear', props)

            props['key_that_must_disappear'] = 'value'
            props.load(filename, clear=False)
            self.assertIn('key_that_must_disappear', props)


if __name__ == '__main__':
    unittest.main()
