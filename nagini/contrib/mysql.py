# -*- coding: utf-8; -*-
from __future__ import absolute_import, division, print_function, unicode_literals

from nagini import BaseJob
import subprocess


class MySqlQueryJob(BaseJob):
    """You can set sql in subclass or override run method and use
    `query` method
    """
    host = None
    port = None
    user = None
    password = None
    config_file = None

    db = None
    table = None
    fields = None
    sql = None
    sort_by = None  # works only with fields

    def run(self):
        sql = 'SELECT {fields} FROM {table}'.format(
            fields=('`%s`' % '`, `'.join(self.fields)) if self.fields else '*',
            table=self.table
        )

        self.query(self.sql or sql)

    def _get_base_args(self):
        args = ['mysql']

        if self.config_file:
            args.append('--defaults-extra-file=%s' % self.config_file)
        else:
            args += ['--host=%s' % self.host, '--port=%s' % str(self.port),
                     '--user=%s' % self.user, '--password=%s' % self.password]

        if self.db:
            args.append('--database=%s' % self.db)

        return args

    def query(self, sql):
        args = self._get_base_args()
        args += ['--quick', '--default-character-set=utf8', '--silent',
                 '--column-names', '--skip-pager', '-e', sql]

        if self.output():
            with open(self.output().path, "wb") as fd:
                if self.sort_by is not None:
                    if isinstance(self.sort_by, int):
                        field_n = self.sort_by
                    else:
                        field_n = self.fields.index(self.sort_by) + 1
                    qp = subprocess.Popen(args, stdout=subprocess.PIPE)
                    sort_cmd = ['sort', '-k{0},{0}'.format(field_n),
                                '-t', '\t']
                    sort = subprocess.Popen(sort_cmd,
                                            stdin=qp.stdout, stdout=fd,
                                            env={'LC_ALL': 'C', 'LANG': 'C'})
                    if qp.wait():
                        raise subprocess.CalledProcessError(qp.returncode,
                                                            args)
                    if sort.wait():
                        raise subprocess.CalledProcessError(sort.returncode,
                                                            sort_cmd)
                else:
                    subprocess.check_call(args, stdout=fd.fileno())
        else:
            subprocess.check_call(args)


class UploadToMySqlJob(MySqlQueryJob):
    """
    Input must be LocalTarget
    Output must be MySqlTarget
    """
    host = None
    port = None
    user = None
    password = None
    config_file = None

    db = None
    table = None
    fields = None
    clear = False

    def run(self):
        sql = '''
        SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE;
        SET autocommit=0;
        START TRANSACTION;
        {delete}
        LOAD DATA LOCAL INFILE '{filename}'
        IGNORE INTO TABLE {table} {fields};
        SHOW WARNINGS;
        COMMIT;'''.format(
            filename=self.input().path,
            table=self.table,
            fields=('(`%s`)' % "`,`".join(self.fields)) if self.fields else '',
            delete='DELETE FROM `%s`;' % self.table if self.clear else ''
        )

        args = self._get_base_args()
        args += ['--default-character-set=utf8', '--local-infile=1',
                 '--silent', '-q', '-e', sql]

        subprocess.check_call(args)
