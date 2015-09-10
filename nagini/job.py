# -*- coding: utf8 -*-
from nagini.properties import load_properties, save_properties
from nagini.utility import flatten
from os.path import join, exists
from os import mkdir, environ
import subprocess
import shutil
import json


class BaseJob(object):
    _check_output_at_start = True
    _check_output_at_end = True
    name = None
    retries = 0
    retry_backoff = 0

    def __init__(self):
        self.props = load_properties()
        self.env = environ.copy()

    def requires(self):
        """Override me!"""
        return []

    def output(self):
        """Override me!"""
        return []

    def input(self):
        """Return outputs of requires

        :rtype: Target|list[Target]
        """
        requires = self.requires()
        if isinstance(requires, (tuple, list, set)):
            return [r.output() for r in requires]
        else:
            return requires.output()

    def is_complete(self):
        return False

    def run(self):
        """Override me!"""
        print "Nagini: this job is empty. Override run() method"

    def on_failure(self):
        for target in flatten(self.output()):
            target.clean_up()

    def on_success(self):
        pass

    def execute(self):
        self.props = load_properties()
        self.props["working.dir.nagini"] = join(self.props["working.dir"],
                                                "nagini_data")
        if not exists(self.props["working.dir.nagini"]):
            mkdir(self.props["working.dir.nagini"])
        print "Init props:"
        print json.dumps(self.props, ensure_ascii=False, indent=4)
        output = flatten(self.output())
        if not output:
            self._check_output_at_start = False
        try:
            print "Nagini: start job"
            if self._check_output_at_start and all(t.exists() for t in output):
                print "All targets exists at start of the job, skip job..."
            else:
                print "Nagini: about to run"
                self.run()
            for key, value in self.env.iteritems():
                self.props["env.%s" % key] = value
            save_properties(self.props)
            if self._check_output_at_end and not all(t.exists() for t in output):
                raise Exception(
                    "Not all output target exists at end of the job"
                )
            else:
                self.on_success()
        except BaseException:
            print "NaginiJob: catch exception. Try on_failure()"
            self.on_failure()
            raise

    def rupdate_props(self, props):
        """Like self.props.update but not override existing props"""
        props.update(self.props)
        self.props = props

    def data_path(self, path=""):
        return join(self.props["working.dir.nagini"], path)

    def clear_data_dir(self):
        if exists(self.props["working.dir.nagini"]):
            shutil.rmtree(self.props["working.dir.nagini"])


class UploadToMySqlJob(BaseJob):
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
        sql = """
        SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE;
        SET autocommit=0;
        START TRANSACTION;
        {delete}
        LOAD DATA LOCAL INFILE '{filename}'
        IGNORE INTO TABLE {table} {fields};
        SHOW WARNINGS;
        COMMIT;""".format(
            filename=self.input().path,
            table=self.table,
            fields=("(`%s`)" % "`,`".join(self.fields)) if self.fields else "",
            delete="DELETE FROM `%s`;" % self.table if self.clear else ""
        )

        args = ["mysql"]
        if self.config_file:
            args.append("--defaults-extra-file=%s" % self.config_file)
        else:
            args += ["--host=%s" % self.host, "--port=%s" % str(self.port),
                     "--user=%s" % self.user, "--password=%s" % self.password]

        if self.db:
            args.append("--database=%s" % self.db)

        args += ["--default-character-set=utf8", "--local-infile=1",
                 "--silent", "-q", "-e", sql]

        subprocess.check_call(args)


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

    def run(self):
        sql = "SELECT {fields} FROM {table}".format(
            fields=("`%s`" % "`, `".join(self.fields)) if self.fields else "*",
            table=self.table
        )

        self.query(self.sql or sql)

    def query(self, sql):
        args = ["mysql"]
        if self.config_file:
            args.append("--defaults-extra-file=%s" % self.config_file)
        else:
            args += ["--host=%s" % self.host, "--port=%s" % str(self.port),
                     "--user=%s" % self.user, "--password=%s" % self.password]

        if self.db:
            args.append("--database=%s" % self.db)

        args += ["--quick", "--default-character-set=utf8", "--silent",
                 "--column-names", "--skip-pager", "-e", sql]

        if self.output():
            with open(self.output().path, "wb") as fd:
                subprocess.check_call(args, stdout=fd.fileno())
        else:
            subprocess.check_call(args)


class ClearAllJob(BaseJob):
    def run(self):
        self.clear_data_dir()
