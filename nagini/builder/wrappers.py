# -*- coding: utf8 -*-
from nagini.loader import load_module, path_to_import
from nagini.flow import BaseFlow, EmbeddedFlow
from templates import render_template
from nagini.utility import flatten
from nagini.job import BaseJob
from os.path import join
import inspect


class JobWrapper(object):
    source_file = None  # full path to source file
    root_path = None  # project root path
    dependencies = None
    class_obj = None
    flow = None  # main flow
    _job_type = "command"

    def __init__(self, class_obj, project):
        self.class_obj = class_obj
        self.name = class_obj.name or class_obj.__name__
        self.module = inspect.getmodule(class_obj)
        self.source_file = inspect.getsourcefile(self.module)
        self.project = project
        if self.source_file.startswith(project.base_dir):
            self.source_file = self.source_file.replace(project.base_dir, "")
            self.source_file = self.source_file.strip("/")
        self.import_path = path_to_import(self.source_file)
        self.dependencies = []
        self.flow = self
        # print "Init job wrapper for", self.class_obj

    def build_dependencies(self):
        try:
            job = self.class_obj()
        except TypeError:
            # print "Error in class", self.class_obj
            return
        for require in flatten(job.requires()):
            if isinstance(require, EmbeddedFlow):
                wrapper = EmbeddedFlowWrapper(require.__class__, self.project)
            elif isinstance(require, BaseFlow):
                wrapper = EmbeddedFlowWrapper(require.__class__, self.project)
                wrapper._build_deps = False
            elif isinstance(require, BaseJob):
                wrapper = JobWrapper(require.__class__, self.project)
            else:
                continue
            wrapper.flow = self.flow
            wrapper.build()
            self.dependencies.append(wrapper)

    def build(self):
        self.build_dependencies()
        self._make_launcher_file()
        self._make_job_file()

    def _job_filename(self):
        return self.name + ".job"

    def _job_name(self):
        return self.name

    def _make_job_file(self):
        content = render_template(
            filename="job-template.job.j2",
            type=self._job_type,
            dependencies=[d._job_name() for d in self.dependencies],
            name=self.name,
            retries=getattr(self.class_obj, "retries", 0),
            retry_backoff=getattr(self.class_obj, "retry_backoff", 0)
        )
        filename = join(self.project.base_dir, self._job_filename())
        with open(filename, "w") as fd:
            fd.write(content)

    def _make_launcher_file(self):
        content = render_template(
            filename="launcher.py.j2",
            import_path=self.import_path,
            name=self.name
        )
        filename = join(self.project.base_dir, self.name + "Launcher.py")
        with open(filename, "w") as fd:
            fd.write(content)


class FlowWrapper(JobWrapper):
    def build(self):
        self.build_dependencies()
        if self.dependencies:
            self._make_launcher_file()
            self._make_job_file()


class EmbeddedFlowWrapper(JobWrapper):
    _build_deps = True
    _job_type = "flow"

    def __init__(self, class_obj, project):
        JobWrapper.__init__(self, class_obj, project)
        if issubclass(self.class_obj, EmbeddedFlow):
            job = self.class_obj()
            self.name = class_obj.name or job.source_flow().__class__.__name__

    def build_dependencies(self):
        if self._build_deps:
            JobWrapper.build_dependencies(self)

    def _job_filename(self):
        return "%s.%s.job" % (self.flow.name, self.name)

    def _job_name(self):
        return "%s.%s" % (self.flow.name, self.name)
