#!/usr/bin/python2
# -*- coding: utf8 -*-
from os.path import join, isdir
from nagini.builder.package import PlainProjectPackage, ProjectPackage
from os import getcwd, listdir
from nagini.client import AzkabanClient
import argparse
import sys
import os


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(dest="root", nargs="?", type=str, default=getcwd(),
                        help="Project root dir")
    parser.add_argument("-p", "--plain", dest="plain", default=False,
                        action="store_true",
                        help="Don't build nagini project files")
    parser.add_argument('-a', '--all', dest='all', default=False,
                        action='store_true',
                        help='If set "root" must be directory of project '
                             'directories')
    parser.add_argument('-H', '--host', dest='host', required=True,
                        help='Host to upload projects')
    parser.add_argument('-u', '--user', dest='user', required=True,
                        help='Username to authenticate on server')
    parser.add_argument('-P', '--password', dest='password', required=True,
                        help='Password to authenticate on server')
    args = parser.parse_args()

    config = {
        'server': {
            'host': args.host,
            'username': args.user,
            'password': args.password
        }
    }

    client = AzkabanClient(config["server"]["host"])
    client.login(config["server"]["username"], config["server"]["password"])

    projects = []
    os.environ['NAGINI_BUILDING'] = 'true'
    print 'Inspecting projects:'

    if args.all:
        items = map(lambda p: join(args.root, p), listdir(args.root))
    else:
        items = [args.root]

    for root_path in items:
        if isdir(root_path):
            if args.plain:
                project = PlainProjectPackage(root_path)
            else:
                project = ProjectPackage(root_path)
            project.build(config=config)
            projects.append(project)

    print "Uploading projects:"
    for item in projects:
        sys.stdout.write("{0:<56}".format(item.name))
        sys.stdout.flush()
        client.upload_project_zip(item.name, item.zip_path)
        sys.stdout.write("{0:>4}\n".format("OK"))
        sys.stdout.flush()
        item.clear()


if __name__ == "__main__":
    main()
