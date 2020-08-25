#! /usr/bin/env python
"""
  $ ds_file path/to/file

"""
import sys
import os
import socket
import getpass
import json


def main(options, args):

    path = os.path.abspath(args[0])
    statbuf = os.stat(path)

    d = dict(action='transfer', srcpath=path, username=getpass.getuser(),
             host=socket.getfqdn(), transfermethod='scp', size=statbuf.st_size)

    print(json.dumps(d))

if __name__ == '__main__':

    # Parse command line options with nifty new optparse module
    from argparse import ArgumentParser

    argprs = ArgumentParser("JobSource sender")

    (options, args) = argprs.parse_known_args(sys.argv[1:])

    if len(args) > 1:
        argprs.error("Unrecognized arguments: {}".format(str(args)))

    main(options, args)
