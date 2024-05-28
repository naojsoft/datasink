#!/usr/bin/env python
"""
   Usage:
     $ ds_worker -f <sinkconfig>.yml [log options] queuename ...

realm: other
realm_host: localhost
realm_username: 'guest'
realm_password: 'guest'
num_workers: 2

"""

import sys
import os
import threading
from argparse import ArgumentParser

from datasink.initialize import default_topic
from datasink.worker import JobSink
from datasink import log


def main(options, args):
    name = options.name
    # this processor's name
    if name is None:
        name = "worker-{}".format(os.getpid())

    logger = log.make_logger(name, options)

    jobsink = JobSink(logger, name)
    jobsink.read_config(options.configfile)

    ev_quit = threading.Event()

    jobsink.start_workers(ev_quit=ev_quit)
    jobsink.serve(ev_quit=ev_quit, topic=options.topic)


if __name__ == '__main__':

    argprs = ArgumentParser("job worker")

    argprs.add_argument("-f", "--config", dest="configfile",
                        help="Specify the configuration file for this realm")
    argprs.add_argument("-n", "--name", dest="name", default=None,
                        metavar="NAME",
                        help="Specify the NAME for this worker")
    argprs.add_argument("-t", "--topic", dest="topic", default=default_topic,
                        metavar="TOPIC",
                        help="Dot separated topic")
    log.addlogopts(argprs)

    (options, args) = argprs.parse_known_args(sys.argv[1:])

    if options.configfile is None:
        argprs.error("Please specify a config file with -f")

    # if len(args) == 0:
    #     argprs.error("You need to specify queues on command line")

    main(options, args)
