#! /usr/bin/env python
"""
Submit a job to the datasink

Usage:
  $ ds_client -f <realm_config>.yml -k <topic,...> -j <job file>

or, to read the job from stdin:

  $ cat <job file> | ds_client -f <realm_config>.yml -k <topic,...>

"""
import sys
import json
from argparse import ArgumentParser

from datasink.client import JobSource, default_topic

from g2base import ssdlog


def main(options, args):

    logger = ssdlog.make_logger(options.name, options)

    # create JobSource
    datasrc = JobSource(logger, options.name)

    # Configure and connect
    datasrc.read_config(options.configfile)
    datasrc.connect()

    # Read JSON-formatted job file
    if options.jobfile is not None:
        # filename specified on cmd line
        with open(options.jobfile, 'r') as in_f:
            buf = in_f.read()
        job_d = json.loads(buf)

    else:
        # or from stdin if none specified in params
        buf = sys.stdin.read()
        job_d = json.loads(buf)

    # Submit this job to the exchange
    datasrc.submit(job_d, topic=options.topic)

    datasrc.shutdown()


if __name__ == '__main__':

    argprs = ArgumentParser("JobSource sender")

    argprs.add_argument("-f", "--config", dest="configfile",
                        help="Specify the configuration file for this realm")
    argprs.add_argument("-j", "--job", dest="jobfile", default=None,
                        help="Specify the job file to send")
    argprs.add_argument("-t", "--topic", dest="topic", default=default_topic,
                        metavar="TOPIC",
                        help="Dot separated topic")
    argprs.add_argument("--host", dest="realm_host", default='localhost',
                        metavar="HOST",
                        help="Use HOST for this realm")
    argprs.add_argument("-n", "--name", dest="name", default='anonymous',
                        metavar="NAME",
                        help="Specify the NAME for this datasource")
    ssdlog.addlogopts(argprs)

    (options, args) = argprs.parse_known_args(sys.argv[1:])

    if options.configfile is None:
        argprs.error("Please specify a config file with -f")

    if len(args) > 1:
        argprs.error("Unrecognized arguments: {}".format(str(args)))

    main(options, args)
