#! /usr/bin/env python3
"""
Submit a job as a job source.

See tutorial document.

Usage:
  $ ds_pub.py -n NAME -f PUB_CFG -t TOPIC -j JOB_FILE --loglevel=20 --stderr

or, to read the job from stdin:

  $ cat JOB_FILE | ds_pub.py -n NAME -f PUB_CFG -t TOPIC --loglevel=20 --stderr

Where:
  - NAME is the name of the publisher (not so important, but good for
    debugging or the job sink knowing who sent this)

  - PUB_CFG is the job source configuration YAML file

  - TOPIC is a dotted topic (e.g. "foo", "foo.bar", "foo.bar.baz", etc)

  - JOB_FILE is a JSON-formatted job file

Example:
  $ ds_pub.py -n Publisher1 -f pub.yml -t foo -j job.json --loglevel=20 --stderr

Repeat as needed with different job files, topics.
"""
import sys
import json
from argparse import ArgumentParser

from datasink.client import JobSource, default_topic
from datasink import log


def main(options, args):

    logger = log.make_logger(options.name, options)

    # create JobSource
    jobsrc = JobSource(logger, options.name)

    # Configure and connect
    jobsrc.read_config(options.configfile)
    jobsrc.connect()

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
    jobsrc.submit(job_d, topic=options.topic)

    jobsrc.shutdown()


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
    log.addlogopts(argprs)

    (options, args) = argprs.parse_known_args(sys.argv[1:])

    if options.configfile is None:
        argprs.error("Please specify a config file with -f")

    if len(args) > 1:
        argprs.error("Unrecognized arguments: {}".format(str(args)))

    main(options, args)
