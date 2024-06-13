#!/usr/bin/env python3
"""
Read and process a job from a job sink.

See tutorial document.

Usage:
  $ ds_sub.py -n NAME -f SUB_CFG --loglevel=20 --stderr

Where:
  - NAME is the name of the job sink (this should be the same as a name of
    a queue defined in the HUB_CFG--see hub.yml)

  - SUB_CFG is the job sink/worker configuration YAML file

  - TOPIC is a dotted topic (e.g. "foo", "foo.bar", "foo.bar.baz") which can
    include wild cards (specified as #) for any component(s).

Example:
  $ /ds_sub.py -f sub.yml -n ins1 --loglevel=20 --stderr --topic=foo

It will run and consume on the ins1 queue until you interrupt it.
"""

import sys
import os
import threading
from argparse import ArgumentParser

from datasink.initialize import default_topic
from datasink.worker import JobSink
from datasink import log


def job_doit(work_unit, fn_ack):
    """Simple job processor for 'doit' action.
    We just print the job
    """
    job = work_unit['job']

    try:
        action = job.get('action', None)
        # this is not actually necessary, since we shouldn't even be
        # called unless the action keyword was 'doit'
        assert action == 'doit'

        # do some kind of work on this job
        print("-------------------")
        print("got a job from {source_origin} at time {time_origin}".format(**job))

        # ACK: release this from the queue
        fn_ack(True, "success", {})

    except Exception as e:
        errmsg = f"error handling job '{action}': {e}"
        # NACK this
        fn_ack(False, errmsg, {})

def main(options, args):
    name = options.name
    # this processor's name
    if name is None:
        name = "worker-{}".format(os.getpid())

    logger = log.make_logger(name, options)

    jobsink = JobSink(logger, name)
    jobsink.read_config(options.configfile)

    # register a callback to handle 'doit' jobs
    jobsink.add_action('doit', job_doit)

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
