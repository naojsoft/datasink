#! /usr/bin/env python
"""
Activates and runs a datasink hub

Usage:
  $ ds_hub -f other.yml

Where other.yml looks like:

realm: other
realm_host: localhost
realm_username: 'guest'
realm_password: 'guest'
backlog_queue: 'backlog'
default_priority: 1
persist: true
queues:
    archiver:
        enabled: true
        persist: true
        transient: false
        priority: 1

"""

import sys
from argparse import ArgumentParser

from datasink.initialize import (read_config, configure_exchange, handle_dlx,
                                 setup_queue, example_dlx_cb)


def main(options, args):

    # read config for the datasink "realm"
    configfile = options.configfile
    if configfile is None:
        raise ValueError("Please specify a config file with -f")

    config = read_config(configfile)

    # then configure the exchange
    connection, channel = configure_exchange(config)

    # and finally set up datasink queues
    queues = config.get('queues', [])
    if len(queues) > 0:
        for name, dct in queues.items():
            if dct.get('enabled', False):
                setup_queue(channel, name, dct, config, bind=True)

    print("[*] Waiting for dead letters. To exit press Ctrl+C")
    handle_dlx(channel, config, example_dlx_cb)


if __name__ == "__main__":

    argprs = ArgumentParser("configure datasink hub")

    argprs.add_argument("-f", "--config", dest="configfile",
                        help="Specify the configuration file for this realm")

    (options, args) = argprs.parse_known_args(sys.argv[1:])

    main(options, args)
