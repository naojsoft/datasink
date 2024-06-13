#! /usr/bin/env python3
"""
Create queues and process wayward jobs.

See tutorial document.

Usage:
  $ ds_hub -f HUB_CFG [--dlx]

Where:
  - HUB_CFG is the realm configuration YAML file

Example:
  $ ./ds_hub.py -f hub.yml
"""

import sys
from argparse import ArgumentParser

from datasink.initialize import (read_config, configure_exchange, handle_dlx,
                                 setup_queue, example_dlx_cb)


def main(options, args):

    # read config for the datasink "realm"
    configfile = options.configfile
    print(f"configuring configuration from {configfile}...")
    config = read_config(configfile)

    # then configure the exchange
    print(f"configuring exchange...")
    connection, channel = configure_exchange(config)

    # and finally set up datasink queues
    print("configuring queues")

    queues = config.get('queues', [])
    if len(queues) > 0:
        for name, dct in queues.items():
            enabled = dct.get('enabled', False)
            print(f"configuring {name} enabled={enabled}")
            setup_queue(channel, name, dct, config, bind=enabled)

    print("queues configured.")

    if options.do_dlx:
        print("[*] Waiting for dead letters. To exit press Ctrl+C")
        try:
            handle_dlx(channel, config, example_dlx_cb)

        except KeyboardInterrupt:
            print("Caught keyboard interrupt, exiting hub...")


if __name__ == "__main__":

    argprs = ArgumentParser("configure datasink hub")

    argprs.add_argument("--dlx", dest="do_dlx", action="store_true",
                        default=False,
                        help="Stick around to handle wayward jobs")
    argprs.add_argument("-f", "--config", dest="configfile",
                        help="Specify the configuration file for this realm")

    (options, args) = argprs.parse_known_args(sys.argv[1:])

    if options.configfile is None:
        argprs.error("Please specify a config file with -f")

    main(options, args)
