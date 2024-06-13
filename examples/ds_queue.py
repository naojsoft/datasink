#! /usr/bin/env python3
"""
Create queues and process wayward jobs.

See tutorial document.

Usage:
  $ ./ds_queue.py -f HUB_CFG -a <action> -n NAME [-t <topic>]

Where:
  - HUB_CFG is the realm configuration YAML file

  - NAME is the name of the job sink (this should be the same as a name of
    a queue defined in the HUB_CFG--see hub.yml)

  - TOPIC is a dotted topic (e.g. "foo", "foo.bar", "foo.bar.baz") which can
    include wild cards (specified as #) for any component(s).

Example:
  $ ./ds_queue.py -f hub.yml -a disable -n ins2
"""

import sys
from argparse import ArgumentParser

from datasink.initialize import (read_config, configure_exchange,
                                 setup_queue, default_topic)


def main(options, args):

    # read config for the datasink "realm"
    configfile = options.configfile
    config = read_config(configfile)

    # then configure the exchange
    connection, channel = configure_exchange(config)

    queue_name = options.name
    if not queue_name in config['queues']:
        names = list(config['queues'].keys())
        raise ValueError(f"sink name (-n) should be one of {names}")

    q_cfg = config['queues'][queue_name]
    action = options.action

    if action == 'create':
        enabled = q_cfg.get('enabled', False)
        print(f"configuring {queue_name} enabled={enabled}")
        setup_queue(channel, queue_name, q_cfg, config, bind=enabled)

    elif action == 'purge':
        print(f"purging {queue_name} ...")
        channel.queue_purge(queue_name)

    elif action == 'delete':
        print(f"deleting {queue_name} ...")
        channel.queue_delete(queue_name, if_unused=False, if_empty=True)

    elif action == 'enable':
        if options.topic is None:
            topic = q_cfg.get('topic', default_topic)
        else:
            topic = options.topic
        print(f"configuring {queue_name} enabled=True")
        channel.queue_bind(queue_name, exchange=config['realm'],
                           routing_key=topic)

    elif action == 'disable':
        if options.topic is None:
            topic = q_cfg.get('topic', default_topic)
        else:
            topic = options.topic
        print(f"configuring {queue_name} enabled=False")
        channel.queue_unbind(queue_name, exchange=config['realm'],
                             routing_key=topic)

    else:
        raise ValueError(f"sorry, I don't know how to do action '{action}'")


if __name__ == "__main__":

    argprs = ArgumentParser("configure a sink queue")

    argprs.add_argument("-a", "--action", dest="action",
                        help="create|purge|delete|enable|disable")
    argprs.add_argument("-f", "--config", dest="configfile",
                        help="Specify the configuration file for this realm")
    argprs.add_argument("-n", "--name", dest="name", default=None,
                        metavar="NAME",
                        help="Specify the NAME for this queue")
    argprs.add_argument("-t", "--topic", dest="topic", default=None,
                        metavar="TOPIC",
                        help="Dot separated topic")

    (options, args) = argprs.parse_known_args(sys.argv[1:])

    if options.configfile is None:
        argprs.error("Please specify a config file with -f")

    if options.name is None:
        argprs.error("Please specify a sink name with -n")

    main(options, args)
