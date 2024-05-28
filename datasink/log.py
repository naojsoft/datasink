#
# Simple common log format for Python logging module
#
import os
import logging, logging.handlers

STD_FORMAT = '%(asctime)s | %(levelname)1.1s | %(filename)s:%(lineno)d (%(funcName)s) | %(message)s'


# Default max logsize is 200 MB
max_logsize = 200 * 1024 * 1024

# Default maximum number of backups
max_backups = 4


def make_logger(logname, options, format=STD_FORMAT):

    # Create top level logger.
    logger = logging.Logger(logname)
    logger.setLevel(logging.DEBUG)

    fmt = logging.Formatter(format)

    if options.logfile is not None:
        filename = options.logfile
        fileHdlr  = logging.handlers.RotatingFileHandler(filename,
                                                         maxBytes=options.logsize,
                                                         backupCount=options.logbackups)
        fileHdlr.setFormatter(fmt)
        fileHdlr.setLevel(options.loglevel)
        logger.addHandler(fileHdlr)

    # Add output to stderr, if requested
    if options.logstderr:
        stderrHdlr = logging.StreamHandler()
        stderrHdlr.setLevel(options.loglevel)
        stderrHdlr.setFormatter(fmt)
        logger.addHandler(stderrHdlr)

    if options.pidfile:
        # save process id to a file if a path was specified
        mypid = os.getpid()
        with open(options.pidfile, 'w') as pid_f:
            pid_f.write(str(mypid))

    return logger


def simple_logger(logname, level=logging.ERROR, format=STD_FORMAT):

    # Create top level logger.
    logger = logging.getLogger(logname)
    logger.setLevel(level)

    fmt = logging.Formatter(format)

    # Add output to stderr
    stderrHdlr = logging.StreamHandler()
    stderrHdlr.setFormatter(fmt)
    logger.addHandler(stderrHdlr)

    return logger


def addlogopts(optprs):
    if hasattr(optprs, 'add_option'):
        # older optparse
        add_argument = optprs.add_option
    else:
        # newer argparse
        add_argument = optprs.add_argument

    add_argument("--log", dest="logfile", metavar="FILE",
                 help="Write logging output to FILE")
    add_argument("--loglevel", dest="loglevel", metavar="LEVEL",
                 default=20, type=int,
                 help="Set logging level to LEVEL")
    add_argument("--logsize", dest="logsize", metavar="NUMBYTES",
                 type=int, default=max_logsize,
                 help="Set maximum logging level to NUMBYTES")
    add_argument("--logbackups", dest="logbackups", metavar="NUM",
                 type=int, default=max_backups,
                 help="Set maximum number of backups to NUM")
    add_argument("--pidfile", dest="pidfile", metavar="FILE",
                 help="Set FILE for saving the pid")
    add_argument("--stderr", dest="logstderr", default=False,
                 action="store_true",
                 help="Copy logging also to stderr")
