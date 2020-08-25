#
# datasink.py -- a program to receive data from Gen2
#
"""
A program to receive data from the Subaru Telescope Gen2 system.

Typical use:

$ datasink -f <configfile>

"""
import sys
import os, signal

from g2base import ssdlog, myproc

from . import worker, initialize, transfer


def server(options, config):
    # Create top level logger.
    logger = ssdlog.make_logger('datasink', options)

    key = config.get('key', None)
    if key is None:
        self.logger.error("Configuration file contains no 'key' directive")
        sys.exit(0)

    datadir = config.get('datadir', None)
    if datadir is None:
        datadir = os.getcwd()
        logger.warning("Storing files in {}".format(datadir))
        logger.info("To change this, add 'datadir' directive to config")
    else:
        logger.info("Storing files in {}".format(datadir))

    # this datasink's name
    name = key.split('-')[0]
    queue_names = [name]
    config['queue_names'] = queue_names

    # takes care of transfers into datadir
    xfer = transfer.Transfer(logger, datadir)

    def xfer_file(work_unit, fn_ack):
        job = work_unit['job']
        info, res = {}, {}

        xfer.transfer(job, info, res)

        # ACK allows another job to be released to us
        fn_ack(True, '', {})

        # After the transfer, dictionary `res` should contain a result code.
        if 'xfer_code' not in res:
            logger.error("No result code after transfer: %s" % (str(res)))
            return

    jobsink = worker.JobSink(logger, name)
    jobsink.add_action('transfer', xfer_file)
    jobsink.serve(config)

    logger.info("Exiting program.")
    sys.exit(0)


def main(options, args):

    if options.conffile is None:
        raise RuntimeError("Please specify a configuration file with -f")

    config = initialize.read_config(options.conffile)

    pidfile = config.get('pidfile', "/tmp/datasink.pid")

    if options.kill:
        try:
            try:
                with open(pidfile, 'r') as pid_f:
                    pid = int(pid_f.read().strip())

                print("Killing %d..." % (pid))
                os.kill(pid, signal.SIGKILL)
                print("Killed.")

            except IOError as e:
                print("Cannot read pid file (%s): %s" % (
                    pidfile, str(e)))
                sys.exit(1)

            except OSError as e:
                print("Error killing pid (%d): %s" % (
                    pid, str(e)))
                sys.exit(1)

        finally:
            sys.exit(0)

    detach = config.get('detach', False)
    if detach:
        print("Detaching from this process...")
        sys.stdout.flush()
        try:
            try:
                output = '/dev/null'
                child = myproc.myproc(server, args=[options, config],
                                      pidfile=pidfile, detach=True,
                                      stdout=output,
                                      stderr=output)
                child.wait()

            except Exception as e:
                print("Error detaching process: %s" % (str(e)))

            # TODO: check status of process and report error if necessary
        finally:
            sys.exit(0)

    # non-detach operation
    server(options, config)
