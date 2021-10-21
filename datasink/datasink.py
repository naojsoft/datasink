#
# datasink.py -- a program to receive data from Gen2
#
"""
A program to receive data from the Subaru Telescope Gen2 system.

Typical use:

$ datasink -f <configfile>

"""
import sys
import threading
import os, signal
import tarfile

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

    # if this is set, file will be moved here after transfer
    movedir = config.get('movedir', None)
    unpack_tarfiles = config.get('unpack_tarfiles', False)

    # if this is set, only instruments matching this instrument
    # will be transferred
    insfilter = config.get('insfilter', None)

    # this datasink's name
    name = key.split('-')[0]
    queue_names = [name]
    config['queue_names'] = queue_names

    # takes care of transfers into datadir
    xfer = transfer.Transfer(logger, datadir,
                             storeby=config.get('storeby', None),
                             md5check=config.get('md5check', False))

    def xfer_file(work_unit, fn_ack):
        job = work_unit['job']
        info, res = {}, {}

        if insfilter is not None:
            if job['insname'] not in insfilter:
                # ACK allows another job to be released to us
                fn_ack(True, '', {})
                return

        # get particulars of transfer method
        if 'host' not in job:
            job['host'] = config['transfer_host']
        if 'transfermethod' not in job:
            job['transfermethod'] = config['transfer_method']
        if 'username' not in job:
            job['username'] = config['transfer_username']
        job['direction'] = config.get('transfer_direction', 'from')

        xfer.transfer(job, info, res)

        # ACK allows another job to be released to us
        fn_ack(True, '', {})

        # After the transfer, dictionary `res` should contain a result code.
        if 'xfer_code' not in res:
            logger.error("No result code after transfer: %s" % (str(res)))
            return

        if res['xfer_code'] == 0:
            dst_path = res['dst_path']
            dst_dir, filename = os.path.split(dst_path)
            file_pfx, file_ext = os.path.splitext(filename)
            file_ext = file_ext.lower()

            try:
                if (unpack_tarfiles and
                    file_ext in ['.tar', '.tgz', '.tar.gz']):
                    if movedir is not None:
                        extract_dir = movedir
                    else:
                        extract_dir = dst_dir
                    # unpack tar file
                    with tarfile.open(dst_path, 'r') as tar_f:
                        tar_f.extractall(path=extract_dir)
                    # & remove tarball
                    os.remove(dst_path)
                else:
                    if movedir is not None:
                        move_path = os.path.join(movedir, filename)
                        os.rename(res['dst_path'], move_path)

                logger.info("unpack/move completed")

            except Exception as e:
                logger.error("Error unpacking/moving file after transfer: {}".format(e),
                             exc_info=True)

    ev_quit = threading.Event()

    jobsink = worker.JobSink(logger, name)
    jobsink.config = config
    jobsink.add_action('transfer', xfer_file)

    jobsink.start_workers(ev_quit)
    jobsink.serve(ev_quit)

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
