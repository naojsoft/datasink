#
# transfer.py -- class for managing transfers of files
#
# This is open-source software licensed under a BSD license.
# Please see the file LICENSE.txt for details.
#

# stdlib imports
import sys
import os
import time
import datetime
import subprocess
import socket
import json

class TransferError(Exception):
    pass
class md5Error(TransferError):
    pass

class Transfer:

    def __init__(self, logger, datadir,
                 md5check=False, mountmangle=None, storeby=None):

        self.logger = logger
        # Base of where to store any FITS files we receive directly
        self.datadir = datadir

        # Should we verify md5 checksum
        self.md5check = md5check
        # controls use of subdirectories for storing files
        self.storeby = storeby

        # Used to mangle remote filenames for NFS copying (see transfermethod())
        if mountmangle:
            self.mountmangle = mountmangle.rstrip('/')
        else:
            self.mountmangle = None

        # my hostname, for logging
        self.myhost = socket.getfqdn()

    def calc_md5sum(self, filepath):
        try:
            start_time = time.time()
            # NOTE: this will stall us a long time, so make sure
            # you've called this function from a thread
            proc = subprocess.Popen(['md5sum', filepath],
                                    stdout=subprocess.PIPE)
            result = proc.communicate()[0]
            if proc.returncode == 0:
                calc_md5sum = result.split()[0]
                calc_md5sum = calc_md5sum.decode('latin1')
            else:
                raise md5Error(result)

            self.logger.debug("%s: md5sum=%s calc_time=%.3f sec" % (
                    filepath, calc_md5sum, time.time() - start_time))
            return calc_md5sum

        except Exception as e:
            raise TransferError("Error calculating md5sum for '%s': %s" % (
                filepath, str(e)))

    def check_md5sum(self, filepath, req):
        """Check the md5sum of a file.  Requires the command 'md5sum' is
        installed.
        """
        calc_md5sum = self.calc_md5sum(filepath)

        md5sum = req.get('md5sum', None)
        if md5sum is None:
            # For now only raise a warning when checksum seems to be
            # missing
            #raise md5Error("%s: upstream md5 checksum missing!" % (
            #    filepath))
            self.logger.warning("%s: missing checksum. upstream md5 checksum turned off?!" % (
                    filepath))
            return calc_md5sum

        # Check MD5
        if calc_md5sum != md5sum:
            errmsg = "%s: md5 checksums don't match recv='%s' sent='%s'" % (
                filepath, calc_md5sum, md5sum)
            raise md5Error(errmsg)

        return md5sum

    def get_newpath(self, filename, req):

        if self.storeby is None:
            newpath = os.path.abspath(os.path.join(self.datadir, filename))
        elif self.storeby == 'propid':
            propid = req.get('propid', None)
            if propid is None:
                raise TransferError("Storing by PROP-ID and propid is 'None': {}".format(req))
            newpath = os.path.abspath(os.path.join(self.datadir, propid,
                                                   filename))
        elif self.storeby == 'insname':
            insname = req.get('insname', None)
            if insname is None:
                raise TransferError("Storing by instrument and insname is 'None': {}".format(req))
            newpath = os.path.abspath(os.path.join(self.datadir, insname,
                                                   filename))
        else:
            raise TransferError("I don't know how to store by '%s'" % (
                self.storeby))

        return newpath

    def transfer(self, req, info, xfer_dict):

        filepath = req['srcpath']

        (dirpath, filename) = os.path.split(filepath)
        self.logger.debug("Preparing to transfer %s..." % (filename))
        newpath = self.get_newpath(filename, req)

        # check for file exists already; if so, rename it and allow the
        # transfer to continue
        self.check_rename(newpath)

        info.update(dict(md5sum=None, filesize=None))

        try:

            # Copy file
            self.transfer_from(filepath, req['host'], newpath,
                               transfermethod=req['transfermethod'],
                               username=req.get('username', None),
                               port=req.get('port', None),
                               result=xfer_dict, info=info, req=req)

        except Exception as e:
            errmsg = "Failed to transfer file '%s': %s" % (filename, str(e))
            self.logger.error(errmsg, exc_info=True)

            info['errmsg'] = errmsg
            return

    def check_rename(self, newpath):
        if os.path.exists(newpath):
            renamepath = newpath + time.strftime(".%Y%m%d-%H%M%S",
                                                 time.localtime())
            self.logger.warn("File '%s' exists; renaming to '%s'" % (
                newpath, renamepath))
            os.rename(newpath, renamepath)
            return True
        return False

    def transfer_from(self, filepath, host, newpath,
                      transfermethod='ftps', username=None,
                      password=None, port=None, result={},
                      info={}, req={}):

        """This function handles transfering a file via one of the following
        protocols: { ftp, ftps, sftp, http, https, scp, copy (nfs) }

        Parameters
        ----------
          filepath: str
              file path on the source host
          host: str
              the source hostname
          newpath: str
              file path on the destination host
          transfermethod: str (optional, defaults to 'ftps')
              one of the protocols listed above
          port: int (optional)
              port for the protocol
          username: str or None (optional)
              username for ftp/ftps/sftp/http/https/scp transfers
          password: str or None (optional)
              password for ftp/ftps/sftp/http/https transfers
          result: dict (optional)
              dictionary to store info about transfer
          info: dict (optional)
              metadata info collected about the file
          req: dict
              the original file transfer request
        """

        self.logger.info("transfer file (%s): %s <-- %s" % (
            transfermethod, newpath, filepath))
        (directory, filename) = os.path.split(filepath)

        result.update(dict(time_start=datetime.datetime.now(),
                           src_host=host, src_path=filepath,
                           dst_host=self.myhost, dst_path=newpath,
                           xfer_method=transfermethod))

        if not username:
            username = os.environ.get('LOGNAME', 'anonymous')

        if transfermethod == 'copy':
            # NFS mount is assumed to be setup.  If we have an alternate
            # mount location locally, then mangle the path to reflect the
            # mount on this host
            if self.mountmangle and filepath.startswith(self.mountmangle):
                sfx = filepath[len(self.mountmangle):].lstrip('/')
                copypath = os.path.join(self.mountmangle, sfx)
            else:
                copypath = filepath

            cmd = ("cp %s %s" % (copypath, newpath))
            result.update(dict(src_path=copypath))

        elif transfermethod == 'scp':
            # passwordless scp is assumed to be setup
            cmd = ("scp %s@%s:%s %s" % (username, host, filepath, newpath))

        else:
            # <== Set up to do an lftp transfer (ftp/sftp/ftps/http/https)

            if password is not None:
                login = '"%s","%s"' % (username, password)
            else:
                # password to be looked up in .netrc
                login = '"%s"' % (username)

            setup = "set xfer:log yes; set net:max-retries 5; set net:reconnect-interval-max 2; set net:reconnect-interval-base 2; set xfer:disk-full-fatal true;"

            # Special args for specific protocols
            if transfermethod == 'ftp':
                setup = "%s set ftp:use-feat no; set ftp:use-mdtm no;" % (setup)

            elif transfermethod == 'ftps':
                setup = "%s set ftp:use-feat no; set ftp:use-mdtm no; set ftp:ssl-force yes;" % (
                    setup)

            elif transfermethod == 'sftp':
                setup = "%s set ftp:use-feat no; set ftp:ssl-force yes;" % (
                    setup)

            elif transfermethod == 'http':
                pass

            elif transfermethod == 'https':
                pass

            else:
                raise TransferError("Request to transfer file '%s': don't understand '%s' as a transfermethod" % (
                    filename, transfermethod))

            if port:
                cmd = ("""lftp -e '%s get %s -o %s; exit' -u %s %s://%s:%d""" % (
                    setup, filepath, newpath, login, transfermethod, host, port))
            else:
                cmd = ("""lftp -e '%s get %s -o %s; exit' -u %s %s://%s""" % (
                    setup, filepath, newpath, login, transfermethod, host))


        try:
            result.update(dict(xfer_cmd=cmd))

            self.logger.info(cmd)
            res = os.system(cmd)

            # Check size
            statbuf = os.stat(newpath)
            info['filesize'] = statbuf.st_size
            size = req.get('size', None)
            if size != None:
                if info['filesize'] != size:
                    raise md5Error("File size (%d) does not match sent size (%d)" % (
                        info['filesize'], size))
            self.logger.debug("passed file size check (%s)" % (str(size)))

            if self.md5check:
                # Check MD5 hash
                md5sum = self.check_md5sum(newpath, req)
            else:
                md5sum = None
            info['md5sum'] = md5sum

            result.update(dict(time_done=datetime.datetime.now(),
                               md5sum=md5sum, xfer_code=res))

        except (OSError, md5Error) as e:
            self.logger.error("Command was: %s" % (cmd))
            errmsg = "Failed to transfer file '%s': %s" % (
                filename, str(e))
            result.update(dict(time_done=datetime.datetime.now(),
                               res_str=errmsg, xfer_code=-1))
            raise TransferError(errmsg)

        if res != 0:
            self.logger.error("Command was: %s" % (cmd))
            errmsg = "Failed to transfer file '%s': exit err=%d" % (
                filename, res)
            result.update(dict(res_str=errmsg))
            raise TransferError(errmsg)


class TransferRequest:

    def __init__(self, srcpath, dstpath,
                 username, host, transfermethod,
                 size=None, md5sum=None, priority=None,
                 **kwargs):

        time_created = datetime.datetime.now()
        time_created_s = time_created.isoformat()

        # TODO: do we need something more unique than this?
        _id = time_created_s.replace('-', '')
        _id = _id.replace(':', '')
        _id = _id.replace('.', '_')

        self._d = dict(srcpath=srcpath, dstpath=dstpath,
                       username=username, host=host, id=_id,
                       transfermethod=transfermethod, size=size,
                       md5sum=md5sum, time_created=time_created.isoformat(),
                       priority=priority)
        # add user-added metadata
        self._d.update(kwargs)

    @property
    def id(self):
        return self._d['id']

    def __getitem__(self, key):
        return self._d[key]

    def to_string(self):
        return json.dumps(self._d)

    def load(self, filepath):
        with open(filepath, 'r') as in_f:
            buf = in_f.read()
        self._d = json.loads(buf)

    def store(self, filepath):
        with open(filepath, 'w') as out_f:
            out_f.write(json.dumps(self._d))

    def get(self, *args):
        if len(args) == 1:
            return self._d[args[0]]
        elif len(args) == 2:
            return self._d.get(args[0], args[1])
        raise ValueError("Too many parameters to get()")

    def as_dict(self):
        return self._d

    def __str__(self):
        return self.to_string()

    def _can_cmp(self, other):
        return not((self['priority'] is None) or (other['priority'] is None))

    def __lt__(self, other):
        if self._can_cmp(other):
            return self['priority'] < other['priority']
        else:
            return NotImplemented
