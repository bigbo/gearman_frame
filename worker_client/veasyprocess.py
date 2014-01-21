#!/usr/local/bin/python
#-*- coding: UTF-8 -*-
# subwork

__author__   ="tommy (bychyahoo@gamil.com)"
__date__     ="2009-01-06 16:33"
__copyright__="Copyright 2009 tudou, Inc"
__license__  ="Td, Inc"
__version__  ="0.1"

import os
import time
import signal
import tempfile
import traceback
import subprocess

__all__ = ["subwork", "trace_back", "os", "time", "traceback", "subprocess", "signal"]

def trace_back():
    try:
        type, value, tb = sys.exc_info()
        return str(''.join(traceback.format_exception(type, value, tb)))
    except:
        return ''

def getCurpath():
    try:
        return os.path.normpath(os.path.join(os.getcwd(),os.path.dirname(__file__)))
    except:
        return

class subwork:
    """add timeout support!
    if timeout, we SIGTERM to child process, and not to cause zombie process safe!
    """
    def __init__(self, stdin=None, stdout=None, stderr=None, cmd=None, cwd=None, timeout=5*60*60):
        """default None
        """
        self.cmd       = cmd
        self.Popen     = None
        self.pid       = None
        self.returncode= None
        self.stdin     = None
        self.stdout    = stdout
        self.stderr    = stderr
        self.cwd       = cwd
        self.timeout   = int(timeout)
        self.start_time= None
        self.msg       = ''

    def send_signal(self, sig):
        """Send a signal to the process
        """
        os.kill(self.pid, sig)

    def terminate(self):
        """Terminate the process with SIGTERM
        """
        self.send_signal(signal.SIGTERM)

    def kill(self):
        """Kill the process with SIGKILL
        """
        self.send_signal(signal.SIGKILL)

    def wait(self):
        """ wait child exit signal,
        """
        self.Popen.wait()

    def free_child(self):
        """
        kill process by pid
        """
        try:
            self.terminate()
            self.kill()
            self.wait()
        except:
            pass

    def run(self):
        """
        run cmd
        """
#        print "[subwork]%s" % split_cmd(self.cmd)
        code = True
        try:
            self.Popen = subprocess.Popen(args=split_cmd(self.cmd), stdout=self.stdout, stderr=self.stderr, cwd=self.cwd)
            self.pid   = self.Popen.pid
            self.start_time = time.time()
            while self.Popen.poll() == None and (time.time() - self.start_time) < self.timeout :
                time.sleep(1)
                #print "running... %s, %s, %s" % (self.Popen.poll(), time.time() - self.start_time, self.timeout)
        except:
            self.msg += trace_back()
            self.returncode = -9998
            code = False
#            print "[subwork]!!error in Popen"
        # check returncode
        if self.Popen.poll() == None: # child is not exit yet!
            self.free_child()
            self.returncode = -9999
        else:
            self.returncode = self.Popen.poll()
        # return
        return {"code":code,\
                "msg":self.msg,\
                "req":{"returncode":self.returncode},\
                }

def split_cmd(s):
    """
    str --> [], for subprocess.Popen()
    """
    SC = '"'
    a  = s.split(' ')
    cl = []
    i = 0
    while i < len(a) :
        if a[i] == '' :
            i += 1
            continue
        if a[i][0] == SC :
            n = i
            loop = True
            while loop:
                if a[i] == '' :
                    i += 1
                    continue
                if a[i][-1] == SC :
                    loop = False
                    m = i
                i += 1
            #print a[n:m+1]
            #print ' '.join(a[n:m+1])[1:-1]
            cl.append((' '.join(a[n:m+1]))[1:-1])
        else:
            cl.append(a[i])
            i += 1
    return cl

def check_zero(dic=None):
    """
    check returncode is zero
    """
    if not isinstance(dic, dict):
        raise TypeError, "dist must be a Distribution instance"
#    print "returncode :", int(dic["req"]["returncode"])
    if int(dic["req"]["returncode"]) == 0:
        return True, dic["msg"]
    else:
        return False, dic["msg"]


def shell_2_tty(_cmd=None, _cwd=None, _timeout=5*60*60):
    """
    """
    try:
        shell=subwork(cmd=_cmd, stdout=None, stderr=None, cwd=_cwd, timeout=_timeout)
        return check_zero(shell.run())
    except:
        return False, trace_back()

def shell_2_tempfile(_cmd=None, _cwd=None, _timeout=5*60*60):
    """
    to collect out-string by tempfile
    """
    try:
        try:
            fout=tempfile.TemporaryFile()
            ferr=tempfile.TemporaryFile()
            shell=subwork(cmd=_cmd, stdout=fout, stderr=ferr, cwd=_cwd, timeout=_timeout)
            req=check_zero(shell.run())
            # get media info from tmp_out
            fout.seek(0)
            out=fout.read()
            if not out:
                ferr.seek(0)
                out=ferr.read()

            return req[0], str(out)
        finally:
            fout.close()
            ferr.close()
    except:
        return False, trace_back()

#---------------------------------------------
# main-test
#---------------------------------------------
if __name__ == '__main__' :
    pass
    cmds = "ping www.google.cn"
    cmds = "ls -la"
    print shell_2_tty(_cmd=cmds, _cwd=None, _timeout=10)
    #print shell_2_tempfile(_cmd=cmds, _cwd=None, _timeout=10)
    print "\nexit!"
    #time.sleep(60)
