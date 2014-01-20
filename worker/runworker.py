#!/usr/bin/python2
# -*- coding: utf-8 -*-
#/*******************************************************************************
# * Author	 : ljingb.
# * Email	 : ljb90@live.cn
# * Last modified : 2013-11-19 11:39
# * Filename	 : runbase.py
# * Description	 : 实现任务处理
# * *****************************************************************************/

from workers import Worker, ThreadWorker
import json
import os,sys
import logging
import veasyprocess
import datetime

logger = logging.getLogger(__name__)
HOSTS_LIST = ['0.0.0.0:5000']

class IPMI(object):
    def __init__(self,args = None, kwargs = None):
       # 预留参数接口,便于扩展初始化
        print 'WORKER IS RUNING....'

    def callback(self,worker,job):
        json_data = json.loads(job.data)
        #job.unique 为job的唯一ID
        try:
            result = self.on_callback(json_data)
        except Exception, err:
            logger.info(err)
            result = {'SHUTDOWN': True}
        return json.dumps(result)

#ipmitool -I lanplus -H 10.2.20.237 -U root -P xxxx lan print
    def on_callback(self, json_data):
        #执行一些指令等,直接调用veasyprocess中的shell命令
        try:
            cmd = json_data['command']
            status,out_infos = veasyprocess.shell_2_tempfile(_cmd=cmd,_cwd=None,_timeout=15)
            if not status:
                print "fail"
                return {'re':'fail'}
            else:
                print out_infos
        except:
            print "exit"
            return {'SHUTDOWN': True}
        print 'Executed successfully!'
        return json_data



def build_workers(workers=3, *args, **kwargs):
    handle = IPMI(*args, **kwargs)
    for i in range(int(workers)):
        t = ThreadWorker(
                task=args[0][0],
                callback = handle.callback,
                workername = args[0][1],
                host_list = HOSTS_LIST
                )
        t.start()


def How_Use():
    print ('''\
    This progream is worker clients.
        Options include:
         --version :0.1
         --help:Input parameters
         Threads num, workername ,workername
         eg: python runbase.py 3 test IDC-A
         ''')

if __name__=="__main__":
    if not len(sys.argv) == 4:
        How_Use()
        sys.exit()
    build_workers(sys.argv[1],sys.argv[2:])


