#!/usr/bin/python2
# -*- coding: utf-8 -*-
#/*******************************************************************************
# * Author	 : ljingb.
# * Email	 : ljb90@live.cn
# * Last modified : 2013-11-19 11:39
# * Filename	 : worker_client
# * Description	 : 实现任务处理
# * *****************************************************************************/

from workers import Worker, ThreadWorker
import json
import os,sys
import logging
import veasyprocess
import datetime
import yaml


logger = logging.getLogger(__name__)
HOSTS_LIST = None
DATA_SAVE = None
TIME_OUT = 3
DEBUG = 0

class do_thing(object):
    def __init__(self, config_file = 'config.yaml'):
        global HOSTS_LIST
        global TIME_OUT
        global DATA_SAVE
        global DEBUG
        try:
            open_config = open(config_file)
        except:
            print 'not find cofig, please add in catalog!'
            sys.exit()
        load_config = yaml.load(open_config)
        try:
            HOSTS_LIST = load_config['HOSTS_LIST'].split(' ')
        except:
            print 'HOSTS_LIST in config.yaml init error,please checking!'
            sys.exit()
        try:
            DATA_SAVE = load_config['DATA_STORAGE']
        except:
            print 'DATA_SAVE in config.yaml init error,please checking!'
            sys.exit()
        try:
            if load_config['TIME_OUT']:
                TIME_OUT = load_config['TIME_OUT']
        except:
            print 'TIME_OUT in config.yaml init error,please checking!'
            sys.exit()
        try:
            DEBUG = load_config['DEBUG']
            if DEBUG:
                DATA_SAVE = 0
        except:
            print 'DEBUG in config.yaml init error,please checking!'
            sys.exit()
        open_config.close()
        print 'WORKER IS RUNING....'

    def callback(self,worker,job):
        json_data = json.loads(job.data)
        #job.unique 为job的唯一ID
        try:
            json_data = self.on_callback(json_data)
        except Exception, err:
            logger.info(err)
            json_data = {'SHUTDOWN': True}
        return json.dumps(json_data)


    def on_callback(self, json_data):
        #执行一些指令等,直接调用veasyprocess中的shell命令
        table_date = datetime.datetime.now().strftime("%Y_%m_%d")
        cmd_argvs = ('%s \"%s\"') % (json_data['command'], json_data['argvs'])
        try:
            status,out_infos = veasyprocess.shell_2_tempfile(_cmd=cmd_argvs,_cwd=None,_timeout=TIME_OUT)
            if not status:
                print "fail"
                return {'re':'fail'}
            elif DATA_SAVE or not DEBUG:
                print out_infos
            else:
                print ('job_data:%s\nresult:%s\n') % (json_data, out_infos)
        except:
            print "exit"
            return {'SHUTDOWN': True}
        print 'Executed successfully!'
        return json_data

def build_workers(workers=3, *args, **kwargs):
    handle = do_thing(*args, **kwargs)
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
         eg: python runbase.py 3 test TEST
         ''')

if __name__=="__main__":
    if not len(sys.argv) == 4:
        How_Use()
        sys.exit()
    build_workers(sys.argv[1],sys.argv[2:])
