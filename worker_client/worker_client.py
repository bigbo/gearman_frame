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
from clients import Client
from gearman.constants import *
import json
import sys
import logging
import veasyprocess
import datetime
import yaml


logger = logging.getLogger(__name__)
RETRY = None
TIME_OUT = 3
DEBUG = 0

class do_thing(object):
    def __init__(self, config_file = 'config.yaml'):
        global TIME_OUT
        global RETRY
        global DEBUG
        try:
            open_config = open(config_file)
        except:
            print 'not find cofig, please add in catalog!'
            sys.exit()
        load_config = yaml.load(open_config)
        try:
            self.hosts_list = load_config['HOSTS_LIST'].split(' ')
            self.client = Client(self.hosts_list)
        except:
            print 'HOSTS_LIST in config.yaml init error,please checking!'
            sys.exit()
        try:
            RETRY = int(load_config['RETRY'])
        except:
            print 'RETRY in config.yaml init error,please checking!'
            sys.exit()
        try:
            if load_config['TIME_OUT']:
                TIME_OUT = load_config['TIME_OUT']
        except:
            print 'TIME_OUT in config.yaml init error,please checking!'
            sys.exit()
        try:
            DEBUG = load_config['DEBUG']
        except:
            print 'DEBUG in config.yaml init error,please checking!'
            sys.exit()
        open_config.close()
        print 'WORKER CLIENT IS RUNING....'

    def callback(self,worker,job):
        json_data = json.loads(job.data)
        #job.unique 为job的唯一ID
        try:
            json_data = self.on_callback(json_data)
        except Exception, err:
            logger.info(err)
            json_data = {'SHUTDOWN': True}
       
        if DEBUG:
            print ('request:%s\n') % json_data
            if DEBUG == 2:
                raw_input('DEBUG,Press Enter to continue:')

        if isinstance(json_data, dict) and json_data.has_key('FLAG'):
            if json_data['FLAG']:
                print json_data['MESSAGE']
            else:
                if json_data['MESSAGE'] == -1:
                    print ('request:%s\n') % json_data
                    return json.dumps(json_data)
                try:
                    message = int(json_data['MESSAGE'])
                except (TypeError, ValueError):
                    print ('request is not int !:[MESSAGE] = %s') % json_data['MESSAGE']
                    return json.dumps(json_data)
                if message % RETRY == 0:
                    task_name = job.task
                else:
                    task_name = 'update'

            self.client.send_job(name = str(task_name),
                            data = job.data,
                            wait_until_complete = False,
                            background = True,
                            priority = PRIORITY_NONE)

        return json.dumps(json_data)


    def on_callback(self, json_data):
        #执行一些指令等,直接调用veasyprocess中的shell命令
        #循环尝试执行worker指令次数仅对执行自写脚本起作用:
        #   out_infos获取的是标准输出(str类型)
        #   设计为返回数据以空格分隔,第一位为执行指令状态(true/false),第二位为执行次数
        #   重复执行标准根据第二位的执行次数来做衡量
        table_date = datetime.datetime.now().strftime("%Y_%m_%d")
        cmd_argvs = ('%s \"%s\"') % (json_data['command'], json_data['argvs'])
        if DEBUG:
            print ('job_data:%s\n') % json_data
            if DEBUG == 2:
                raw_input('DEBUG,Press Enter to continue:')
        try:
            status,out_infos = veasyprocess.shell_2_tempfile(_cmd=cmd_argvs,_cwd=None,_timeout=TIME_OUT)
            if not status:
                return {'FLAG': False, 'MESSAGE': -1}
            if isinstance(out_infos, str) and out_infos.split(' ')[0] == 'False': 
                return {'FLAG': False, 'MESSAGE': out_infos.split(' ')[1].replace('\n','')}
            elif isinstance(out_infos, str) and out_infos.split(' ')[0] == 'True':
                return {'FLAG': True, 'MESSAGE': out_infos}
        except:
            print "error, exit!"
            return {'SHUTDOWN': True}
        print 'Executed successfully!'
        return {'FLAG': True, 'MESSAGE': out_infos}


def build_workers(workers=3, *args, **kwargs):
    handle = do_thing(*args, **kwargs)
    for i in range(int(workers)):
        t = ThreadWorker(
                task=args[0][0],
                callback = handle.callback,
                workername = args[0][1],
                host_list = handle.hosts_list
                )
        t.start()


def How_Use():
    print ('''\
    This progream is worker clients.
        Options include:
         --version :0.1
         --help:Input parameters
         Threads num, workername ,workername
         eg: python worker_client.py 3 test TEST
         ''')

if __name__=="__main__":
    if not len(sys.argv) == 4:
        How_Use()
        sys.exit()
    build_workers(sys.argv[1],sys.argv[2:])
