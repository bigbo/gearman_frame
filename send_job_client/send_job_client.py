#!/usr/bin/python2
# -*- coding:utf-8 -*-
#/*******************************************************************************
# * Author	 : ljingb.
# * Email	 : ljb90@liv.cn
# * Last modified : 2013-11-22 15:32
# * Filename	 : send_job_client
# * Description	 :简单的任务发送脚本,功能:
#                 1.异步发送多项任务:
#                   只管向server端发送任务,返回结果只现实任务是否被发送到server端,不统计worker端任务执行结果
#                 2.同步发送多项任务:
#                   向server端发送任务,返回的结果包含worker端执行结果反馈(长链接).可以再次捕获执行失败后的任务,进行相应处理
#                 3.需要读取配置文件,程序根目录下的config.yaml文件
# * *****************************************************************************/

import gearman
from gearman.constants import * 
import json
from clients import Client
import sys,threading
import yaml
import time
from loger import Logger

logger = Logger(logname='log/log.txt', loglevel=4, callfile=__file__).get_logger()

PLUGIN_NAME = None
DATA_LIST = []
IMPORT_MODULE = None
STATUS = 0

sys.path.insert(0,sys.path[0]+'/plugin')

def DEBUG_MODE(debug, data):
    if debug:
        print ('log:%s\n') % data
        logger.debug(('log:%s\n') % data)
        if debug == 2:
            raw_input('DEBUG,Press Enter to continue:')

class Send_Job_Client(object):
    def __init__(self, config_file = 'config.yaml'):
        """初始化配置文件"""
        global PLUGIN_NAME
        try:
            self.open_config = open(config_file)
        except:
            print 'not find cofig, please add in catalog!'
            logger.error('not find config, please add in catalog!')
            sys.exit()            
        load_config = yaml.load(self.open_config)
        try:
            self.hosts_list = load_config['HOSTS_LIST'].split(' ')
            self.client = Client(self.hosts_list)
        except:
            print 'HOSTS_LIST in config.yaml init error,please checking!'
            logger.error('HOSTS_LIST in config.yaml init error, please checking!')
            sys.exit()
        try:
            PLUGIN_NAME = load_config['PLUGIN_NAME']
        except:
            print 'PLUGIN_NAME in config.yaml init error,please checking!'
            logger.error('PLUGIN_NAME in config.yaml init error, please checking!')
            sys.exit()
        try:
            self.cycle = load_config['CYCLE']
        except:
            print 'CYCLE in config.yaml init error,please checking!'
            logger.error('CYCLE in config.yaml init error,please checking!')
            sys.exit()
        try:
            self.update = load_config['UPDATE']
        except:
            print 'UPDATE in config.yaml init error,please checking!'
            logger.error('UPDATE in config.yaml init error,please checking!')
            sys.exit()
        try:
            self.debug = load_config['DEBUG']
        except:
            print 'DEBUG in config.yaml init error,please checking!'
            logger.error('DEBUG in config.yaml init error,please checking!')
            sys.exit()
        try:
            self.log = load_config['LOG']
        except:
            print 'LOG in config.yaml init error,please checking!'
            logger.error('LOG in config.yaml init error,please checking!')
            sys.exit()

        self.open_config.close()

    def check_request_status(self, job_request):
        """
            可以增加或是使用多种方法,对出现有问题(延时过高/失败等)任务再次请求链接/放弃等处理(同步单线程长链接).
            提示:当使用多线程异步并发的时候,注意,此时只能现实任务的提交状况,而不能获取任务之情后的反馈,此时需要在worker端进行任务完成情况
        """
        if job_request.complete:
            info = "Job %s finished!  Result: %s - %s" % (job_request.gearman_job.unique,
                                                                job_request.state, job_request.result)
            DEBUG_MODE(self.debug, info)
            if self.log:
                logger.info(info)
        elif job_request.timed_out:
            """例如在此可以设置,如果有任务超时,则可以选择重试任务并直到成功.
                client.wait_until_jobs_accepted(submitted_requests,wait_until_complete = True,poll_timeout = None)
                """
            info = "Job %s timed out!" % job_request.gearman_job.unique
            DEBUG_MODE(self.debug, info)
            if self.log:
                logger.info(info)
        elif job_request.state == JOB_UNKNOWN:
            info = "Job %s connection failed!" % job_request.gearman_job.unique
            DEBUG_MODE(self.debug, info)
            if self.log:
                logger.info(info)

    def Transfer_Mode(self, data_for_process = [], back_ground = 'async'):
        jobs_infos = {}
        list_of_jobs = []

        if back_ground[1] == 'async':
            back_ground = True
            wait = False
        elif back_ground[1] == 'sync':
            back_ground = False
            wait = True
        elif len(back_ground) == 3 and back_ground[1] == 'stop':
            data_for_process = [{
                'task_name':back_ground[2],
                'data_pack':{'SHUTDOWN': True}
                    }]
            back_ground = False
        else:
            print 'transfer mode error !'
            logger.error('transfer mode error!') 
            return sys.exit()
    
        if not data_for_process:
            print "the jobs list is null!"
            logger.error('the jobs list is null!')
            return -1

        for tasks in data_for_process:
            assert isinstance(tasks, dict)
            DEBUG_MODE(self.debug, tasks)

            list_of_jobs.append(dict(task=tasks['task_name'], 
                                data=json.dumps(tasks['data_pack']),
                                priority = PRIORITY_LOW
                                ))

        submitted_requests = self.client.send_jobs(
                                            list_of_jobs,
                                            wait_until_complete=wait,
                                            background=back_ground)

        if self.log:
            logger.info("the total job have: %d" % len(submitted_requests))

        """对任务请求后状态的监管方法如下.
            submit_multiple_requests(jobs_requests, wait_until_complete, poll_timeout)
            wait_until_jobs_accepted(job_requests, poll_timeout=None)
            wait_until_jobs_completed(job_requests, poll_timeout=None)
            以第一种方法为事例
        """
        if self.debug or wait:
            completed_requests = self.client.wait_until_jobs_accepted(submitted_requests,poll_timeout = 2)
            for completed_job_request in completed_requests:
                self.check_request_status(completed_job_request)
       
        return 0


class Double_Thread(threading.Thread):
    def __init__(self, daemon):
        threading.Thread.__init__(self)
        self.daemon = daemon
    
    def run(self):
        global IMPORT_MODULE
        global DATA_LIST
        global STATUS
        while 1:
            if self.daemon:
                if handle.log:
                    logger.info("daemon thread,update data!")
                DATA_LIST = IMPORT_MODULE.get_data()
            else:
                if handle.log:
                    logger.info("undaemon thread,upfate data!")
                DATA_LIST = IMPORT_MODULE.get_data()

            DEBUG_MODE(handle.debug, DATA_LIST)

            if STATUS == 0: 
                time.sleep(handle.update * 60)
            STATUS = 0

def How_Use():
     print ('''\
This progream is send clients.
     Options include:
         --version :0.1
         --help:Input parameters
         async:is Asynchronous
         sync:is Synchronous
         stop:is send stop work,stop + task_name
         ''')

if __name__=="__main__":
    handle = Send_Job_Client()
    try:
        IMPORT_MODULE = __import__(PLUGIN_NAME)
    except:
        print "no have this PLUGIN !"
        logger.error("no have this PLUGIN !")
        sys.exit()

    if not len(sys.argv) >= 2:
        How_Use()
        sys.exit()
    if sys.argv[1] != 'stop':
        threads = Double_Thread(True)
        threads.start()
        time.sleep(handle.update)

    assert isinstance(DATA_LIST, list)
    
    while 1:
        if not DATA_LIST and sys.argv[1] != 'stop':
            STATUS = -1
            time.sleep(handle.update)
            continue
        
        try :
            STATUS = handle.Transfer_Mode( data_for_process = DATA_LIST, back_ground = sys.argv)
            if STATUS == -1:
                time.sleep(handle.update)
        except:
            How_Use()
            break

        if float(handle.cycle) == 0.0:
            break
        time.sleep(handle.cycle)
