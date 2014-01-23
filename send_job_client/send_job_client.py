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
from gearman.constants import JOB_UNKNOWN
import json
from clients import Client
import sys,threading
import yaml
import time

HOSTS_LIST = None
PLUGIN_NAME = None
CYCLE = None
UPDATE = None
DEBUG = None
DATA_LIST = []
IMPORT_MODULE = None

sys.path.insert(0,sys.path[0]+'/plugin')

def Init_Client(config_file = 'config.yaml'):
    """初始化配置文件"""
    global HOSTS_LIST
    global PLUGIN_NAME
    global CYCLE
    global DEBUG
    global UPDATE

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
        PLUGIN_NAME = load_config['PLUGIN_NAME']
    except:
        print 'PLUGIN_NAME in config.yaml init error,please checking!'
        sys.exit()
    try:
        CYCLE = load_config['CYCLE']
    except:
        print 'CYCLE in config.yaml init error,please checking!'
        sys.exit()
    try:
        UPDATE = load_config['UPDATE']
    except:
        print 'UPDATE in config.yaml init error,please checking!'
        sys.exit()
    try:
        DEBUG =load_config['DEBUG']
    except:
        print 'DEBUG in config.yaml init error,please checking!'
        sys.exit()

    open_config.close()
    return 0


def check_request_status(job_request):
    """
        可以增加或是使用多种方法,对出现有问题(延时过高/失败等)任务再次请求链接/放弃等处理(同步单线程长链接).
        提示:当使用多线程异步并发的时候,注意,此时只能现实任务的提交状况,而不能获取任务之情后的反馈,此时需要在worker端进行任务完成情况
    """
    if job_request.complete:
        print "Job %s finished!  Result: %s - %s" % (job_request.gearman_job.unique, job_request.state, job_request.result)
    elif job_request.timed_out:
        print "Job %s timed out!" % job_request.gearman_job.unique
        #例如在此可以设置,如果有任务超时,则可以选择重试任务并直到成功.
#       client.wait_until_jobs_accepted(submitted_requests,wait_until_complete = True,poll_timeout = None)
    elif job_request.state == JOB_UNKNOWN:
        print "Job %s connection failed!" % job_request.gearman_job.unique

def Transfer_Mode(data_for_process = [], back_ground = 'async'):
    client = Client(HOSTS_LIST)
    jobs_infos = {}
    list_of_jobs = []

    if back_ground[1] == 'async':
        back_ground = True
    elif back_ground[1] == 'sync':
        back_ground = False
    elif len(back_ground) == 3 and back_ground[1] == 'stop':
        data_for_process = [{
            'task_name':back_ground[2],
            'data_pack':{'SHUTDOWN': True}
                }]
        back_ground = False
    else:
        print 'transfer mode error !'
        return sys.exit()

    for tasks in data_for_process:
        list_of_jobs.append(dict(task=tasks['task_name'], data=json.dumps(tasks['data_pack'])))

    submitted_requests = client.send_jobs(
                                        list_of_jobs,
                                        wait_until_complete=True,
                                        background=back_ground)
    print "the total job have: %d" % len(submitted_requests)
#对任务请求后状态的监管方法如下.
#submit_multiple_requests(jobs_requests, wait_until_complete, poll_timeout)
#wait_until_jobs_accepted(job_requests, poll_timeout=None)
#wait_until_jobs_completed(job_requests, poll_timeout=None)

#以第一种方法为事例
    completed_requests = client.wait_until_jobs_accepted(submitted_requests,poll_timeout = 2)

    if DEBUG:
        for completed_job_request in completed_requests:
            check_request_status(completed_job_request)

    return None


class Double_Thread(threading.Thread):
    def __init__(self, daemon):
        threading.Thread.__init__(self)
        self.daemon = daemon

    def run(self):
        global IMPORT_MODULE
        global DATA_LIST
        while 1:
            if self.daemon:
                print "daemon thread,update data!"
                DATA_LIST = IMPORT_MODULE.get_data()
            else:
                print "undaemon thread,update data!"
                DATA_LIST = IMPORT_MODULE.get_data()
            if DEBUG:
                print "DATA_LIST:", DATA_LIST
            time.sleep(UPDATE*60)

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
    if Init_Client():
        print "init fail!"
        sys.exit()
    try:
        IMPORT_MODULE = __import__(PLUGIN_NAME)
    except:
        print "no have this PLUGIN !"
        sys.exit()

    commands = {
    'async' : Transfer_Mode,
    'sync' : Transfer_Mode,
    'stop' : Transfer_Mode
        }
    if not len(sys.argv) >= 2:
        How_Use()
        sys.exit()
    if sys.argv[1] != 'stop':
        threads = Double_Thread(True)
        threads.start()
        time.sleep(UPDATE)

    while 1:
        if not DATA_LIST and sys.argv[1] != 'stop':
            continue

        try :
            commands[sys.argv[1]](data_for_process = DATA_LIST,back_ground = sys.argv)
        except:
            How_Use()
            break

        if float(CYCLE) == 0.0:
            break
        time.sleep(CYCLE)
