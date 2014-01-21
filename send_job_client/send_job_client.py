#!/usr/bin/python2
# -*- coding:utf-8 -*-
#/*******************************************************************************
# * Author	 : ljingb.
# * Email	 : ljb90@liv.cn
# * Last modified : 2013-11-22 15:32
# * Filename	 : runclients.py
# * Description	 :简单的任务发送脚本,功能:
#                 1.异步发送多项任务:
#                   只管向server端发送任务,返回结果只现实任务是否被发送到server端,不统计worker端任务执行结果
#                 2.同步发送多项任务:
#                   向server端发送任务,返回的结果包含worker端执行结果反馈(长链接).可以再次捕获执行失败后的任务,进行相应处理
# * *****************************************************************************/

import gearman
from gearman.constants import JOB_UNKNOWN
import json
from clients import Client
import sys

#服务启动ip+端口,建议使用脚本前先设置,默认端口为本机的5000
HOSTS_LIST = ['0.0.0.0:5000']


def check_request_status(job_request):
    """
    可以增加或是使用多种方法,对出现有问题(延时过高/失败等)任务再次请求链接/放弃等处理(同步单线程长链接).
    提示:
       当使用多线程异步并发的时候,注意,此时只能现实任务的提交状况,而不能获取任务之情后的反馈,此时需要在worker端进行任务完成情况
    """
    if job_request.complete:
        print "Job %s finished!  Result: %s - %s" % (job_request.gearman_job.unique, job_request.state, job_request.result)
    elif job_request.timed_out:
        print "Job %s timed out!" % job_request.gearman_job.unique
        #例如在此可以设置,如果有任务超时,则可以选择重试任务并直到成功.
#        client.wait_until_jobs_accepted(submitted_requests,wait_until_complete = True,poll_timeout = None)
    elif job_request.state == JOB_UNKNOWN:
        print "Job %s connection failed!" % job_request.gearman_job.unique

def Asynchronous():
    client = Client(HOSTS_LIST)

    list_of_jobs = []
    data_for_process = 'hello world'
    jobs_infos = {}

    data_for_process = json.dumps(jobs_infos)
    list_of_jobs.append(dict(task="test", data=data_for_process))


    submitted_requests = client.send_jobs(list_of_jobs,wait_until_complete=True, background=True)
    print "the total job have: %d" % len(submitted_requests)

#对任务请求后状态的监管方法如下.
#submit_multiple_requests(jobs_requests, wait_until_complete, poll_timeout)
#wait_until_jobs_accepted(job_requests, poll_timeout=None)
#wait_until_jobs_completed(job_requests, poll_timeout=None)

#以第一种方法为事例
    completed_requests = client.wait_until_jobs_accepted(submitted_requests,poll_timeout = 2)

    for completed_job_request in completed_requests:
        check_request_status(completed_job_request)

    return None

def Synchronous():
    client = Client(HOSTS_LIST)
    list_of_jobs = []
    data_for_process = 'hello world'
    jobs_infos = {}

    list_of_jobs.append(dict(task="test", data=data_for_process))

    submitted_requests = client.send_jobs(list_of_jobs,wait_until_complete=True, background=False)
    print "the total job have: %d" % len(submitted_requests)

    completed_requests = client.wait_until_jobs_accepted(submitted_requests,poll_timeout = 2)

    for completed_job_request in completed_requests:
        check_request_status(completed_job_request)

    return None

def Send_Stop_Work():
    client = Client(HOSTS_LIST)

    list_of_jobs = []
    data_for_process = 'hello world'


    jobs_infos = {'SHUTDOWN': True}
    data_for_process = json.dumps(jobs_infos)
    list_of_jobs.append(dict(task="test", data=data_for_process))

    submitted_requests = client.send_jobs(list_of_jobs,wait_until_complete=True, background=False)
    print "the total job have: %d" % len(submitted_requests)

    completed_requests = client.wait_until_jobs_accepted(submitted_requests,poll_timeout = 2)

    for completed_job_request in completed_requests:
        check_request_status(completed_job_request)

    return None



def How_Use():
     print ('''\
This progream is send clients.
     Options include:
         --version :0.1
         --help:Input parameters
         async:is Asynchronous
         sync:is Synchronous
         stop:is send stop work
         ''')



if __name__=="__main__":
    commands = {
    'async' : Asynchronous,
    'sync' : Synchronous,
    'stop' : Send_Stop_Work
        }
    if not len(sys.argv) == 2:
        How_Use()
        sys.exit()
    commands[sys.argv[1]]()

