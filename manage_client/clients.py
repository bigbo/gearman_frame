#!/usr/bin/python2
# -*- coding:utf-8 -*-
#/*******************************************************************************
# * Author	 : ljingb 
# * Email	 : ljb90@live.cn
# * Last modified : 2013-11-18 17:26
# * Filename	 : clients.py
# * Description	 :基于gearman的client类,实现任务发送功能
# * *****************************************************************************/

from gearman.client import GearmanClient
from gearman.constants import JOB_UNKNOWN
from loger import Logger

logger = Logger(logname='log/log.txt', loglevel=4, callfile=__file__).get_logger()

"""继承GearmanClient类,对已有的功能增加log,其中包括一下方法:
submit_multiple_requests(jobs_requests, wait_until_complete, poll_timeout)
wait_until_jobs_accepted(job_requests, poll_timeout=None)
wait_until_jobs_completed(job_requests, poll_timeout=None)
以及下面两个任务发送方法
"""
class Client(GearmanClient):
    def __init__(self, host_list=['0.0.0.0:5000'], *args, **kwargs):
        super(Client, self).__init__(host_list, *args, **kwargs)

    """发送单一任务
        submit_job(task, 任务名称
            data, 任务数据(二进制)
            unique=None,是否唯一标识
            priority=PRIORITY_NONE,优先级选择(PRIORITY_NONE, PRIORITY_LOW, PRIORITY_HIGH,JOB_UNKNOWN, JOB_PENDING)
            background=False,同步还是异步执行
            wait_until_complete=True,是否等待执行成功后返回
            max_retries=0, 最大重试次数
            poll_timeout=None, 超时时间(秒)
            )
    """
    def send_job(self, name, data, unique=False, *args, **kwargs):
        logger.info("Send job task %s, %r" %(name, data))
        self.submit_job(task=name, data=data, unique=unique, *args, **kwargs)

    """发送多个任务
        其中任务作为一个字典集合来发送
        例如(同单一任务的参数含义相同):dic_of_jobs = {'task': task, 'data': data, 'unique': unique, 'priority': priority}
            list_of_jobs = [dic_of_jobs1, dic_of_jobs2]
            submit_multiple_jobs(list_of_jobs,列表[字典],list_of_jobs
                    background=False,同步还是异步执行默认同步执行
                    wait_until_complete=True,是否等待执行成功后返回
                    max_retries=0,最大重试次数
                    poll_timeout=None,超时时间(s)
                    )
    def send_jobs(self, jobs, wait_until_complete=False, background=False):
        logger.info("Send jobs task num %d" %(len(jobs)))
        return self.submit_multiple_jobs(jobs, wait_until_complete=wait_until_complete,
                                     background=background)

