#!/usr/bin/python2
# -*- coding:utf-8 -*-
#/*******************************************************************************
# * Author	 : ljjingb.
# * Email	 : ljb90@liv.cn
# * Last modified : 2013-11-22 15:32
# * Filename	 : runworkers.py
# * Description	 :
# * *****************************************************************************/

from workers import Worker, ThreadWorker
from gearman.worker import GearmanWorker

def callbacks(GearmanWorker, job):
    json_data = job.data
    result = json_data

    print result
    return result


for i in range(10):
    """
    range,为开辟线程数
    参数:
    task:任务名称
    callback:函数回调的名称
    workername:工人的名字(哪个IDC)
    daemon:是否为守护线程,默认Flase
    host_list:服务器的接口,list
    """
    t = ThreadWorker(task="test",callback=callbacks,workername='TEST',daemon=False)
#    print dir(t.daemon)
    t.start()
