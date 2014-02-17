#!/usr/bin/python2
# -*- coding:utf-8 -*-
#/*******************************************************************************
# * Author	 : ljingb
# * Email	 : ljb90@live.cn
# * Last modified : 2013-11-18 17:10
# * Filename	 : admin.py
# * Description	 :
# * *****************************************************************************/

#from __future__ import division, unicode_literals, print_function
from gearman.admin_client import GearmanAdminClient
from workers import Worker

import json
import os

class Admin(GearmanAdminClient):
    def __init__(self, host_list=['0.0.0.0:5000'], *args, **kwargs):
        super(Admin, self).__init__(host_list=host_list, *args, **kwargs)
        self.host_list = host_list

    def get_response_time(self):
        """
        测试服务器的响应时间
        return float
        """
        return super(Admin, self).ping_server()

    def get_status(self):
        """
        获取queue server的状态
        返回字典return {}
        """
        return super(Admin, self).get_status()

    def get_workers(self):
        """
        查看worker以及他们工作的任务
        return {}
        """
        return super(Admin, self).get_workers()

    def get_version(self):
        """
        查看的Gearman服务器的版本号
        return str
        """
        return super(Admin, self).get_version()

    def empty_task(self, task):
        """
        清空某任务，采用lazy的方式。
        使用一个什么都不干的worker收拾任务。
        另:还可以用unregister_task注销任务,
        两种效果都不太好,顾需要实际测试处理.
        """
        def callback(worker, job):
            print "Efforts to clean up the list..."
            return json.dumps({'a': 'a'})

        worker = Worker(self.host_list)
        worker.register_task(task, callback)
        worker.safely_work()

    def start_server(self, port=5000):
        """
        开启任务，调用gearmand -d 命令，指定在本地的某port
        """
        os.system('gearmand -d -L 0.0.0.0 -p %s' %str(port))
        print("Job Server Start at port %s" %str(port))

    def send_shutdown(self, graceful=True):
        """
        shut downserver, 关闭连接的服务器,但是不是实时关闭。
        """
        print("Job Server will shutdown")
        return super(Admin, self).send_shutdown(graceful)
