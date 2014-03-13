#!/usr/bin/python2
# -*- coding:utf-8 -*-
#/*******************************************************************************
# * Author	 : ljingb
# * Email	 : ljb90@live.cn
# * Last modified : 2013-11-25 15:48
# * Filename	 : manage_client
# * Description	 :一个简单的gearman的管理客户端,功能:
#                1.服务的启动(默认端口0.0.0.0:5000)
#                2.显示服务端的状态(任务队列等的详细情况)
#                3.获取所有worker的详细信息(参数:空/['指定任务名'])
#                4.结束任务(参数:all/['指定任务名'])
#                5.停止服务
#                6.查看与服务器的链接状况
#                7.清空队列
# * *****************************************************************************/

from admin import Admin
import time
import sys
from clients import Client
import json
from gearman.constants import *
from loger import Logger 

#服务启动ip+端口,建议使用脚本前先设置,默认端口为本机的5000
HOSTS_LIST = ['0.0.0.0:5000']

class Gearman_Manage(object):
    def __init__(self, host_list = ['0.0.0.0:5000']):
        """初始化服务端/客户端服务 """
        self.logger = Logger(logname='log/log.txt', loglevel = 3, callfile = __file__).get_logger()
        try:
            self.server = Admin(host_list)
            self.client = Client(host_list)
        except:
            print "Gearman server host port is error!"
            self.logger.error("Dispatch a task name %s, %r" %(task_name, json_data))
            sys.exit()


    def show_status(self):
        """查看server状态信息"""
        current_status = self.server.get_status()
        num = 0

        for status in current_status:
            print status

    def get_worker(self, task_name = None):
        """查看worker端状态信息"""
        workers = []
        for w in self.server.get_workers():
            if w['tasks']:
                workers.append( w )

        print "totla workers: %d" % (len(workers))

        if not task_name:
            for i in workers:
                print "the IP:[%s]---Worker_name:[%s]---Task_name:[%s]"%(i['ip'],i['client_id'],i['tasks'])
        else:
            for i in workers:
                if task_name and i['tasks'][0] == task_name:
                    print "the IP:[%s]---Worker_name:[%s]---Task_name:[%s]"%(i['ip'],i['client_id'],i['tasks'])
        return workers


    def send_task(self, task_name, json_data, priority=PRIORITY_NONE):
        """发送控制指令"""
        self.client.send_job(name=task_name, data=json.dumps(json_data),
                        wait_until_complete=False, background=True, priority=priority)
        print ("Dispatch a task name %s, %r" %(task_name, json_data))
        self.logger.info("Dispatch a task name %s, %r" %(task_name, json_data))


    def clear_workers(self, task_name = None,priority = PRIORITY_HIGH):
        """关闭worker"""
        current_status = self.server.get_status()
        num = 0

        if not task_name:
            print "I don't know which worker will be clear!"
            return

        if task_name == 'all':
            for status in current_status:
                num = 0
                num = int(status['workers'])
                for i in range(num):
                    self.send_task(status['task'], {'SHUTDOWN': True},priority)
                print "stop worker total:%d" % num
        else:
            for status in current_status:
                if status['task'] == task_name:
                    num = int(status['workers'])
                print status

            for i in range(num):
                self.send_task(task_name,{'SHUTDOWN': True},priority)
            print "stop worker total:%d" % num
            if num == 0:
                print "Task list no have name is '%s'  task!" % task_name

        return None

    def clear_server_list(self, task_name = None):
        """清理server job 队列"""
        current_status = self.server.get_status()

        if not task_name:
            print "I don't know clear which data list!"
            return
        if task_name == 'all':
            pass
        else:
            num = [i['queued'] for i in current_status if task_name == i['task']]
            print "the list len:%d" % num[0]
            self.server.empty_task(str(task_name))

    def start_server(self, prot = 5000):
        """启动服务器"""
        self.server.start_server(prot)
        self.logger.info("start server.")

    def stop_server(self):
        """停止服务器"""
        try:
            self.server.send_shutdown()
            self.logger.info("stop server.")
        except:
            print "server is not run!"

    def ping_server(self):
        """查看服务器连通状况"""
        try:
            print self.server.get_response_time()
        except:
            print "server is not run!"

def How_Use():
     print ('''\
This progream is gearman admin client.
     Options include:
         --version :0.1
         --help:Input parameters
         start-server:is start server
         show-status:is show status
         get-workers:is get workers
         stop-worker:is clear workers
         stop-server:is stop server
         ping-server:is ping server
         clear-list :is clear list of server
         ''')


if __name__=="__main__":
    handle = Gearman_Manage(HOSTS_LIST)
    commands = {
    'start-server' : handle.start_server,
    'show-status' : handle.show_status,
    'get-workers' : handle.get_worker,
    'stop-worker' : handle.clear_workers,
    'stop-server' : handle.stop_server,
    'ping-server' : handle.ping_server,
    'clear-list'  : handle.clear_server_list
        }
    if not len(sys.argv) > 1:
        How_Use()
        sys.exit()
    try:
        if len(sys.argv) == 2:
            commands[sys.argv[1]]()
        else:
            commands[sys.argv[1]](sys.argv[2])
    except:
        print "server will not run or command have errro!"
        How_Use()
