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

#服务启动ip+端口,建议使用脚本前先设置,默认端口为本机的5000
HOSTS_LIST = ['0.0.0.0:5000']

def show_status():
    admin = Admin(HOSTS_LIST)
    current_status = admin.get_status()
    num = 0

    for status in current_status:
        print status

def get_workers(task_name = None):
    workers = []
    admin = Admin(HOSTS_LIST)
    for w in admin.get_workers():
        if w['tasks']:
            workers.append( w )

    print "totla workers: %d" % (len(workers))

    if not task_name:
        for i in workers:
            print "the IP:%s---Worker_name:%s---Task_name:%s"%(i['ip'],i['client_id'],i['tasks'])
    else:
        for i in workers:
            if task_name and i['tasks'][0] == task_name:
                print "the IP:%s---Worker_name:%s---Task_name:%s"%(i['ip'],i['client_id'],i['tasks'])
    return workers


def send_task(task_name, json_data, priority=PRIORITY_NONE):
    client = Client(HOSTS_LIST)
    client.send_job(name=task_name, data=json.dumps(json_data),
                    wait_until_complete=False, priority=priority)
    print ("Dispatch a task name %s, %r" %(task_name, json_data))


def clear_worker(task_name = None, priority = 'high'):
    p = {
      'high': PRIORITY_HIGH,
      'normal': PRIORITY_NONE,
      'low': PRIORITY_LOW
        }

    send_task(task_name, {'SHUTDOWN': True}, p.get(priority, PRIORITY_NONE))


def clear_workers(task_name = None,priority = 'high'):
    admin = Admin(HOSTS_LIST)
    current_status = admin.get_status()
    num = 0

    if not task_name:
        print "I don't know which worker will be clear!"
        return

    if task_name == 'all':

        for status in current_status:
            num = 0
            num = int(status['workers'])
            for i in range(num):
                clear_worker(status['task'], priority=priority)
    else:
        for status in current_status:
            if status['task'] == task_name:
                num = int(status['workers'])
            print status

        for i in range(num):
            clear_worker(task_name, priority=priority)

        if num == 0:
            print "Task list no have name is '%s'  task!" % task_name

    return None

def clear_server_list(task_name = None):
    admin = Admin(HOSTS_LIST)
    current_status = admin.get_status()

    if not task_name:
        print "I don't know clear which data list!"
        return
    if task_name == 'all':
        pass
    else:
        admin.empty_task(str(task_name))

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


def start_server(prot = 5000):
    admin = Admin(HOSTS_LIST)
    admin.start_server(prot)

def stop_server():
    admin = Admin(HOSTS_LIST)
    admin.send_shutdown()

def ping_server():
    admin = Admin(HOSTS_LIST)
    print admin.get_response_time()

if __name__=="__main__":
    commands = {
    'start-server' : start_server,
    'show-status' : show_status,
    'get-workers' : get_workers,
    'stop-worker' : clear_workers,
    'stop-server' : stop_server,
    'ping-server' : ping_server,
    'clear-list'  : clear_server_list
        }
    if not len(sys.argv) > 1:
        How_Use()
        sys.exit()
    if len(sys.argv) == 2:
        commands[sys.argv[1]]()
    else:
        commands[sys.argv[1]](sys.argv[2])

