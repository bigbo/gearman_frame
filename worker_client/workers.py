#!/usr/bin/python2
# -*- coding:utf-8 -*-
#/*******************************************************************************
# * Author	 : ljingb.
# * Email	 : ljb90@live.cn
# * Last modified : 2013-11-18 17:33
# * Filename	 : workers.py
# * Description	 : 继承gearman的基类,并进行相应的扩展.
# * *****************************************************************************/

from gearman.worker import GearmanWorker
import json
import threading
import logging

logger = logging.getLogger(__name__)

WILL_SHUT_DOWN = 2
SHUT_DOWN = 1

class Worker(GearmanWorker):
    def __init__(self, host_list=['0.0.0.0:5000']):
        self.continue_work = 10
        super(Worker, self).__init__(host_list=host_list)

    def on_job_execute(self, current_job):
        return super(Worker, self).on_job_execute(current_job)

    def on_job_exception(self, current_job, exc_info):
        return super(Worker, self).on_job_exception(current_job, exc_info)

#成功后返回状态,并判断任务字符串中是否有SHURDIWN,有则关闭任务
    def on_job_complete(self, current_job, job_result):
        """
        任务后的操作,可以返回数据,或是做一些操作,log记录等.
        """
#        print("Current_job %r complete" %current_job)
        json_data = json.loads(job_result)
        if isinstance(json_data, dict) and json_data.has_key('SHUTDOWN'):
            self.continue_work = WILL_SHUT_DOWN
            return super(Worker, self).on_job_complete(current_job, job_result)
        return super(Worker, self).on_job_complete(current_job, job_result)

    def safely_work(self):
        self.continue_work = 10
        try:
            self.work()
        except Exception, err:
            print "error"
            logger.error(err)
            pass

    def after_poll(self, any_activity):
        return True

    def poll_connections_until_stopped(self, submitted_connections, callback_fxn, timeout=None):
        def smart_callback(any_activity):
            if self.continue_work == WILL_SHUT_DOWN:
                self.continue_work -= 1
                return callback_fxn(any_activity)
            elif self.continue_work == SHUT_DOWN:
                self.continue_work -= 1
                return callback_fxn(any_activity) and self.continue_work
            else:
                return callback_fxn(any_activity) and self.continue_work

        return super(Worker, self).poll_connections_until_stopped(submitted_connections,
                                                       smart_callback, timeout=timeout)

#多线程,调用方式为函数回调方法.
class ThreadWorker(threading.Thread):
    def __init__(self, task, callback, workername = 'workaholic', daemon=False, host_list=['0.0.0.0:5000']):
        threading.Thread.__init__(self)
        self.workername = workername
        self.task = task
        self.callback = callback
        self.daemon = daemon
        self.worker = Worker(host_list=host_list)

    def run(self):
        self.worker.set_client_id(self.workername)
        self.worker.register_task(self.task, self.callback)
        self.worker.safely_work()
