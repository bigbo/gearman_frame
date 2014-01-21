gearman_frame
=============
## This is a framework based on distributed gearman
> send_job_client:

    clients.py --> the gearman clients,reseal the gearman API
    send_job_client.py --> the clients examples of use

> manage_client:

    admin.py --> the queue managemant,get status
    manage_client.py --> the admin clients examples of use
    [*]Since part of the package management needs API,so need "workers.py" and "clients.py" 

> worker_client:

    worker_client.py --> the gearman workers, reseal the gearman API
    runworkers.py --> the workers examples of use,the class worker inherit of methods to achieve
    veryeasyprocess --> a package
    config.yaml --> the config file
    eg_worker.py --> a examples of use gearman API


# 一.更新:
> 1.可以获取worker的信息,IP,task,新增可以命名worker名字

> 2.发送job端,当链接超时后,再次执行发送,任务不丢失.

> 3.解决可以通过admin端获取任务的状态以及可以查看到具体IP地址的活动情况

> 4.知道了发送任务的Background参数含义,终于知道为什么多线程的效率比但线程效率低了.其实是这个参数的问题.

> 5.基于之前的base修改完成一份worker的简单框架,实现了基本的任务处理方法调用.

> 6.链接采用长链接,可以通过相应指令结束worker操作.

> 7.基于admin,编写runadmin的客户端,目前实现一些简单的控制功能.

> 8.基于clients,编写runclients的任务两种发送事例.

> 9.基于worker端,编写执行workerclients,可以通过终端参数启动相应的worker

# 二.已知问题:
  [解决]链接超时问题.当到达一定时间后worker端自动停止跳出.

  [解决]重复提交job后,即使job端退出后,在adminclient端获取队列的信息后,依旧有job(需要写清理函数)

  [解决]tasks/base.py 文件是以worker为基础,编写的worker完成任务的方法,目前存在些问题.尚未改进.

  [解决]详细见包说明.worker端在执行任务的时候(多线程),当某些指令执行超时(设置超时时间)或是任务执行失败,会出现单个worker线程退出(崩溃)


# 三.关于veryeasyprocess包的使用(引用原作者):

> 此包对于进行shell命令的延时处理使用的是非多线程机制,使用所有包和函数均为python基本包,显示的封装成两个函数

1.shell_2_tty(_cmd=cmds, _cwd=None, _timeout=10*60)

_cmd 是要执行的外面命令行，要是一个 list， 如果是str，shell=True，会启动一个新的shell去干活的，这样，不利于进程的控制

_cwd 是执行这个命令行前，cd到这个路径下面，这个，对我的用应很重要，如果不需要可以用默认值

_timeout 这个是主角，设置超时时间（秒单位），从真重执行命令行开始计时，墙上时间超过 _timeout后，父进程会kill掉子进程，回收资源，并避免产生

zombie(僵尸进程)并将调用的命令行输出，直接输出到stdout,即是屏幕的终端上,(如果对输出比较讨厌，可以将 stdout = open("/dev/null", "w"), stderr
=open("/dev/null"),等等)

2.shell_2_tempfile(_cmd=cmds, _cwd=None, _timeout=10)

类同1，主要是增加，对命令行的输出，捕获，并返回给父进程，留作分析


# 四.关于异步多线程面临问题:

1.多个work处理如果有log记录时，或是一些内容存储,会出现个别log乱序(多线程异步并发的通病,不便于控制)

2.多个Work内调用的工作函数错误无法处理或通知，只能通过log查看结果(也是多线程不好控制与监测问题)

3.如果worker异常，没有接任务的worker很难发现，可以通过观察Job的数据量或是通过admin端读取整个server的任务队列中任务数量.

4.work启动的数量过少或者每个work工作时间过长,会导致job server服务器的任务过多积累,占用内存过多,以至于整个服务崩溃(具体就要相关的调教配置serv
er端了,不是代码能解决的)

# 五.关于同步多线程调用面临的问题:
1.当Client个数超过Worker个数的时候会出现排队现象，排队时会加长处理时间(也就是同步时候启用多线程,没有单线程速度快的原因);所以在启动多个Woker线程时候要尽量<job的数量

2.开辟大量Worker空闲会导致闲置占用资源,CPU/内存占用过大(目前代码上默认开辟10个worker的连接数)

3.传递的参数必须序列化,以至于执行效率过慢.
