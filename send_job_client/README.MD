## This is the worker client
## worker by python form gearman
> 基于gearman的send_job客户端,运行需要环境中安装yaml配置包


# 更新:

> [3/8/2014]

    1.增加log模式

> [2/24/2014]

    1.调整debug模式,修改可选调试模式:单步/打印执行log
    2.修复发送job的执行优先级,所有优先级均为low级别

> [2/17/2014]

    1.调整debug模式,为单步调试

> [1/22/2014] 
    
    1.重构部分代码,增加配置文件,便于客户端的配置,增加调试模式,增强客户端的通用性
    2.插件(遵守eg中的规范)可以放到plugin文件夹内,即可启动客户端发送相应任务
