#!/usr/bin/python2
# -*- coding: utf-8 -*-
#/*******************************************************************************
# * Author	 : ljingb
# * Email	 : ljb90@live.cn
# * Last modified : 2014-01-09 19:30
# * Filename	 : eg.py
# * Description	 :It is a plug-in case 
# * *****************************************************************************/

def get_data():
#所有任务list中的格式需遵守以下格式
#     task_list = [  'task_name':'xxxx',          --->任务名称（what worker do）
#                    'data_pack':                 --->数据包（任务所有数据）
#                            {   'command':'xxx', --->执行指令
#                                'argvs':'xxx',   --->指令参数
#                             }
#                    ]
                    
    task_list = [
            {'task_name':'test1', 'data_pack':{'command':'ls','argvs':'./'}},
            {'task_name':'test2', 'data_pack':{'command':'ls','argvs':'./'}}]
    return task_list
