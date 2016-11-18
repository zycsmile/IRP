#!/usr/bin/env python
########################################################################
# 
# Copyright (c) 2014 Baidu.com, Inc. All Rights Reserved
# 
########################################################################
 
'''
File: debug_task.py
Author: guoshaodan01(guoshaodan01@baidu.com)
Date: 2014/03/28 09:33:40
'''

import sys

import logging



def exec_task(ref_task_inst, context):
    try:
        options = ref_task_inst.get_options()
    except Exception as e:
        logging.warn('get options failed, err=[%s], task_instance_id=[%s]', e, ref_task_inst.id())
        return -1
    if 'service_id' not in options:
        logging.warn('service_id not found, task_instance_id=[%s]', ref_task_inst.id())
        return -1

    for i in range(10):
        logging.info("description=[begin to exec task], task_instance_id=[%s]", ref_task_inst.id())

    logging.info("description=[wait for signal EXIT], task_instance_id=[%s]", ref_task_inst.id())
    check_signal = ref_task_inst.wait('EXIT', 10)

    return 0
