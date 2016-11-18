#!/usr/bin/env python
########################################################################
# 
# Copyright (c) 2014 Baidu.com, Inc. All Rights Reserved
# 
########################################################################
 
'''
File: bs_commit_delete_task.py
Author: guoshaodan01(guoshaodan01@baidu.com)
Date: 2014/03/28 09:33:40
'''

import logging
import bs_scheduler_task


def exec_task(ref_task_inst, context):
    try:
        options = ref_task_inst.get_options()
    except Exception as e:
        logging.warn('get options failed, err=[%s], task_instance_id=[%s]', e, ref_task_inst.id())
        return -1
    if 'service_id' not in options:
        logging.warn('service_id not found, task_instance_id=[%s]', ref_task_inst.id())
        return -1
    if type(options['force_delete_unvisible']) is not bool:
        logging.warn('force_delete_unvisible should be bool(true/false), task_instance_id=[%s]', ref_task_inst.id())
        return -1

    ret, app_id_list = context.service_manager_.ListAppByService(options['service_id'])
    if ret != 0:
        logging.warn('get app list failed, service_id=[%s], task_instance_id=[%s]', options['service_id'], ref_task_inst.id())
        return -1

    helper = bs_scheduler_task.SchedulerTaskHelper(ref_task_inst.id(), context)
    ret = helper.step_commit_delete(app_id_list, service_id=options['service_id'], force_delete_unvisible=options['force_delete_unvisible'])

    return ret
