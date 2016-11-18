#!/usr/bin/env python
########################################################################
# 
# Copyright (c) 2014 Baidu.com, Inc. All Rights Reserved
# 
########################################################################
 
'''
File: bs_deploy_task.py
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
    helper = bs_scheduler_task.SchedulerTaskHelper(ref_task_inst.id(), context)
    app_id=None
    app_group_id=None
    if 'app_id' in options:
        app_id = options['app_id']
    if 'app_group_id' in options:
        app_group_id = options['app_group_id']


    task = bs_scheduler_task.BsSchedulerTask(context=context, service_id=options['service_id'], options=options, ref_task_inst=ref_task_inst, scheduler_task_helper=helper, run_mode='deploy', app_id=app_id, app_group_id=app_group_id)
    ret, err = task.run()

    return ret
