#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" Utility that makes placements for index partitions into a cluster. 
"""

__author__ = 'guoshaodan01@baidu.com (Guo Shaodan)'

import logging
import am_task
import json

import bs_scheduler

class CheckStateTask(am_task.AMTask):
    def __init__(self, task_name, context, options, service_id, app_id=None, app_group_id=None):
        super(CheckStateTask, self).__init__(task_name, context, options, service_id, app_id, app_group_id)

    def __is_null(self, value):
        return (value == None or len(value) == 0)
            
    def run(self):
        #TODO:make a tag on zk
        logging.info("begin to exec task, task_name=[%s]", self.task_name_)
        self.app_id_

        #determine scope
        app_id_list = []
        if self.__is_null(self.app_group_id_) and self.__is_null(self.app_id_):
            ret, app_id_list = self.context_.service_manager_.ListAppByService(self.service_id_)
        elif not self.__is_null(self.app_group_id_):
            ret, app_id_list = self.context_.service_manager_.ListAppByAppGroup(self.service_id_, self.app_group_id_)
        if not self.__is_null(self.app_id_) and self.app_id_ not in app_id_list:
            app_id_list.append(self.app_id_)
        
        #get spec ignore non-existing app
        app_spec = None
        app_to_check = {}

        bh_app = self.context_.bhcli_.get_app("")
        app_descr_results = bh_app.describe_batch(app_id_list)
        check_result = {"total_app":0, "not_add":0, "need_to_restore":0, "error": 0, "app_not_add":[], "app_to_restore": [], "app_error":[]}
        check_result["total_app"] = len(app_id_list)
        for app_id, result in app_descr_results.iteritems():
            if result[0] != 0:
                check_result["not_add"] += 1
                check_result["app_not_add"].append(app_id)
                continue

            #get spec
            ret,goal_resource_spec = self.context_.service_manager_.AppGetSpec("resource", app_id, self.service_id_)
            if ret != 0:
                check_result["error"] += 1
                check_result["app_error"].append(app_id)
                logging.warning("get spec of app failed, skipped, task_name=[%s], app_id=[%s]", self.task_name_, app_id)
                continue

            #get resource-spec
            #TODO: tag
            app_descr = json.loads(result[1])
            app_spec = app_descr["app_spec"]
            num_online = 0
            num_offline = 0
            for app_inst in app_descr["app_instance"]:
                container = app_inst["container_instance"]
                state = app_inst["runtime"]["run_state"]
                if state == "STOP" or state == "DESTROY" or container["state"] == "offline":
                    num_offline += 1
                else:
                    num_online += 1

            if num_online < goal_resource_spec["total_containers"]:
                check_result["need_to_restore"] += 1
                check_result["app_to_restore"].append(app_id)

        return 0, json.dumps(check_result)





