#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" Utility that makes placements for index partitions into a cluster. 
"""

__author__ = 'guoshaodan01@baidu.com (Guo Shaodan)'

import logging
import am_task
import json

class BcDeployTask(am_task.AMTask):
    def __init__(self, task_name, context, options, service_id, app_id=None, app_group_id=None):
        super(BcDeployTask, self).__init__(task_name, context, options, service_id, app_id, app_group_id)

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
        
        #get spec ignore existing app
        app_spec = None
        app_to_deploy = {}

        bh_app = self.context_.bhcli_.get_app("")
        app_descr_results = bh_app.describe_batch(app_id_list)
        for app_id, result in app_descr_results.iteritems():
            if result[0] == 0:
                logging.info("app is existed, skipped in deploy task, task_name=[%s], app_id=[%s]", self.task_name_, app_id)
                continue

            #get spec
            ret,tmp_app_spec = self.context_.service_manager_.AppGetSpecAll(app_id, self.service_id_)
            if ret != 0:
                logging.warning("get spec of app failed, skipped, task_name=[%s], app_id=[%s]", self.task_name_, app_id)
                continue
            app_spec = tmp_app_spec
            #TODO: trick for test index
            app_spec["resource"]["container_resource"]["disk_size"] = app_spec["resource"]["container_resource"]["workspace_size"]

            app_to_deploy[app_id] = app_spec

        if len(app_to_deploy) == 0:
            logging.info("no app to deploy, just finish task, task_name=[%s], service_id=[%s], app_group_id=[%s], app_id=[%s]", self.task_name_, self.service_id_, self.app_group_id_, self.app_id_)
            return 0

        logging.info("app to deploy [%d]"%len(app_to_deploy))
        for id,spec in app_to_deploy.iteritems():
            bh_app = self.context_.bhcli_.get_app(id)
            ret,res = bh_app.add(min_usable=0, app_spec=spec, containers=None)
            if ret != 0:
                logging.warning("add app failed, app_id=[%s]", id)
            logging.info("add app, ret=[%d], res=[%s], app_id=[%s]", ret, res, id)
            ret,res = bh_app.set_ns_lock(2000000000)

        return 0



