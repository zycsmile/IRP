#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" unittest for bhcli helper
"""

__author__ = 'guoshaodan01@baidu.com (Guo Shaodan)'

import am_conf
import logging
import am_global
import os
import sys
import bc_deploy_task
import traceback
import service_manager
import bhcli_helper
import am_task
import threading

class TestDeploy:
    def __init__(self):
        ret = LOG.init_log(am_global.log_prefix)
        if ret != 0:
            print "[FATAL] InitLogger failed"
            sys.exit(1)
        ret = am_conf.LoadConf(os.path.join(am_global.conf_dir, am_global.conf_file))
        if ret != 0:
            print "[FATAL] LoadConf failed"
            sys.exit(1)

        try:
            self.__service_mgr = service_manager.ServiceManager(am_conf.g_conf.zk_server, am_conf.g_conf.zk_root)
        except Exception, e:
            print "init service_mananger failed"
            print e
            traceback.print_exc()
            sys.exit(1)
        print "init service_manager sucessfully"
        self.__service_mgr.Init()

        #TODO: move to upper level
        try:
            bh_mgr = bhcli_helper.BHManager("./fake_bhcli.py", am_conf.g_conf.bh_cluster)
            #bh_mgr = bhcli_helper.BHManager(am_conf.g_conf.bh_loc, am_conf.g_conf.bh_cluster)
            logging.info("init bhcli manager successfully")
            self.task_context_ = am_task.TaskContext(self.__service_mgr, bh_mgr)
            logging.info("init task context successfully")
            self.queue_lock_ = threading.Lock()
            self.task_queue_ = {}
            logging.info("init task lock and queue successfully")
        except Exception, e:
            print "init task context failed"
            print e
            print traceback.print_exc()
            sys.exit(1)

    def run(self):
        task = bc_deploy_task.BcDeployTask("bc_deploy_task.test", self.task_context_, service_id='bc', options=None)
        ret = task.run()
        print "[FINISH] %d" % ret

if __name__ == "__main__":
    t = TestDeploy()
    t.run()


    
    
