#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# minlingang@baidu.com
# 2014/09/08
#

import sys
import os
import logging
import traceback
import threading
import json
import service_manager as SM
import am_global as AG
import am_conf as AC
import am_task as AT
import bhcli_helper as BH
import bc_deploy_task
import check_state_task
import debug_task
import am_task_manager
import level_manager
import am_log as LOG
import bs_scheduler_task as BST

# change global config

AG.zkcli_path = "/home/work/beehive/env/appmaster-m14/bin/zkcli"
AG.tmp_dir = "./data/tmp/"
AG.data_dir = "./data/"

g_zk_server = "127.0.0.1:2181"
g_zk_root = "/beehive/appmaster_M14_V4"
g_zk_user = "beehive"
g_zk_pass = "beehive_token"
g_bhcli_path = "/home/work/beehive/env/shell-m14/bin/bhcli"
g_bhcli_cluster = "bj"
g_service_id = "testenv-bcb"

def mock_am_conf():
    class MockAmConf(object):
        def __init__(self, zks, zkr, bhl, bhc):
            self.zk_server = zks
            self.zk_root = zkr
            self.bh_loc = bhl
            self.bh_cluster = bhc
            self.bh_debug = 0
            self.task_timeout = {}
            self.task_timeout["default"] = 86400
            self.task_timeout["bs_scheduler"] = 604800
            self.check_task_interval = 120
    AC.g_conf = MockAmConf(g_zk_server, g_zk_root, g_bhcli_path, g_bhcli_cluster)

def main():
    if len(sys.argv) != 2:
        print "USAGE: python %s <dump-file>" % (sys.argv[0], )
        return

    dump_file = sys.argv[1]

    mock_am_conf()

    service_mgr = SM.ServiceManager(g_zk_server, g_zk_root, g_zk_user, g_zk_pass)
    bh_mgr = BH.BHManager(g_bhcli_path, g_bhcli_cluster)
    task_context = AT.TaskContext(service_mgr, bh_mgr)
    
    tool = BST.SchedulerTaskHelper("task_debug", task_context)
    ret, err_msg, all_apps, all_machines, all_resource = tool.step_dump(g_service_id, dump_file)
    if ret != 0:
        print "FAIL:%s" % (err_msg, )
    else:
        print "SUCC"


if __name__ == "__main__":
    try:
        main()
    except:
        print traceback.print_exc()
