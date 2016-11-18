#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" unittest for bhcli helper
"""

__author__ = 'guoshaodan01@baidu.com (Guo Shaodan)'

import json
import os
import sys

import bhcli_helper
import am_conf
import am_log
import am_global

if __name__ == "__main__":
    ret = am_log.init_log(os.path.join(am_global.log_dir, "test"))
    if ret != 0:
        print "[FATAL] InitLogger failed"
        sys.exit(1)
    conf_files = list()
    conf_files.append(os.path.join(am_global.conf_dir, "am.conf"))
    conf_files.append(os.path.join(am_global.conf_dir, "ip.conf.sample"))
    conf_files.append(os.path.join(am_global.conf_dir, "bhcli.conf.sample"))
    ret = am_conf.LoadConf(conf_files)
    if ret != 0:
        print "[FATAL] LoadConf failed"
        sys.exit(1)

    bh_mgr = bhcli_helper.BHManager(am_conf.g_conf.bh_loc, am_conf.g_conf.bh_cluster)

    #describe app
    app_id = "bs_vip_0"
    bh_app = bh_mgr.get_app(app_id)
    ret, app_descr = bh_app.describe()
    print ("describe app finished, app_id=[%s], ret=[%d]") % (app_id, ret)
    
    bh_resource = bh_mgr.get_resource(app_id)
    ret, group_descr = bh_resource.describe_container_group(app_id)
    print ("describe resource finished, app_id=[%s], ret=[%d]") % (app_id, ret)
    container_group = json.loads(group_descr)

    ret, out = bh_resource.update_container_group(app_id, container_group)
    print ("update resource finished, app_id=[%s], ret=[%d]") % (app_id, ret)

    ssd = container_group["container_group_spec"]["resource"]["disks"]["dataSpace"]["sizeMB"]
    cpu = container_group["container_group_spec"]["resource"]["cpu"]["numCores"]
    mem = container_group["container_group_spec"]["resource"]["memory"]["sizeMB"]

    ret, out = bh_resource.update_containers(app_id, cpu, mem, ssd)
    print ("update containers finished, app_id=[%s], ret=[%d]") % (app_id, ret)

    #app_spec = {"package":{}, "dynamic_data":{}, "resource":{}, "naming":{}}
    #containers = {"container_instance":[]}
    #ret,out= bh_app.add(app_spec, 0, containers)
    #print "add app successfully, ret=[%d], out=[%s]" % (ret, out)
    #ret,out=bh_app.set_ns_lock(100)
    #print ret, out

    """
    results = bh_app.describe_batch(["bs_fake_0", "bs_2", "bs_3"])
    print results


    bh_machine = bh_mgr.get_machine('')
    and_filters = []
    or_filters = []
    and_filters.append({"attribute_name":"idc", "cmp_operator":"==", "value":"m1"})
    ret, all_machines = bh_machine.get_capable(and_filters, or_filters, 0)
    #print ("get capable machine finished, ret=[%d], result=[%s]") % (ret, all_machines)
    print "[OK]"
    """

    
    
