#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" unittest for service_manager
"""

__author__ = 'guoshaodan01@baidu.com (Guo Shaodan)'

import am_conf
import am_log
import am_global
import os
import sys
import opzk
import service_manager as SM

if __name__ == "__main__":
    ret = am_log.InitLogger(am_global.log_dir)
    if ret != 0:
        print "[FATAL] InitLogger failed"
        sys.exit(1)
    ret = am_conf.LoadConf(os.path.join(am_global.conf_dir, am_global.conf_file))
    if ret != 0:
        print "[FATAL] LoadConf failed"
        sys.exit(1)


    #sm = SM.ServiceManager("10.38.189.15:1181,10.38.189.15:2181", "am_test")
    sm = SM.ServiceManager(am_conf.g_conf.zk_server, am_conf.g_conf.zk_root)
    print am_conf.g_conf.zk_server, am_conf.g_conf.zk_root

    #test filler
    print "test to fill cache"
    #t = opzk.ZKCacheFiller("zk_cache_filler", sm.zkcli)
    #t.start()
    #t.join()


    print "to init service manager"
    sm.Init()
    if ret != 0:
        print "Init service manager from zookeeper failed"
        sys.exit(1)
    print "init service_manager sucessfully"
    ret, applist = sm.ListAppByService('bs-testenv')
    print ret


    ret = sm.AddService("bs");
    print ret
    ret = sm.AddApp("bs_vip_0", "bs")
    print ret
    ret = sm.AddApp("bs_vip_1", "bs")
    print ret
    ret = sm.AddApp("bs_se_1", "bs")
    print ret
    ret, applist = sm.ListAppByService("bs")
    print ret, applist
    meta_all = {"package":{"package_id":"bs_vip_3", "module_name":"bs","data_source":"ftp://cq01-ps-pa-rdtest3.cq01//home/work/bs_output2/"}, "dynamic_data":{}, "resource":{"container_resource":{"memory_size":5}}, "naming":{}}
    ret = sm.AppSetMetaAll("bs_vip_0", "bs", meta_all)
    print ret
    ret, meta = sm.AppGetMetaAll("bs_vip_0", "bs")
    print ret, meta
    ret = sm.AppSetMetaAttr("bs_vip_0", "bs", "resource.container_resource.workspace_size", 6)
    print ret
    ret, value = sm.AppGetMetaAttr("bs_vip_0", "bs", "resource.container_resource.workspace_size")
    print ret, value
    ret, meta = sm.AppGetMetaAll("bs_vip_0", "bs")
    print ret, meta

    print "get package"
    ret, spec = sm.AppGetSpec("package", "bs_vip_0", "bs")
    print ret, spec
    print "get data"
    ret, spec = sm.AppGetSpec("dynamic_data", "bs_vip_0", "bs")
    print ret, spec
    print "get resource"
    ret, spec = sm.AppGetSpec("resource", "bs_vip_0", "bs")
    print ret, spec
    print "get naming"
    ret, spec = sm.AppGetSpec("naming", "bs_vip_0", "bs")
    print ret, spec

    print "get all spec"
    ret, spec = sm.AppGetSpecAll("bs_vip_0", "bs")
    print ret, spec

    print "app_group"
    ret = sm.AddAppGroup("bs_vip", "bs")
    print ret
    ret = sm.AppGroupAddApp("bs_vip", "bs", "bs_vip_0")
    print ret
    ret = sm.AppGroupAddApp("bs_vip", "bs", "bs_vip_1")
    print ret
    ret = sm.AppGroupAddApp("bs_vip", "bs", "bs_vip_2")
    print ret
    ret, applist = sm.ListAppByAppGroup("bs", "bs_vip")
    print ret, applist
    ret = sm.DelAppGroup("bs_vip", "bs")
    print ret
    ret, applist = sm.ListAppByAppGroup("bs", "bs_vip")
    print ret, applist

    print "del"
    ret = sm.DelApp("bs_vip_0", "bs")
    print ret
    ret = sm.DelService("bs")
    print ret
    ret, applist = sm.ListAppByService("bs")
    print ret, applist
    ret = sm.DelApp("bs_se_1", "bs")
    print ret
    ret = sm.DelService("bs")
    print ret



#TODO 
