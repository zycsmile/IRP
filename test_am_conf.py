#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" Utility that makes placements for index partitions into a cluster. 
"""

__author__ = 'guoshaodan01@baidu.com (Guo Shaodan)'

import am_conf
import am_log
import am_global
import os
import sys

if __name__ == "__main__":
    ret = am_log.InitLogger(am_global.log_dir)
    if ret != 0:
        print "[FATAL] InitLogger failed"
        sys.exit(1)
    ret = am_conf.LoadConf(os.path.join("no", "such", "file"))
    if ret == 0:
        print "[FATAL] Load a nonexisted conf"
    ret = am_conf.LoadConf(os.path.join(am_global.conf_dir, am_global.conf_file))
    if ret != 0:
        print "[FATAL] LoadConf failed"
    else:
        print "[%s]" % am_conf.g_conf.zk_server
        print "[%s]" % am_conf.g_conf.zk_root
        print "[%s]" % am_conf.g_conf.bh_loc
        print "[%s]" % am_conf.g_conf.bh_cluster


    print "[OK]"

    
    
