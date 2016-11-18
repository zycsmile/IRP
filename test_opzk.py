#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" unittest for bhcli helper
"""

__author__ = 'guoshaodan01@baidu.com (Guo Shaodan)'

import am_conf
import am_log
import am_global
import os
import sys
import opzk

if __name__ == "__main__":
    ret = am_log.InitLogger(am_global.log_dir)
    if ret != 0:
        print "[FATAL] InitLogger failed"
        sys.exit(1)
    ret = am_conf.LoadConf(os.path.join(am_global.conf_dir, am_global.conf_file))
    if ret != 0:
        print "[FATAL] LoadConf failed"
        sys.exit(1)


    zkcli = opzk.ZkcliHelper("./zkcli", "10.38.189.15:1181,10.38.189.15:2181", 
            "am_test", "beehive", "beehive_token")
    ret, out = zkcli.List("")
    print ret, out
    ret, out = zkcli.Set("", '{"root":"am"}')
    print ret, out
    ret, out = zkcli.Get("")
    print ret, out
    ret, out = zkcli.Set("a", "{}")
    print ret, out   
    ret, out = zkcli.Set("a/b", "{}")
    print ret, out   
    ret, out = zkcli.List("a")
    print ret, out
    ret, out = zkcli.DelR("")
    print ret, out
    ret, out = zkcli.List("")
    print ret, out
  
