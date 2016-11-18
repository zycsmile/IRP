#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""method that read global conf
"""

__author__ = 'guoshaodan01@baidu.com (Guo Shaodan)'

import ConfigParser
import logging

g_conf = None

class AmConf(object):
    def __init__(self, parser):

        self.zk_server = parser.get("zookeeper", "zk_server")
        logging.info("got conf zookeeper.zk_server: [%s]", self.zk_server)
        self.zk_root = parser.get("zookeeper", "zk_root")
        logging.info("got conf zookeeper.zk_root: [%s]", self.zk_root)
        self.zk_user = parser.get("zookeeper", "zk_username")
        logging.info("got conf zookeeper.zk_user: [%s]", self.zk_user)
        self.zk_pass = parser.get("zookeeper", "zk_password")
        logging.info("got conf zookeeper.zk_pass: [%s]", self.zk_pass)

        self.bh_loc = parser.get("bhcli", "bh_location")
        logging.info("got conf bhcli.bh_location: [%s]", self.bh_loc)
        self.bh_cluster = parser.get("bhcli", "bh_cluster")
        logging.info("got conf bhcli.bh_cluster: [%s]", self.bh_cluster)
        self.matrix_cluster = parser.get("bhcli", "matrix_cluster")
        logging.info("got conf matrix_cluster: [%s]", self.matrix_cluster)
        self.bh_debug = parser.getint("bhcli", "debug")
        logging.info("got conf bhcli.debug: [%d]", self.bh_debug)

        self.task_timeout = {}
        for k, v in parser.items("task_timeout_sec"):
            self.task_timeout[k] = int(v)
            logging.info("got conf task_timeout_sec.%s: [%d]", k, self.task_timeout[k])
        #default is necessary
        self.task_timeout["default"] = parser.getint("task_timeout_sec", "default")
        logging.info("got conf task_timeout_sec.default: [%d]", self.task_timeout['default'])

        self.check_task_interval = parser.getint("task_manager", "check_task_interval")
        logging.info("got conf task_manager.check_task_interval: [%d]", self.check_task_interval)

def LoadConf(conf_file):
    global g_conf
    ret = 0
    conf_parser = ConfigParser.ConfigParser()
    try:
        ret = conf_parser.read(conf_file)
    except Exception as e:
        logging.error("read config file failed: config_file=[%s]", conf_file)
        return 1
    if len(ret) == 0:
        logging.error("read config file failed: config_file=[%s]", conf_file)
        return 1

    logging.info("read conf file successfully, config_file=[%s]", conf_file)
    try:
        g_conf = AmConf(conf_parser)
    except Exception, e:
        logging.error("read config file failed: config_file=[%s], error=[%s]", conf_file, e)
        return 1
    logging.info("read conf key-value successfully, config_file=[%s]", conf_file)

    return 0

