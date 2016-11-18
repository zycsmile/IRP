#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" Copyright (c) 2013 Baidu.com, Inc. All Rights Reserved
"""

""" init log
"""

__author__ = 'guoshaodan01@baidu.com (Guo Shaodan)'

import os
import logging
import logging.handlers

def init_log(log_path, level=logging.INFO, when="D", backup=7, 
        format="%(levelname)s: %(asctime)s: %(funcName)s:%(filename)s:%(lineno)d * %(thread)d %(message)s",
             datefmt="%m-%d %H:%M:%S"):
    """
    init_log - initialize log module
    
    Args:
      log_path      - Log file path prefix. 
                      Log data will go to two files: log_path.log and log_path.log.wf
                      Any non-exist parent directories will be created automatically
      level         - msg above the level will be displayed
                      DEBUG < INFO < WARNING < ERROR < CRITICAL
                      the default value is logging.INFO
      when          - how to split the log file by time interval
                      'S' : Seconds
                      'M' : Minutes
                      'H' : Hours
                      'D' : Days
                      'W' : Week day
                      default value: 'D'
      format        - format of the log
                      default format:
                      %(levelname)s: %(asctime)s: %(filename)s:%(lineno)d * %(thread)d %(message)s
                      INFO: 12-09 18:02:42: log.py:40 * 139814749787872 HELLO WORLD
      backup        - how many backup file to keep
                      default value: 7
    """
    formatter = logging.Formatter(format, datefmt)
    logger = logging.getLogger()
    logger.setLevel(level)

    dir = os.path.dirname(log_path)
    if not os.path.isdir(dir):
        os.makedirs(dir)

    #handler = logging.handlers.TimedRotatingFileHandler(log_path + ".log", 
    #                                                    when=when, 
    #                                                    backupCount=backup)
    
    handler = logging.FileHandler(log_path + ".log")
    handler.setLevel(level)
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    handler = logging.FileHandler(log_path + ".log.wf") 
    handler.setLevel(logging.WARNING)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return 0

def setup_logger(logger_name, log_file, level=logging.INFO, 
        format="%(levelname)s: %(asctime)s: %(funcName)s:%(filename)s:%(lineno)d * %(thread)d %(message)s",
        datefmt="%m-%d %H:%M:%S", when='D', backup=7):

    l = logging.getLogger(logger_name)
    formatter = logging.Formatter(format, datefmt)

    file_handler = logging.FileHandler(log_file, mode='a')
    file_handler = logging.handlers.TimedRotatingFileHandler(log_file, when=when, backupCount=backup)
    file_handler.setFormatter(formatter)

    l.setLevel(level)
    l.addHandler(file_handler)
