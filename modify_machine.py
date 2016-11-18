#!/usr/bin/env python
########################################################################
# 
# Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved
# 
########################################################################
 
'''
File: change_dump.py
Author: work(work@baidu.com)
Date: 2015/11/10 15:59:41
'''
import sys
import json
import re


def build_resource_map(file_name):
    offline_mac_list = []
    con = open(file_name, 'r')
    for line in con:
        offline_mac_list.append(line.strip())
    return offline_mac_list


if __name__ == '__main__':
    filename = sys.argv[2]
    offline_list = build_resource_map(filename)
    dump_file_name = sys.argv[1]
    try:
        dump_file_handler = open(dump_file_name, 'r')
    except Exception as e:
        print "open dump file failed"
        sys.exit()
    try:
        json_obj = json.loads(dump_file_handler.read())
    except Exception as e:
        print "json load error"
        sys.exit()
    try:
        for mac in json_obj['machine']:
            if mac['hostId'] in offline_list:
                mac['hostInfo']['state'] = 3
    except Exception as e:
        print e
        print "app change error"
        sys.exit()
    new_file_name = "new_" + dump_file_name
    new_file_content=json.dumps(json_obj, indent=4)
    new_file_handler = open(new_file_name, 'w')
    new_file_handler.write(new_file_content)
    new_file_handler.close()

