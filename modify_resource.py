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
import traceback


resource_list = ['cpu', 'mem', 'ssd']

def build_resource_map(file_name, resource):
    if resource == 'cpu':
        pattern = re.compile(r'(?P<type>b\w_[\w\-_]+) .*cpu=(?P<cpu>\d+)')
    if resource == 'mem':
        pattern = re.compile(r'(?P<type>b\w_[\w\-_]+) .*mem=(?P<mem>\d+)')
    if resource == 'ssd':
        pattern = re.compile(r'(?P<type>b\w_[\w\-_]+) .*ssd=(?P<ssd>\d+)')
    resource_map = {}
    with open(file_name, 'r') as file_handler:
        for line in file_handler:
            match = pattern.match(line)
            if match:
                resource_map[match.group('type')] = int(float(match.group(resource)))
    print resource_map
    return resource_map


if __name__ == '__main__':
    filename = sys.argv[2]
    resource_map = {}
    for i in resource_list:
        resource_map[i] = build_resource_map(filename, i)
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
        for i in resource_list:
            if resource_map[i] == {}:
                continue
            for app_id, app_info_dict in json_obj['app'].iteritems():
                print app_id
                index_type = app_id[:app_id.rfind("_")]
                if index_type in resource_map[i]:
                    if 'bs' in index_type:
                        if i == 'cpu':
                            json_obj['app'][app_id]["goal_resource_spec"]["resource"]['cpu']['numCores'] = int(resource_map[i][index_type])
                        elif i == 'mem':
                            json_obj['app'][app_id]["goal_resource_spec"]["resource"]['memory']['sizeMB'] = int(resource_map[i][index_type])
                        elif i == 'ssd':
                            json_obj['app'][app_id]["goal_resource_spec"]["resource"]['disks']['dataSpace']['sizeMB'] = int(resource_map[i][index_type])

                    elif 'bc' in index_type:
                        if i == 'cpu':
                            json_obj['app'][app_id]["resource_spec"]["resource"]['cpu']['numCores'] = int(resource_map[i][index_type])

    except Exception as e:
        print e
        traceback.print_exc()
        print "app change error"
        sys.exit()
    new_file_name = "new_" + dump_file_name
    new_file_content=json.dumps(json_obj, indent=4)
    new_file_handler = open(new_file_name, 'w')
    new_file_handler.write(new_file_content)
    new_file_handler.close()

