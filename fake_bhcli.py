#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" unittest for bhcli helper
"""

__author__ = 'guoshaodan01@baidu.com (Guo Shaodan)'

import os
import sys
import json
import getopt

if __name__ == "__main__":
    if sys.argv[1] == 'machine':
        if sys.argv[2] == 'get-capable':
            #res = """
            # {"resource_token":"rm_token-0000000023", "machine_spec":[{"hostname":"yf-pa-bsvip179-0-j2-off.yf01", "cpu_model":"E5645", "cpu_core_physical":24, "cpu_core_normalized":1500, "memory":48, "disk_size":1200, "disk_count":5, "ssd_size":0, "ssd_count":0, "disk_paths":["\/home", "\/ssd4", "\/ssd1", "\/ssd2", "\/ssd3"], "size_per_disk":[800, 100, 100, 100, 100], "ssd_paths":[], "size_per_ssd":[], "nic_bandwidth":125, "idc":"yf01", "failure_domain":"F10.36.102", "service_unit":"jx", "state":"online", "listening_port":8765, "time_to_dead":0}, {"hostname":"yf-pa-bsvip253-0-j2-off.yf01", "cpu_model":"E5645", "cpu_core_physical":24, "cpu_core_normalized":1500, "memory":48, "disk_size":1200, "disk_count":5, "ssd_size":0, "ssd_count":0, "disk_paths":["\/home", "\/ssd4", "\/ssd1", "\/ssd2", "\/ssd3"], "size_per_disk":[800, 100, 100, 100, 100], "ssd_paths":[], "size_per_ssd":[], "nic_bandwidth":125, "idc":"yf01", "failure_domain":"F10.36.102", "service_unit":"jx", "state":"online", "listening_port":8765, "time_to_dead":0}], "machine_resource_remain":[{"cpu_core_normalized":1500, "memory":48, "disk_size":1200, "disk_count":5, "ssd_size":0, "ssd_count":0, "disk_paths":["\/home", "\/ssd4", "\/ssd1", "\/ssd2", "\/ssd3"], "size_per_disk":[800, 100, 100, 100, 100], "ssd_paths":[], "size_per_ssd":[], "nic_bandwidth_in":125, "nic_bandwidth_out":125}, {"cpu_core_normalized":1500, "memory":48, "disk_size":1200, "disk_count":5, "ssd_size":0, "ssd_count":0, "disk_paths":["\/home", "\/ssd4", "\/ssd1", "\/ssd2", "\/ssd3"], "size_per_disk":[800, 100, 100, 100, 100], "ssd_paths":[], "size_per_ssd":[], "nic_bandwidth_in":125, "nic_bandwidth_out":125}], "related_container_groups":["bs_fake_0"]}
            # """
            mac_spec = """
            {"hostname":"szjjh-testenv0.szjjh01", "cpu_model":"E5620", "cpu_core_physical":2, "cpu_core_normalized":1150, "memory":32, "disk_size":10800, "disk_count":12, "ssd_size":0, "ssd_count":0, "disk_paths":["\/home", "\/home\/disk1", "\/home\/disk3", "\/home\/disk4", "\/home\/disk5", "\/home\/disk6", "\/home\/disk7", "\/home\/disk8", "\/home\/disk9", "\/home\/disk10", "\/home\/disk11", "\/home\/disk2"], "size_per_disk":[900, 900, 900, 900, 900, 900, 900, 900, 900, 900, 900, 900], "ssd_paths":[], "size_per_ssd":[], "nic_bandwidth":125, "idc":"szjjh01", "failure_domain":"10.226.89", "service_unit":"hd", "state":"online", "listening_port":20, "time_to_dead":100, "label":["index"]}
            """ 
            res = {"resource_token":"rm_token-0000000023", "machine_spec":[], "machine_resource_remain":[], "related_container_groups":[]}
            i = 0
            while i < 300:
                ms = json.loads(mac_spec)
                ms["hostname"] = "szjjh-testenv%d.szjjh01" % i
                res["machine_spec"].append(ms)
                res["machine_resource_remain"].append(ms)
                i+=1
            print json.dumps(res)
            sys.exit(0)

    if sys.argv[1] == 'app':
        if sys.argv[2] == 'describe':
            app_id = ""
            opts, args = getopt.getopt(sys.argv[3:], 'h', ['app_id=', 'cluster='])
            for k,v in opts:
                if k == '--app_id':
                    app_id = v
            if app_id == "bs_fake_0":
                desc = open("./bs_desc", "r")
                res = desc.read()
                print res
                sys.exit(0)
            else:
                sys.exit(1)
                
        elif sys.argv[2] == 'add':
            f=open("tmp","a")
            f.write(str(sys.argv))
            opts, args = getopt.getopt(sys.argv[3:], 'h', ['app_id=', 'cluster=', 'containers=', 'data_spec=', 'package_spec=', 'naming_spec=', 'resource_spec=', 'min_usable='])
            opts_d = {}
            for k, v in opts:
                opts_d[k] = v
            f.close()

            #f = open("containers.%s" % opts_d['--app_id'], "w")
            #fi = open(opts_d['--containers'], 'r')
            #f.write(fi.read())
            #f.close()
            #fi.close()
            sys.exit(0)
        else:
            print " ".join(sys.argv)
            
            
    if sys.argv[1] == 'resource':
        if sys.argv[2] == 'describe':
            app_id = ""
            opts, args = getopt.getopt(sys.argv[3:], 'h', ['app_id=', 'cluster='])
            for k, v in opts:
                if k == '--app_id':
                    app_id = v
            if app_id == "bs_fake_0":
                desc = open("./bs_desc", "r")
                res = desc.read()
                print res
                sys.exit(0)
            else:
                sys.exit(1)
    
