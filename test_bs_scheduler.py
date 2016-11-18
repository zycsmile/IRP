#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" unittest for bhcli helper
"""

__author__ = 'guoshaodan01@baidu.com (Guo Shaodan)'

import am_conf
import json
import am_log
import am_global
import os
import sys
import bs_scheduler

def test_merge(default_options):
    default_options["run_mode"] = "tm"
    default_options["kkk"] = "vvv"

    return default_options
 
if __name__ == "__main__":
    ret = am_log.InitLogger(am_global.log_dir)
    if ret != 0:
        print "[FATAL] InitLogger failed"
        sys.exit(1)
    ret = am_conf.LoadConf(os.path.join(am_global.conf_dir, am_global.conf_file))
    if ret != 0:
        print "[FATAL] LoadConf failed"
        sys.exit(1)

    sch_options = test_merge(bs_scheduler.default_options)
    print sch_options["run_mode"]
    print sch_options["kkk"]
    print sch_options["output_format"]

    mac_spec_str = """
           {"hostname":"szjjh-testenv0.szjjh01", "cpu_model":"E5620", "cpu_core_physical":2, "cpu_core_normalized":1150, "memory":32, "disk_size":10800, "disk_count":12, "ssd_size":0, "ssd_count":0, "disk_paths":["\/home", "\/home\/disk1", "\/home\/disk3", "\/home\/disk4", "\/home\/disk5", "\/home\/disk6", "\/home\/disk7", "\/home\/disk8", "\/home\/disk9", "\/home\/disk10", "\/home\/disk11", "\/home\/disk2"], "size_per_disk":[900, 900, 900, 900, 900, 900, 900, 900, 900, 900, 900, 900], "ssd_paths":[], "size_per_ssd":[], "nic_bandwidth":125, "idc":"szjjh01", "failure_domain":"10.226.89", "service_unit":"hd", "state":"online", "listening_port":20, "time_to_dead":100, "label":["index"]}
           """
    cg_spec_str = """
{"owner":"", "container_group_id":"bs-testenv_vip_0", "container_resource":{"workspace_size":3, "normalized_cpu_cores":50, "memory_size":2, "disk_size":57, "dedicated_disk_count":0, "ssd_size":0, "dedicated_ssd_count":0, "nic_bandwidth_in":5, "nic_bandwidth_out":5, "listening_port_count":20, "dedicated_server":false}, "total_containers":1, "max_container_per_machine":1, "max_container_per_failure_domain":1, "max_container_per_idc":0, "max_container_per_service_unit":0, "exclude_failure_domains":[], "include_failure_domains":[], "exclude_idc":[], "include_idc":[], "exclude_service_unit":[], "include_service_unit":[], "work_path_keyword":"bs"}
   """

    mac_spec = json.loads(mac_spec_str)
    cg_spec1 = json.loads(cg_spec_str)
    cg_spec2 = json.loads(cg_spec_str)

    cg_spec1["include_machine_label"] = [ "index" ]
    cg_spec2["include_machine_label"] = [ "index" ]
    app_id1 = "bs-testenv_vip_0"
    app_id2 = "bs-testenv_se_0"
    cg_spec1["container_group_id"] = app_id1
    cg_spec2["container_group_id"] = app_id2

    placement_manager = bs_scheduler.IndexPlacementManager(bs_scheduler.default_options)
    print "to add machine to scheduler"
    node = placement_manager.create_slavenode_from_bh_spec(mac_spec, mac_spec)
    if node == None:
        print "add machine failed"
        sys.exit(1)

    print "to add index partition"
    partition_id1 = app_id1[app_id1.find('_')+1:]
    ref_partition = placement_manager.create_index_partition_from_bh_spec(partition_id1, cg_spec1, "group_1", "layer1")
    if ref_partition == None:
        print "create index partition failed"
    partition_id2 = app_id2[app_id2.find('_')+1:]
    ref_partition = placement_manager.create_index_partition_from_bh_spec(partition_id2, cg_spec2, "group_1", "layer1")
    if ref_partition == None:
        print "create index partition failed"

    placement_manager.allocate(partition_list=[partition_id1, partition_id2], slavenode_list=None)
    placement_manager.compress_action_list()
    placement_manager.allocate_slots()
    placement_manager.allocate_storage()



   
