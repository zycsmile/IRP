#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# minlingang@baidu.com
# 2014/09/08
#

import sys
import json
import traceback
import bs_scheduler_task as bst

# get app_id list
# /home/work/beehive/env/appmaster-m14/bin/zkcli
# zkcli node list  --server='10.207.49.44:2181' --path='/beehive/appmaster/service/bs/app'

rebalance_default_options = """{
        "BS_PORT_RANGE":16,
        "BS_SERVICE_BASE_PORT":11681,
        "NUM_BS_SLOT_PER_NODE":200,
        "action_file":"",
        "allow_move_group":false,
        "break_up_dist":8,
        "check_plan":true,
        "commit_interval_sec":1200,
        "index_cross_idc" : ["bs_vip", "bs_se"],
        "continue_index_type":[
            "bs_wdna",
            "bs_wdnb",
            "bs_wp",
            "bs_tiebadaily",
            "bs_dailywk",
            "bs_dailyzhidao",
            "bs_hourwk",
            "bs_hourzd",
            "bs_dnews",
            "bs_dnews-se",
            "bs_dnews-weibo"
        ],
        "cpu_idle_cross_real_idc":0.2,
        "cpu_rate":100,
        "debug":false,
        "exclude_error_machine":true,
        "group_guide":{},
        "index_conflict_to_avoid":{},
        "manual_connection":true,
        "manual_delete":true,
        "media":"ssd",
        "output_format":"bash",
        "output_index_distr":{},
        "ratio_cross_real_idc":0.1,
        "service_id":"bs",
        "stable_index_types":{},
        "storage_style":"share",
        "disable_empty_storage":true,
        "offline_mode":true,
        "reserve_mem_percent": 0.0,
        "skip_reconfig": {"cpu":0}
        }"""

tool = bst.SchedulerTaskHelper("task_debug", None)

dump_file_name = "./data_debug/dump-file-empty"
app_from_file = "./data_debug/app_list"
plan_file = "./data_debug/plan-file-empty"
sim_file = "./data_debug/sim-file-empty"

def build_options(op_type):
    try:
        options = json.loads(rebalance_default_options)
        return options
    except:
        print "build_options failed"
        print traceback.print_exc()
        exit(1)

def build_app_list(op_type):
    app_list = []
    with open(app_from_file, "r") as fin:
        for item in fin:
            app_list.append(item.strip())
    return app_list

def build_dump_info(op_type):
    ret, all_apps, all_macs = tool.load_dump_file(dump_file_name)
    if ret != 0:
        print "failed to load dump fail:%s" % (dump_file_name, )
        exit(1)
    return (all_apps, all_macs)


def dump_res_file(all_apps, all_machines, dump_file):
    """
        dump res file
    """
    dump_dict = {}
    dump_dict['machine'] = all_machines
    dump_app = {}
    for app_id, app_info in all_apps.iteritems():
        dump_app[app_id] = app_info.to_dict()
    dump_dict['app'] = dump_app
    dump_str = json.dumps(dump_dict, sort_keys=True, indent=4, separators=(',', ':'))
    try:
        ref_dump_file = open(dump_file, 'w')
        ref_dump_file.write(dump_str)
        ref_dump_file.close()
    except Exception as e:
        logging.warning('write dump file failed, file=[%s], error=[%s]', dump_file, e)
        return 1
    return 0

def main():

    global dump_file_name
    global app_from_file
    global plan_file
    global sim_file
    global res_file
    global alloc_file
    res_file = None

    dump_file_name = sys.argv[1]
    if len(sys.argv) == 4:
        plan_file = sys.argv[2]
        sim_file = sys.argv[3]
        #app_to_scheduler = build_app_list(op_type)
        app_to_scheduler = []
    elif len(sys.argv) == 2:
        plan_file = dump_file_name + ".plan"
        sim_file = dump_file_name + ".sim"
        simulate_check_file = dump_file_name + '.simlate_check_file'
        res_file = dump_file_name + ".res"
        alloc_file = dump_file_name + ".alloc"
        app_to_scheduler = []
    else:
        pass

    op_type = "rebalance"
    #op_type = "apply"

    options = build_options(op_type)
    all_apps, all_macs = build_dump_info(op_type)
    run_mode = op_type
    all_apps = tool.modify_app_resource_spec(all_apps, options)
    action_list, err_msg = tool.step_generate_plan( \
                            all_apps,
                            all_macs,
                            options,
                            plan_file,
                            sim_file,
                            simulate_check_file,
                            run_mode,
                            app_to_scheduler)

    if action_list is None:
        print "failed to generate plan, err_msg:%s" % (err_msg, )
    if res_file:
        options["media"] = "ssd"
        tool.step_apply_actions(action_list, options, all_apps, alloc_file, 
                True, options['offline_mode'])
        ret = dump_res_file(all_apps, all_macs, res_file)
        if ret != 0:
            print "failed to dump res file"

if __name__ == "__main__":
    main()
