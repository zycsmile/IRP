#!/usr/bin/env python
########################################################################
# 
# Copyright (c) 2014 Baidu.com, Inc. All Rights Reserved
# 
########################################################################
 
'''
File: bs_scheduler_task.py
Author: guoshaodan01(guoshaodan01@baidu.com)
Date: 2014/03/31 03:35:42
'''
import json
import sys
import time

import logging
import am_task
import bs_scheduler
import am_global
import re
import random
import collections

class AppInfo(object):
    """all info of an app neened in scheduler task
       app_id: app_id in beehive
       added: whether been added into beehive and deployed or not
       app_spec: specs -- package,dynamic_data, resource, naming
       containers: resource placement plan allocated by appmaster
    """
    def __init__(self, app_id=None, added=None, app_spec=None, 
            resource_spec=None, goal_resource_spec=None, 
            containers=None, instance_state=None, 
            group=None, layer=None, managed=False):
        self.app_id = app_id
        self.added = added
        self.app_spec = app_spec
        self.resource_spec = resource_spec
        self.goal_resource_spec = goal_resource_spec
        if containers is None:
            containers = []
        self.containers = containers
        self.group = group
        self.layer = layer
        self.managed = managed
        #since the state NEW/TBD is stored in NS, it is not convenient to handle, has to convert instance_id to (ip,port) whatever
        #{instance_id:{'run_state':state, 'ns_state':state, 'ns_key':(ip, port), 'ns_visible':bool}}
        #ip,port is in type of str
        self.instance_state = instance_state

    def to_dict(self):
        info_dict = {}
        info_dict['app_id'] = self.app_id
        info_dict['added'] = self.added
        info_dict['app_spec'] = self.app_spec
        info_dict['resource_spec'] = self.resource_spec
        info_dict['containers'] = self.containers
        info_dict['group'] = self.group
        info_dict['layer'] = self.layer
        info_dict['managed'] = self.managed
        info_dict['instance_state'] = self.instance_state
        info_dict['goal_resource_spec'] = self.goal_resource_spec
        return info_dict

    def init_from_dict(self, info_dict):
        self.app_id = info_dict['app_id']
        self.added = info_dict['added']
        self.app_spec = info_dict['app_spec']
        self.resource_spec = info_dict['resource_spec']
        self.containers = info_dict['containers']
        self.group = info_dict['group']
        self.layer = info_dict['layer']
        self.managed = info_dict['managed']
        self.instance_state = info_dict['instance_state']
        self.goal_resource_spec = info_dict['goal_resource_spec']

    def add_instance_ns_state(self, state, visible, inst_list):
        if inst_list is None or len(inst_list) == 0:
            return 0
        for (ip, port) in inst_list:
            inst_id = self.get_instance_by_ns(ip, port)
            if inst_id is None:
                logging.error("cannot found app instance by ns_key, ip=[%s], port=[%s]", ip, port)
                continue
                #return 1
            self.instance_state[inst_id]['ns_key'] = (ip, port)
            self.instance_state[inst_id]['ns_state'] = state
            self.instance_state[inst_id]['visible'] = visible
            self.instance_state[inst_id]['ns_added'] = True
        return 0

    def get_instance_by_ns(self, ip, port):
        for ref_container in self.containers:
            if ref_container['hostId'] == ip \
                    and int(ref_container['allocatedResource']['port']['range']['first'])\
                    == int(port):
                return ref_container['id']['groupId']['groupName'] \
                        + "_" + str(ref_container['id']['name']).zfill(10)
        return None

    #support there is only one instance on one host
    def get_port_by_ip(self, ip):
        for ref_container in self.containers:
            if ref_container['hostId'] == ip:
                port = ref_container['allocatedResource']['port']['range']['first']
                if type(port) is int:
                    return '%s' % port
                return port
        return None

    def del_container_by_ip_port(self, ip, port):
        """delete container by ipport
        Args:
            ip:
            port:
        Returns: true or false
        """
        for i, ref_container in enumerate(self.containers):
            if ref_container['hostId'] == ip and \
                    int(ref_container['allocatedResource']['port']['range']['first']) == int(port):
                del self.containers[i]
                return True
        return False

    def del_inst_state_by_ip_port(self, ip, port):
        """delete instance by ipport
        Args:
            ip:
            port:
        Returns: true or false
        """
        inst_id = self.get_instance_by_ns(ip, port)
        if inst_id:
            del self.instance_state[inst_id]
            return True
        return False

class SchedulerTaskHelper(object):
    def __init__(self, task_instance_id, context):
        self._task_instance_id = task_instance_id
        self._context = context

        bs_scheduler.set_log()

    def merge_options(self, default_options, up_options):
        for k,v in up_options.iteritems():
            default_options[k] = v
        return default_options

    def get_blank_container(self):
        container_str = """
        {
            "conf": {
                "resource": {
                    "disks":{
                        "workspace":{
                            "mountPoint":{
                                "target":""
                            },
                            "numInodes":1000
                        },
                        "dataDisks":[]
                    }
                }
            }, 
            "id": {
                "groupId":{
                    "cluster":"bj",
                    "groupName":"",
                    "userName":"beehive"
                },
                "name":""
            }, 
            "schedulePolicy": {}
        }
        """
        return json.loads(container_str)

    def _write_dump_file(self, all_apps, all_machines, dump_file):
        dump_dict = {}
        dump_dict['machine'] = all_machines
        dump_app = {}
        for app_id, app_info in all_apps.iteritems():
            dump_app[app_id] = app_info.to_dict()
        
        dump_dict['app'] = dump_app
        dump_str = json.dumps(dump_dict, sort_keys=True, indent=4, separators=(',',':'))
        try:
            ref_dump_file = open(dump_file, 'w')
            ref_dump_file.write(dump_str)
            ref_dump_file.close()
        except Exception as e:
            logging.warning('write dump file failed, file=[%s], error=[%s], task_instance_id=[%s]', dump_file, e, self._task_instance_id)
            return 1, 'write dump file failed'
        return 0, ''

    def _write_action_file(self, action_list, action_file):
        try:
            ref_action_file = open(action_file, 'w')
            for action_item in action_list:
                if action_item == None:
                    continue
                ref_action_file.write(action_item.to_string())
                ref_action_file.write('\n')
            ref_action_file.close()
        except Exception as e:
            logging.warning('write action file failed, file=[%s], error=[%s], task_instance_id=[%s]', action_file, e, self._task_instance_id)
            return 1, 'write action file failed'
        return 0, ''

    def _write_simulate_file(
            self,
            orig_csv,
            eval_orig,
            later_csv,
            eval_later,
            simulate_file,
            eval_json,
            simulate_check_file):
        try:
            ref_simulate_file = open(simulate_file, 'w')
            ref_simulate_file.write('Orig:\n%s\n\nLater:\n%s\n\nEvaluation:\nOrig:\n%s\nLater:\n%s' % (orig_csv, later_csv, ''.join(eval_orig), ''.join(eval_later)))
            ref_simulate_file.close()
    
        except Exception as e:
            logging.warning('write simulate file failed, file=[%s], error=[%s], task_instance_id=[%s]', simulate_file, e, self._task_instance_id)
            return 1, 'write simulate file failed'

        try:
            ref_simulate_check_file = open(simulate_check_file, 'w')
            json.dump(eval_json, ref_simulate_check_file, sort_keys=True, indent=4)
            ref_simulate_check_file.close()
    
        except Exception as exception:
            logging.warning('write simulate check file failed, file=[%s], error=[%s], '
                    'task_instance_id=[%s]', simulate_check_file, exception, self._task_instance_id)
            return 1, 'write simulate check file failed'

        return 0, ''

    def _write_commit_replications_check_file(
            self,
            check_file_name,
            instance_error_app,
            running_instance_error_app,
            commit_check_json,
            commit_check_file):
        try:
            ref_check_file = open(check_file_name, 'w')
            ref_check_file.write('commit: %d app(s) instance error\n' % len(instance_error_app))
            for app_id, app_instance in instance_error_app.iteritems():
                ref_check_file.write('commit: app=%s lost instance [%d != %d + %d + %d + %d] %s\n' %
                    (app_id, app_instance['replications'],
                    len(app_instance['running']), len(app_instance['to_running']),
                    len(app_instance['deploying']), len(app_instance['deploy_fail']),
                    app_instance))
                        
            ref_check_file.write('commit: %d app(s) running instance error\n' %
                    len(running_instance_error_app))
            for app_id, app_instance in running_instance_error_app.iteritems():
                ref_check_file.write('commit: app=%s running instance error [%d != %d + %d] %s \n' %
                    (app_id, app_instance['replications'],
                    len(app_instance['running']), len(app_instance['to_running']),
                    app_instance))
                        
            ref_check_file.close()

        except Exception as exception:
            logging.warning('write replications check file failed, file=[%s], error=[%s],'
                ' task_instance_id=[%s]', check_file_name, exception, self._task_instance_id)
            return 1, 'write replications check file failed'

        try:
            ref_commit_check_file = open(commit_check_file, 'w')
            json.dump(commit_check_json, ref_commit_check_file, sort_keys=True, indent=4)
            ref_commit_check_file.close()
        except Exception as exception:
            logging.warning('write commit check json file failed, file=[%s], error=[%s],'
                ' task_instance_id=[%s]', commit_check_file, exception, self._task_instance_id)

        return 0, ''

    def _write_commit_delete_replications_check_file(
            self,
            check_file_name,
            running_instance_error_app):
        try:
            check_file = open(check_file_name, 'a')
            check_file.write('commit_delete: %d app(s) running instance error\n' %
                    len(running_instance_error_app))
            for app_id, app_instance in running_instance_error_app.iteritems():
                check_file.write('commit_delete: app=%s running instance error [%d != %d] %s \n'
                    % (app_id, app_instance['replications'],
                    len(app_instance['running']),
                    app_instance))
                        
            check_file.close()

        except Exception as exception:
            logging.warning('write replications check file failed, file=[%s], error=[%s],'
                ' task_instance_id=[%s]', check_file_name, exception, self._task_instance_id)
            return 1, 'write replications check file failed'

        return 0, ''


    def _write_commit_check_file(self, success_file, all_success_instance, fail_file, all_fail_instance):
        try:
            ref_success_file = open(success_file, 'w')
            for inst_id in all_success_instance.iterkeys():
                ref_success_file.write(inst_id)
                ref_success_file.write('\n')
            ref_success_file.close()
        except Exception as e:
            logging.warning('write success_instance file failed, file=[%s], error=[%s], task_instance_id=[%s]', success_file, e, self._task_instance_id)
            return 1, 'write success_instance file failed'

        try:
            ref_fail_file = open(fail_file, 'w')
            for inst_id in all_fail_instance.iterkeys():
                ref_fail_file.write(inst_id)
                ref_fail_file.write('\n')
            ref_fail_file.close()
        except Exception as e:
            logging.warning('write fail_instance file failed, file=[%s], error=[%s], task_instance_id=[%s]', fail_file, e, self._task_instance_id)
            return 1, 'write fail_instance file failed'

        return 0, ''

    def load_dump_file(self, dump_file):
        """
        Returns, ret, all_apps, all_machines
        """
        if dump_file is None or len(dump_file) == 0:
            logging.warning('opend dump file failed, dump file name is empty, task_instance_id=[%s]', self._task_instance_id)
            return 1, None, None
        try:
            ref_dump = open(dump_file, 'r')
        except Exception as e:
            logging.warning('open dump file failed, file=[%s], err=[%s], task_instance_id=[%s]', dump_file, e, self._task_instance_id)
            return 1, None, None
        try:
            dump_content = json.loads(ref_dump.read())
        except Exception as e:
            logging.warning('deserialize dump file failed, file=[%s], err=[%s], task_instance_id=[%s]', dump_file, e, self._task_instance_id)
            return 1, None, None
        all_apps = {}
        try:
            for app_id, app_info_dict in dump_content['app'].iteritems():
                app_info = AppInfo()
                app_info.init_from_dict(app_info_dict)
                all_apps[app_id] = app_info
        except Exception as e:
            logging.warning('init app info from dict failed, app_id=[%s], file=[%s], err=[%s], task_instance_id=[%s]', app_id, dump_file, e, self._task_instance_id)
            return 1, None, None
        return 0, all_apps, dump_content['machine']

    def step_resize(self, service_id):
        """
        resize app_type to matrix
        """
        logging.info('begin to resize app_type resource, task_instance_id=[%s], \
                service_id=[%s]', self._task_instance_id, service_id)
        logging.info("ListAppTypeByService() begin")
        start = time.time()
        ret, app_type_list = self._context.service_manager_.ListAppTypeByService(service_id)
        logging.info("ListAppTypeByService() elasped time: %s" % (time.time() - start))
        if ret != 0:
            logging.warning('list app_type by service id failed, task_instance_id=[%s], \
                    service_id=[%s]', self._task_instance_id, service_id)
            return 1, 'list app_type by service id failed'
        ret, err = self.resize_am_app_type(app_type_list, service_id)
        if ret != 0:
            logging.warning('resize_am_app_type failed, task_instance_id=[%s], \
                service_id=[%s] app_typ=[%s]', self._task_instance_id, service_id, app_type)
            return 1, 'resize_am_app_type failed'
        return ret, err

    def step_dump(self, service_id, dump_file, sch_options=None):
        """
        Returns, ret, err_msg, all_apps, all_machines
        """
        logging.info('begin to dump snapshot, task_instance_id=[%s], \
                service_id=[%s]', self._task_instance_id, service_id)
        """
        AM has it's own app list, should add this first
        """

        logging.info("ListAppByService() begin")
        start = time.time()
        ret, app_id_list = self._context.service_manager_.ListAppByService(service_id)
        
        logging.info("ListAppByService() elasped time: %s" % (time.time() - start))
        if ret != 0:
            logging.warning('list app by service id failed, task_instance_id=[%s], \
                    service_id=[%s]', self._task_instance_id, service_id)
            return 1, 'list app by service id failed', None, None

        #step 1: get all app infos, check existence in beehive and get spec
        logging.info("get_am_app_infos() begin")
        start = time.time()
        ret, all_apps = self.get_am_app_infos(app_id_list, service_id)
        logging.info("get_am_app_infos() elasped time: %s" % (time.time() - start))
        if ret != 0:
            logging.warning('dump all app infos failed, count_app=[%d], task_instance_id=[%s]', \
                    len(app_id_list), self._task_instance_id)
            return 1, 'dump all app infos failed', None, None

        #step 2: dump snapshot, get all machines and related container-group(app)
        #TODO: wheth to support other constrains besides include_machine_labels?
        include_labels = []
        for k, app_info in all_apps.iteritems():
            if app_info.goal_resource_spec is None:
                continue
            try:
                tmp_labels = app_info.goal_resource_spec["schedulePolicy"]\
                        ["tagExpression"].split('|')
                for label in tmp_labels:
                    if label not in include_labels:
                        include_labels.append(label)
            except Exception as e:
                logging.warning("no include_machine_label in resource spec, \
                        cannot allocate resource, err=[%s], app_id=[%s], task_instance_id=[%s], \
                        goal_resource_spec=[%s]", e, app_info.app_id, self._task_instance_id, 
                        json.dumps(app_info.goal_resource_spec))
                return 1, 'tagExpression is not found', None, None 

        logging.info("dump_machines() begin")
        start = time.time()
        ret, all_machines = self.dump_machines('|'.join(include_labels), all_apps)
        logging.info("dump_machines() elasped time: %s" % (time.time() - start))
        if ret != 0:
            logging.warning("dump machines failed, task_instance_id=[%s], labels=[%s]", 
                    self._task_instance_id, include_labels)
            return ret, all_machines, None, None
       
        logging.info("dump_resource() begin")
        start = time.time()
        ret = self.dump_resource(all_apps, service_id)
        logging.info("dump_resource() elasped time: %s" % (time.time() - start))
        if ret != 0:
            logging.warning("dump resource failed, task_instance_id=[%s]", self._task_instance_id)
            return ret, "dump resource failed", None, None
        
        logging.info("_write_dump_file() begin")
        start = time.time()
        ret, err = self._write_dump_file(all_apps, all_machines, dump_file)
        logging.info("_write_dump_file() elasped time: %s" % (time.time() - start))
        if ret != 0:
            logging.warn('dump snapshot failed, task_instance_id=[%s]', self._task_instance_id)
            return ret, err, None, None

        logging.info('finish to dump snapshot, task_instance_id=[%s], service_id=[%s]', 
                self._task_instance_id, service_id)
        return 0, '', all_apps, all_machines

    def modify_app_resource_spec(self, all_apps, sch_options=None):
        """ modify app resource container spec data
            modify goal_resource_spec
        """
        skip_reconfig_dict = sch_options.get("skip_reconfig")
        try:
            if skip_reconfig_dict:
                for app_id, app_info in all_apps.iteritems():
                    resource_spec = app_info.resource_spec
                    if not resource_spec:
                        continue
                    if skip_reconfig_dict.get("cpu", False):
                        resource_spec["resource"]["cpu"]["numCores"] = 0
                    if skip_reconfig_dict.get("mem", False):
                        resource_spec["resource"]["memory"]["sizeMB"] = 0
                    if skip_reconfig_dict.get("ssd", False):
                        resource_spec["resource"]["disks"]["dataSpace"]["sizeMB"] = 0
        except:
            logging.warn("skip_reconfig failed")
                     
        modify_resource_info_dict = {}
        for app_id, app_info in all_apps.iteritems():
            index_type = app_id[:app_id.rfind("_")]
            db_index = index_type.replace("bs_", "")
            container_group_spec = app_info.goal_resource_spec
            if not container_group_spec:
                continue

            if db_index not in modify_resource_info_dict:
                modify_resource_info_dict[db_index] = {}
                modify_resource_info_dict[db_index]["cpu"] = []
                modify_resource_info_dict[db_index]["mem"] = []
                modify_resource_info_dict[db_index]["ssd"] = []

            cpu = int(container_group_spec["resource"]["cpu"]["numCores"]) \
                    if  container_group_spec.get('resource') and \
                    container_group_spec['resource'].get('cpu') and \
                    container_group_spec['resource']['cpu'].get('numCores') \
                    else 0
            mem = int(container_group_spec["resource"]["memory"]["sizeMB"]) \
                    if  container_group_spec.get('resource') and \
                    container_group_spec['resource'].get('memory') and \
                    container_group_spec['resource']['memory'].get('sizeMB') \
                    else 0
            size = int(container_group_spec["resource"]["disks"]["dataSpace"]["sizeMB"]) \
                    if  container_group_spec.get('resource') and \
                    container_group_spec['resource'].get('disks') and \
                    container_group_spec['resource']["disks"].get('dataSpace') and \
                    container_group_spec["resource"]["disks"]["dataSpace"].get('sizeMB') \
                    else 0

            modify_resource_info_dict[db_index]["cpu"].append(cpu)
            modify_resource_info_dict[db_index]["mem"].append(mem)
            modify_resource_info_dict[db_index]["ssd"].append(size)

        for db_index, app_resources in modify_resource_info_dict.iteritems():
            temp_counter = collections.Counter()
            temp_counter.update(app_resources["cpu"])
            try:
                modify_resource_info_dict[db_index]["cpu"] = temp_counter.most_common(1)[0][0]
            except Exception as e:
                modify_resource_info_dict[db_index]["cpu"] = 0

            temp_counter.clear()
            temp_counter.update(app_resources["mem"])
            try:
                modify_resource_info_dict[db_index]["mem"] = temp_counter.most_common(1)[0][0]
            except Exception as e:
                modify_resource_info_dict[db_index]["mem"] = 0
            temp_counter.clear()
            temp_counter.update(app_resources["ssd"])
            try:
                modify_resource_info_dict[db_index]["ssd"] = temp_counter.most_common(1)[0][0]
            except Exception as e:
                modify_resource_info_dict[db_index]["ssd"] = 0

        for app_id, app_info in all_apps.iteritems():
            index_type = app_id[:app_id.rfind("_")]
            db_index = index_type.replace("bs_", "")
            if db_index not in modify_resource_info_dict:
                continue

            container_group_spec = app_info.goal_resource_spec
            if not container_group_spec:
                continue

            orig = int(container_group_spec["resource"]["cpu"]["numCores"]) \
                    if  container_group_spec.get('resource') and \
                    container_group_spec['resource'].get('cpu') and \
                    container_group_spec['resource']['cpu'].get('numCores') \
                    else 0
            after = int(modify_resource_info_dict[db_index]["cpu"])
            if orig != after:
                container_group_spec["resource"]["cpu"]["numCores"] = \
                    modify_resource_info_dict[db_index]["cpu"]
                logging.info("change normalized_cpu_cores: app_id=%s orig=%s after=%s",
                             app_id, orig, after)

            orig = int(container_group_spec["resource"]["memory"]["sizeMB"]) \
                    if  container_group_spec.get('resource') and \
                    container_group_spec['resource'].get('memory') and \
                    container_group_spec['resource']['memory'].get('sizeMB') \
                    else 0
            after = int(modify_resource_info_dict[db_index]["mem"])
            if orig != after:
                container_group_spec["resource"]["memory"]["sizeMB"] = \
                    modify_resource_info_dict[db_index]["mem"]
                logging.info("change memory_size: app_id=%s orig=%s after=%s",
                             app_id, orig, after)

            orig = int(container_group_spec["resource"]["disks"]["dataSpace"]["sizeMB"]) \
                    if  container_group_spec.get('resource') and \
                    container_group_spec['resource'].get('disks') and \
                    container_group_spec['resource']["disks"].get('dataSpace') and \
                    container_group_spec["resource"]["disks"]["dataSpace"].get('sizeMB') \
                    else 0
            after = int(modify_resource_info_dict[db_index]["ssd"])
            if orig != after:
                container_group_spec["resource"]["disks"]["dataSpace"]["sizeMB"] = \
                    modify_resource_info_dict[db_index]["ssd"]
                logging.info("change ssd_size: app_id=%s orig=%s after=%s",
                             app_id, orig, after)

        return all_apps

    def step_generate_plan(self, all_apps, all_machines, sch_options, 
            plan_file, sim_file, simulate_check_file, run_mode, app_to_handle=None):
        """
        this function is used to generate plan
        """
        #init IndexPlacementManager, TODO: move to bs_scheduler.py
        logging.info('begin to generate plan, task_instance_id=[%s]', self._task_instance_id)
        logging.info('begin to init bs scheduler, task_instance_id=[%s]', self._task_instance_id)
        placement_manager, err_msg = self.create_placement_manager(
                all_apps, all_machines, sch_options)
        if placement_manager is None:
            logging.info('fail to init bs scheduler, task_instance_id=[%s]', self._task_instance_id)
            return None, 'fail to init bs scheduler'
        logging.info('finish to init bs scheduler, task_instance_id=[%s]', self._task_instance_id)
        orig_csv = placement_manager.dump_to_csv()
        eval_json = {}
        eval_orig, eval_json['orig'] = bs_scheduler.plan_evaluation(placement_manager)

        #generate placement plan -- action
        #check if to apply user action
        if "action_file" not in sch_options or len(sch_options['action_file']) == 0:
            logging.info("no action_file specified, to allocate, task_instance_id=[%s]", self._task_instance_id)
            #TODO:restore is not OK
            if run_mode == 'deploy':
                logging.info("call allocate to generate plan in run_mode [%s], task_instance_id=[%s]", run_mode, self._task_instance_id)
                ret = placement_manager.allocate(partition_list=app_to_handle, slavenode_list=None)
                if ret != 0:
                    logging.warning("bs scheduler failed, allocate failed, task_instance_id=[%s]", self._task_instance_id)
                    return None, 'fail to allocate'
            elif run_mode == 'restore':
                logging.info("call allocate to generate plan in run_mode [%s], task_instance_id=[%s]", run_mode, self._task_instance_id)
                ret = placement_manager.restore()
                if ret != 0:
                    logging.warning("bs scheduler failed, allocate failed, task_instance_id=[%s]", self._task_instance_id)
                    return None, 'fail to allocate'
            #TODO: partition to fix
            elif run_mode == 'rebalance':
                logging.info("call generate to generate plan in run_mode [%s], task_instance_id=[%s]", run_mode, self._task_instance_id)
                ret = placement_manager.generate(sch_options['allow_move_group'], sch_options['break_up_dist'], False, None, None)
                if ret != 0:
                    logging.warning("bs scheduler failed, allocate failed, task_instance_id=[%s]", self._task_instance_id)
                    return None, 'fail to allocate'

        else:
            logging.info("action_file is specified, skip allocation, to apply, task_instance_id=[%s]", self._task_instance_id)
            action_file = sch_options["action_file"]
            action_str_list = []
            try:
                f = open(action_file, 'r')
                action_str_list = f.readlines()
                f.close()
            except Exception as e:
                logging.error("read action_file failed, task_instance_id=[%s], file=[%s]", self._task_instance_id, action_file)
                return None, "read action_file failed"
            failed_list = placement_manager.apply_extend_actions(action_str_list)
            if len(failed_list) != 0:
                logging.warning("action executed failed, task_instance_id=[%s], failed_actions=[\n%s]", self._task_instance_id, '\n'.join(failed_list))

        if "action_file" not in sch_options or len(sch_options['action_file']) == 0:
            placement_manager.compress_action_list()
        placement_manager.allocate_slots()
        placement_manager.allocate_storage()

        ret, err = self._write_action_file(placement_manager.action_list, plan_file)
        if ret != 0:
            logging.warn('write action failed, task_instance_id=[%s]', self._task_instance_id)
            return None, err

        later_csv = placement_manager.dump_to_csv()
        eval_later, eval_json['later'] = bs_scheduler.plan_evaluation(placement_manager)
        ret, err = self._write_simulate_file(orig_csv, eval_orig, later_csv, eval_later, sim_file,
                eval_json, simulate_check_file)
        if ret != 0:
            logging.warn('write simulate failed, task_instance_id=[%s]', self._task_instance_id)
            return None, err

        return placement_manager.action_list, ''

    def _has_error(self, app_inst_descr):
        if 'agent_state' not in app_inst_descr or app_inst_descr['agent_state'] == 'unavailable':
            return True
        if "container" not in app_inst_descr or "status" not in app_inst_descr["container"] or \
                "status" not in app_inst_descr["container"]["status"] or \
                app_inst_descr["container"]["status"]["status"] != "OK":
            return True
        if 'status' in app_inst_descr['package'] \
                and app_inst_descr['package']['status'].startswith('Error'):
            return True
        for data_name, data_item in app_inst_descr["dynamic_data"]["items"].iteritems():
            if 'status' in data_item and data_item['status'].startswith('Error'):
                return True
            if 'sub_data' in data_item:
                for sub_data_name, sub_data_item in data_item["sub_data"].iteritems():
                    if 'status' in sub_data_item and sub_data_item['status'].startswith('Error'):
                        return True

        return False

    def resize_am_app_type(self, app_type_list, service_id):
        """
        resize rm resource with am_type thing
        """
        ret, app_id_list = self._context.service_manager_.ListAppByService(service_id)
        if ret != 0:
            logging.warning('list app_type by service id failed, task_instance_id=[%s], \
                service_id=[%s]', self._task_instance_id, service_id)
            return 1, 'list app_type by service id failed'

        for app_type in app_type_list:
            ret, app_type_meta = self._context.service_manager_.AppTypeGetMetaAll(app_type, 
                    service_id)
            if ret != 0:
                logging.warning('get app_type resource failed, task_instance_id=[%s], \
                    service_id=[%s], app_type=[%s]', self._task_instance_id, service_id, app_type)
                return 1, 'get app_type resource failed'
            cpu = int(app_type_meta["resource"]["resource"]["cpu"]["numCores"])
            cpu = (cpu * 4 + 9) / 10
            mem = int(app_type_meta["resource"]["resource"]["memory"]["sizeMB"])
            if "dataSpace" not in app_type_meta["resource"]["resource"]["disks"]:
                logging.warning("app_type [%s] am has no dataSpace", app_type)
                ssd = 0
            else:
                ssd = int(app_type_meta["resource"]["resource"]["disks"]["dataSpace"]["sizeMB"])
            if (app_type.find("rts") != -1):
                logging.warning('app_type=[%s], set mem to 1024 due to tmpfs', app_type)
                mem = 1024

            type_app_list = [app_id for app_id in app_id_list 
                    if app_id[:app_id.rfind("_")] == app_type]

            if len(type_app_list) == 0:
                continue

            bh_resource = self._context.bhcli_.get_resource('')
            try:
                ret, output = bh_resource.describe_container_group(type_app_list[0])
                container_group = json.loads(output)
                ret = self.check_app_resource(container_group, cpu, mem, ssd)
                if ret == 0:
                    continue
            except Exception as e:
                logging.warning("check_app_resource app_id [%s] resource filed", app_id)
                continue

            update_resource_result = self._context.bhcli_.update_resource_batch(type_app_list, 
                    cpu, mem, ssd)

            for app_id, result in update_resource_result.iteritems():
                if result[0] != 0:
                    logging.warning('update_group fail, task_instance_id=[%s], service_id=[%s], \
                            app_type=[%s], output=[%s]', self._task_instance_id, service_id, 
                            app_type, result[1])

            #for app_id in type_app_list:
            #    ret, output = bh_resource.describe_container_group(app_id)
            #    container_group = json.loads(output)
            #    if "dataSpace" in container_group["container_group_spec"]\
            #            ["resource"]["disks"].keys():
            #        container_group["container_group_spec"]["resource"]["disks"]["dataSpace"]\
            #                ["sizeMB"] = ssd
            #    container_group["container_group_spec"]["resource"]["cpu"]["numCores"] = cpu
            #    container_group["container_group_spec"]["resource"]["memory"]["sizeMB"] = mem
            #    ret, out = bh_resource.update_container_group(app_id, container_group)
            #    if ret != 0:
            #        logging.warning('update_container_group failed, task_instance_id=[%s], \
            #                service_id=[%s], app_type=[%s]', self._task_instance_id, service_id, 
            #                app_type)
            #        return 1, 'update_container_group failed'
            #    ret, out = bh_resource.update_containers(app_id, cpu, mem, ssd)
            #    if ret != 0:
            #        logging.warning('update_containers failed, task_instance_id=[%s], \
            #                service_id=[%s], app_type=[%s]', self._task_instance_id, service_id, 
            #                app_type)
            #        return 1, 'update_containers failed'
        return 0, ''

    def check_app_resource(self, container_group, app_type_cpu, app_type_mem, app_type_ssd):
        """
        check resource if changed
        """
        m_cpu = container_group["container_group_spec"]["resource"]["cpu"]["numCores"]
        m_mem = container_group["container_group_spec"]["resource"]["memory"]["sizeMB"]
        if "dataSpace" not in container_group["container_group_spec"]["resource"]["disks"]:
            m_ssd = 0 
        else:
            m_ssd = container_group["container_group_spec"]["resource"]["disks"]\
                    ["dataSpace"]["sizeMB"]
    
        if app_type_cpu == m_cpu and app_type_mem == m_mem and app_type_ssd == m_ssd:
            return 0
        return 1
        
    def get_am_app_infos(self, app_id_list, service_id):
        """get app infos from am zk
        Args:
            app_id_list: app id list
            service_id: service(bc/bs)
        Returns:
            ret:seccess:0/fail:1
            out:appInfo list
        """
        app_info_dict = {}
        ret, app_type_list = self._context.service_manager_.ListAppTypeByService(service_id)
        if ret != 0:
            app_type_list = list()
        for app_id in app_id_list:
            index_type = app_id[:app_id.rfind("_")]
            app_meta = {}
            if index_type not in app_type_list:
                ret, app_meta = self._context.service_manager_.AppGetMetaAll(app_id, service_id)
            else:
                ret, app_meta = self._context.service_manager_.AppTypeGetMetaAll(index_type, 
                        service_id)
            if ret != 0:
                logging.warning("get meta of app failed, skipped in dump,\
                        task_instance_id=[%s], app_id=[%s], service_id=[%s]", 
                        self._task_instance_id, app_id, service_id)
                return 1, None
            elif 'layer' not in app_meta:                                   
                logging.warning('get "layer" of app failed, skipped in dump, \
                        task_instance_id=[%s], app_id=[%s], service_id=[%s]',     
                        self._task_instance_id, app_id, service_id)               
            app_info = AppInfo(app_id, None, None, None, app_meta["resource"], None, None,
                    None, app_meta['layer'], True)
            app_info_dict[app_id] = app_info
        return 0, app_info_dict

    def get_all_app_infos(self, app_id_list, service_id, scope='all'):
        """create and fill class AppInfo for each app in app_id_list
        Args:
            app_id_list: app to get
            service_id: service id
            scope: 'all', get info for each app in app_id_list
                   'added', get info for each app in app_id_list which has already been added/deployed
                   'not-added', get info for each app in app_id_list which havs not been added/deployed yet
        Return:
            ret: return code, non-zero if invalid argument
            dict of all AppInfo: containing app only fit for the scope argument. Once failed for any app, its app_spec and containers will be None
        """
        if scope not in ['all', 'added', 'not-added']:
            logging.warning('scope=[%s] is not supported', scope)
            return -1, None

        app_info_dict = {}
        ret = self.get_app_info_from_bh(app_id_list, service_id, app_info_dict, scope)
        if ret != 0:
            return 1, None
        return ret, app_info_dict

    def get_app_info_from_bh(self, app_id_list, service_id, app_info_dict, scope='all'):
        """create and fill class AppInfo for each app in app_id_list
        Args:
            app_id_list: app to get
            service_id: service id
            app_info_dict: containing app only fit for the scope argument. Once failed for any app, its app_spec and containers will be None
            scope: 'all', get info for each app in app_id_list
                   'added', get info for each app in app_id_list which has already been added/deployed
                   'not-added', get info for each app in app_id_list which havs not been added/deployed yet
        Return:
            ret: return code, non-zero if invalid argument
        """
        bh_app = self._context.bhcli_.get_app("")
        #TODO: 'app exist' instead of 'app describe'
        app_desc_results = bh_app.describe_batch(app_id_list)
        dump_ns_app_id_list = []
        for app_id, result in app_desc_results.iteritems():
            if result[0] != 0:
                logging.warning("app %s is not exists", app_id)
                continue
            try:
                app_descr = json.loads(result[1].decode("gbk"))
                app_spec = app_descr['app_spec']
            except Exception as e:
                logging.info("app is format error, skipped, task_instance_id=[%s], \
                        app_id=[%s], err=[%s]", self._task_instance_id, app_id, e)
                continue

            if "app_id" in app_spec:
                del app_spec['app_id']
            if "resource" in app_spec:
                del app_spec['resource']
            del app_spec['package']
            del app_spec['dynamic_data']
            added = ("app_instance" in app_descr and len(app_descr["app_instance"]) != 0)
            if added: #Added
                if scope == 'not-added':
                    logging.info("app is added already, skipped, task_instance_id=[%s], \
                            app_id=[%s]", self._task_instance_id, app_id)
                    continue
            else:#Not-added
                if scope == 'added':
                    logging.info("app is not added yet, skipped, task_instance_id=[%s], \
                            app_id=[%s]", self._task_instance_id, app_id)
                    continue

            containers = []
            instance_state = {}
            if added:
                #get app_spec and containers from beehive
                for app_inst in app_descr["app_instance"]:
                    inst_id = app_inst["app_instance_id"]
                    if "container" not in app_inst:
                        logging.info("app instance has not container, skipped, instance_id=[%s], \
                                app_id=[%s]", inst_id, app_id)
                        continue
                    container = app_inst["container"]["container_info"]
                    containers.append(container)
                    instance_state[inst_id] = {}
                    if 'runtime' not in app_inst or 'run_state' not in app_inst['runtime']:
                        instance_state[inst_id]['run_state']  = 'NEW' 
                        logging.warning("invalid app, no run_state, consider as NEW, \
                                app_id=[%s]", app_id)
                    else:
                        instance_state[inst_id]['run_state'] = app_inst['runtime']['run_state']
                        #if app_inst['runtime']['run_state'] == "STOP" and \
                        #        app_inst['runtime']['plan_running'] == True:
                        #    instance_state[inst_id]['run_state'] = "DEPLOYFAIL"
                    #NEW by default, since new instance may have not been add into NS (added only after it's already RUNNING)
                    #RUNNING, DELETED and TBD will be updated later
                    instance_state[inst_id]['ns_state'] = 'NEW'
                    instance_state[inst_id]['ns_key'] = (container['hostId'], 
                            str(container['allocatedResource']['port']['range']['first']))
                    if self._has_error(app_inst) and \
                            instance_state[inst_id]['run_state'] in ["NEW", "REPAIR"]:
                        instance_state[inst_id]['run_state'] = "DEPLOYFAIL"
                    instance_state[inst_id]['ns_added'] = False
            if app_id in app_info_dict and app_info_dict[app_id].goal_resource_spec is not None:
                goal_resource_spec = app_info_dict[app_id].goal_resource_spec
                managed = app_info_dict[app_id].managed
            else:
                managed = True
                ret, goal_resource_spec = self._context.service_manager_.AppGetSpec(
                        'resource', app_id, service_id)
                if ret != 0:
                    logging.warning("get resource spec of app failed, skipped in dump, \
                            task_instance_id=[%s], app_id=[%s], service_id=[%s]", 
                            self._task_instance_id, app_id, service_id)
            #TODO: trick for test index

            if 'upstream' not in app_spec["app_attribute"]["labels"]:
                group = None
            else:
                group = app_spec["app_attribute"]["labels"]["upstream"]

            ret,app_meta = self._context.service_manager_.AppGetMetaAll(app_id, service_id)
            layer = None
            if ret != 0:
                logging.warning("get meta of app failed, skipped in dump, \
                        task_instance_id=[%s], app_id=[%s], service_id=[%s]", 
                        self._task_instance_id, app_id, service_id)
                layer = None
            elif 'layer' not in app_meta:
                logging.warning('get "layer" of app failed, skipped in dump, \
                        task_instance_id=[%s], app_id=[%s], service_id=[%s]', 
                        self._task_instance_id, app_id, service_id)
            else:
                layer = app_meta['layer']
            resource_spec = app_descr['container_group']
            if added and len(containers) == 0:
                added = False
            app_info = AppInfo(app_id, added, app_spec, resource_spec, 
                    goal_resource_spec, containers, instance_state, 
                    group, layer, managed)
            #get ns_state
            
            """
            dump fail  will exit
            1 no app
            2 no tag
            3 no visible
            """
            if added:
                dump_ns_app_id_list.append(app_id)
            app_info_dict[app_id] = app_info
        ret = self.dump_ns_app_results(dump_ns_app_id_list, app_info_dict, service_id)
        return ret

    def dump_ns_app_results(self, dump_ns_app_id_list, app_info_dict, service_id):
        """get all apps's naming info for ns_state
        Args:
            dump_ns_app_id_list: the app id list
            all_info_dict: the all app info
            service_id: the service id
        Returns:
            ret: is it successed
        """
        dump_ns_app_results = self._context.bhcli_.dump_ns_app_batch(dump_ns_app_id_list)
        error_map = {}
        error_count = 0
        for app_id, result in dump_ns_app_results.iteritems():
            if result[0] != 0:
                error_map[app_id] = "err=%d: %s" % (result[0], result[1])
                error_count += 1
                continue

            for ns_item in result[2]:
                ip = ns_item['ip']
                port = str(ns_item['port'])
                ns_state = "NEW"
                if 'tag' in ns_item  and 'ns_state' in ns_item['tag']:
                    ns_state = ns_item['tag']['ns_state']
                if 'visible' not in ns_item:
                    err = "\n\tns_item[%s:%s]: visible is not configured" % (ip, port)
                    if app_id in error_map:
                        error_map[app_id] += err
                    else:
                        error_map[app_id] = err
                        error_count += 1
                    continue
                visible = ns_item['visible']
                ret = app_info_dict[app_id].add_instance_ns_state(ns_state, visible, [(ip, port)])
                if ret != 0:
                    err = "\n\tns_item[%s:%s]: add_instance_ns_state failed" % (ip, port)
                    if app_id in error_map:
                        error_map[app_id] += err
                    else:
                        error_map[app_id] = err
                        error_count += 1

        if error_count > 0:
            error_str = ''
            for app_id, error_msg in error_map.iteritems():
                error_str += ("\napp[%s]: %s" % (app_id, error_msg))
            logging.error('dump_ns_app failed, task_instance_id=[%s], service_id=[%s]: %s',
                    self._task_instance_id, service_id, error_str)
            return 1

        return 0
    
    def dump_resource(self, all_apps, service_id):
        """
        dump resource
        """
        container_group_id_list = []
        container_group_id_list.extend(all_apps.keys())
        #ret = self.get_app_info_from_bh(container_group_id_list, service_id, all_apps)
        bh_app = self._context.bhcli_.get_app("")
        app_desc_results = bh_app.describe_batch(container_group_id_list)
        dump_ns_app_id_list = []
        for app_id, result in app_desc_results.iteritems():
            if result[0] != 0:
                logging.warning("app %s is not exists", app_id)
                del all_apps[app_id]
                continue
            try:
                app_descr = json.loads(result[1].decode("gbk"))
                app_spec = app_descr['app_spec']
            except Exception as e:
                logging.info("app is format error, skipped, task_instance_id=[%s], \
                        app_id=[%s], err=[%s]", self._task_instance_id, app_id, e)
                continue
            if "app_id" in app_spec:
                del app_spec['app_id']
            if "resource" in app_spec:
                del app_spec['resource']
            del app_spec['package']
            del app_spec['dynamic_data']
            all_apps[app_id].app_spec = app_spec
            labels = app_descr["app_spec"]["app_attribute"]["labels"]
            if "upstream" in labels:
                all_apps[app_id].group = labels["upstream"]
            all_apps[app_id].resource_spec = app_descr['container_group']
            added = ("app_instance" in app_descr and len(app_descr["app_instance"]) != 0)
            containers = []
            instance_state = {}
            if added:
                dump_ns_app_id_list.append(app_id)
                for app_inst in app_descr["app_instance"]:
                    if "container" not in app_inst:
                        continue
                    container = app_inst["container"]["container_info"]
                    containers.append(container)
                    inst_id = app_inst["app_instance_id"]
                    instance_state[inst_id] = {}
                    if 'runtime' not in app_inst or 'run_state' not in app_inst['runtime']:
                        instance_state[inst_id]['run_state']  = 'NEW'
                        logging.warning("invalid app, no run_state, consider as NEW, \
                                app_id=[%s]", app_id)
                    else:
                        instance_state[inst_id]['run_state'] = app_inst['runtime']['run_state']
                        #if app_inst['runtime']['run_state'] == "STOP" and \
                        #        app_inst['runtime']['plan_running'] == True:
                        #    instance_state[inst_id]['run_state'] = "DEPLOYFAIL"
                    #NEW by default, since new instance may have not been add into NS (added only after it's already RUNNING)
                    #RUNNING, DELETED and TBD will be updated later
                    instance_state[inst_id]['ns_state'] = 'NEW'
                    instance_state[inst_id]['ns_key'] = (container['hostId'], 
                            str(container['allocatedResource']['port']['range']['first']))
                    if self._has_error(app_inst) and \
                            instance_state[inst_id]['run_state'] in ["NEW", "REPAIR"]:
                        instance_state[inst_id]['run_state'] = "DEPLOYFAIL"
                    instance_state[inst_id]['ns_added'] = False
                if len(containers) != 0:
                    all_apps[app_id].added = True
                    all_apps[app_id].containers = containers
            all_apps[app_id].instance_state = instance_state
        
        ret = self.dump_ns_app_results(dump_ns_app_id_list, all_apps, service_id)
        return ret
    
    #TODO: move to bhcli_helper
    def dump_machines(self, include_labels, all_apps):
        """get all machines' info from beehive, filtered by machine label for current service
        get placement distribution for each related container-group and fill into all_apps
        Args:
        Returns:
            ret: return code, 0, OK,
                              no-zero, error, the second return turns to error message
            machines: response of get-capable machine
                      error msg if fail

        """

        bh_machine = self._context.bhcli_.get_machine('')
        ret, machine_list = bh_machine.list_by_labels(include_labels)
        if ret != 0:
            logging.warning("list machine failed, check bhcli config")
            return 1, 'list machine failed, label=[%s]' % ','.join(include_labels)
        all_machine = json.loads(machine_list)
        ret, snapshot_mac = bh_machine.batch_describe(all_machine["hosts"])
        if ret != 0:
            logging.warning("get machine info failed, check bhcli config")
            return 1, 'get machine info failed, hosts=[%s]' % ','.join(all_machine["hosts"])

        for machine_json in snapshot_mac:
            for container_id in machine_json["hostInfo"]["containerIds"]:
                app_id = container_id["groupId"]["groupName"]
                if app_id not in all_apps:
                    app_info = AppInfo(app_id, True)
                    all_apps[app_id] = app_info
        return 0, snapshot_mac

    def create_placement_manager(self, all_apps, all_machines, sch_options):
        """create placement manager due to snapshot
        TODO: should move to be_scheduler
        """

        placement_manager = bs_scheduler.IndexPlacementManager(sch_options)
        #add slavenodes
        i = 0
        logging.info("init scheduler, get [%d] machines, task_instance_id=[%s]", 
                len(all_machines), 
                self._task_instance_id)
        while i < len(all_machines):
            mac_spec = all_machines[i]["hostInfo"]
            mac_remain = all_machines[i]["hostInfo"]["freeResource"]
            node = placement_manager.create_slavenode_from_bh_spec(mac_spec, mac_remain)
            i+=1
            if node == None:
                logging.warning("create node failed, skipped, task_instance_id=[%s], \
                        id=[%s]", self._task_instance_id, mac_spec["id"])
                #return None, "init bs_scheduler failed when creating node, \
                #        hostname=[%s]" % mac_spec['hostname']
                continue
        placement_manager.normalize_mac_cpu()

        #take slot
        for app_id, app_info in all_apps.iteritems():
            placement_manager.add_slavenode_slots(app_id, app_info)
        #add index partitions
        logging.info("init scheduler, get [%d] app"  % len(all_apps))
        for app_id,app_info in all_apps.iteritems():
            #TODO: if app named with app_id.www.su, change way to get partition num
            #add app under the given service_id as index partition
            if app_info.managed:
                #since RM did not support changing resource spec, so using resource_spec in AM to generate plan and resource_spec in RM to reconfig
                ref_partition = placement_manager.create_index_partition_from_bh_spec(
                        app_id, app_info.goal_resource_spec, 
                        app_info.app_spec["app_attribute"]["replicas"], 
                        app_info.group, app_info.layer)
                if ref_partition == None:
                    logging.warning("create index partition failed, task_instance_id=[%s], \
                            app_id=[%s]", self._task_instance_id, app_id)
                    return None, "init bs_scheduler failed when creating index partition, \
                            app_id=[%s]" % (app_id)
                if ref_partition.type in sch_options["stable_index_types"]:
                    ref_partition.to_ignore = True

                for container in app_info.containers:
                    bad_group = placement_manager.place_index_partition_from_bh_container(
                            ref_partition, container, 
                            app_info.instance_state[container['id']['groupId']['groupName'] \
                                    + str("_") \
                                    + str(container['id']['name']).zfill(10)])
            #add other apps as zombie
            else:
                ref_zombie = placement_manager.create_zombie_app_from_bh_spec(app_id, 
                        app_info.resource_spec, len(app_info.containers))
                if ref_zombie == None:
                    logging.warning("create zombie failed, task_instance_id=[%s], app_id=[%s]", 
                            self._task_instance_id, app_id)
                    return None, "init bs_scheduler failed when creating zombie, \
                            app_id=[%s]" % (app_id)

                for container in app_info.containers:
                    placement_manager.place_zombie_instance(ref_zombie, container)

        #init scheduler
        for ref_group in placement_manager.index_groups.itervalues():
            ref_group.determine_real_idc()
        placement_manager.action_list = []

        for i in range(0, len(placement_manager.resource_total)):
            if placement_manager.resource_total[i] == 0:
                placement_manager.resource_total[i] = -1
        #generate IndexPlacementManager finished

        return placement_manager, ''

    #one machine, one partition. A lot of error is avoid by the rule
    def _is_conflicted(self, container_list, hostname):
        for c in container_list:
            if c['hostId'] == hostname:
                return True
        return False

    def step_preview_actions(self, action_list, sch_options, all_apps, prev_file, prev_check_file):
        """step for apply
        Args:
            action_list: list of apply action
            sch_options: scheder options
            all_apps: all managed app
            preview_file: the file to preview
        Returns:
            if success
        """
        preview = self.preview_add_actions(action_list, sch_options, all_apps)
        try:
            ref_prev_file = open(prev_file, 'w')
            ref_prev_check_file = open(prev_check_file, 'w')

            preview_json = {'num': 0, 'fail_apps': {}}
         
            for mac_id, result in preview.iteritems():
                if result[1] == "":
                    continue
                operation_result = json.loads(result[1])
                #logging.info(operation_result)
                batch_operate_result = operation_result["batchOperationResult"]
                if "failedOperations" not in batch_operate_result or \
                        len(batch_operate_result["failedOperations"]) == 0:
                    continue
                for failed_operation in batch_operate_result["failedOperations"]:
                    ref_prev_file.write("%s_%s_%s: %s" % 
                            (mac_id, failed_operation["containerId"]["groupId"]["groupName"],
                            failed_operation["containerId"]["name"], 
                            failed_operation["errorMessage"]))
                    ref_prev_file.write('\n')

                    app_id = failed_operation["containerId"]["groupId"]["groupName"]
                    if app_id not in preview_json['fail_apps']:
                        preview_json['num'] += 1
                        preview_json['fail_apps'][app_id] = {'num':0, 'list':[]}
                    preview_json['fail_apps'][app_id]['num'] += 1
                    reason = {'machine': mac_id, 'reason': failed_operation["errorMessage"]}
                    preview_json['fail_apps'][app_id]['list'].append(reason)

            json.dump(preview_json, ref_prev_check_file, sort_keys=True, indent=4)
            ref_prev_file.close()
            ref_prev_check_file.close()
        except Exception as e:
            logging.warning('write prev file failed, file=[%s], error=[%s], \
                    task_instance_id=[%s]', prev_file, e, self._task_instance_id)
            return 1, 'write prev file failed'
        return 0, ''

    def step_apply_actions(self, action_list, sch_options, all_apps,
            alloc_file, disable_empty_storage, offline_mode=False):
        """step for apply
        Args:
            action_list: list of apply action
            sch_options: scheder options
            all_apps: all managed app
        Returns:
            failed count of apply actions
        """
        if offline_mode:
            logging.warning("Attention!!! Now running in offline mode!!!")
        ret = self.apply_add_actions(action_list, sch_options, all_apps,
                alloc_file, disable_empty_storage, True, offline_mode)
        ret += self.apply_del_actions(action_list, all_apps, offline_mode)
        return ret

    def step_commit(
            self, 
            ref_task_instance, 
            app_id_list, 
            service_id, 
            interval_sec, 
            replications_check_file,
            success_file, 
            fail_file,
            commit_check_file,
            force_delete_move_failed):
        """the commit step, to check the inses if finished
            Args:
            Returns: true or false
        """
        need_check = app_id_list
        #for output
        all_success_instance = {}
        all_fail_instance = {}
        instance_error_app = {}
        running_instance_error_app = {}
        while len(need_check) > 0:
            ret, all_apps = self.get_all_app_infos(need_check, service_id, scope='added')
            if ret != 0:
                logging.warn("dump all app failed, num_app_to_check=[%d], task_instance_id=[%s]", len(need_check), self._task_instance_id)
                return 1
            #still has deploying instance
            need_check = []
            #only STOP instance
            all_fail = []
            total_instance = 0
            total_deploying = 0
            total_to_running = 0
            total_to_delete = 0
            total_deploy_fail = 0
            total_deploy_fail_ids = []
            total_deleted = 0
            sample_deploying = []
            for app_id, app_info in all_apps.iteritems():
                required_num_of_replications = app_info.app_spec["app_attribute"]["replicas"]
                running = []
                to_running = []
                deploying = []
                deploy_fail = []
                to_delete = []
                deleted = []
                unknown = []
                for ref_container in app_info.containers:
                    inst_id = ref_container['id']['groupId']['groupName'] \
                            + str("_") \
                            + str(ref_container['id']['name']).zfill(10)
                    all_state = app_info.instance_state[inst_id]
                    total_instance += 1
                    if not 'visible' in all_state:
                        all_state['visible'] = True
                    if all_state['visible']:
                        if all_state['ns_state'] == 'NEW':
                            #fail to deploy
                            if (all_state['run_state'] == 'DEPLOYFAIL'
                                    or all_state['run_state'] == 'STOP'
                                    or all_state['run_state'] == 'REPAIR'):
                                deploy_fail.append(inst_id)
                                all_fail_instance[inst_id] = None
                            #deploy successfully
                            elif all_state['run_state'] == 'RUNNING':
                                to_running.append(inst_id)
                                all_success_instance[inst_id] = None
                                #some error may be handled by repair
                                if inst_id in all_fail_instance:
                                    del all_fail_instance[inst_id]
                            #deploying
                            else:
                                deploying.append(inst_id)
                                if len(sample_deploying) < 20:
                                    sample_deploying.append(inst_id)
                        elif all_state['ns_state'] == 'TO_BE_DELETED':
                            to_delete.append(inst_id)
                        elif all_state['ns_state'] == 'DELETED':
                            deleted.append(inst_id)
                        elif all_state['ns_state'] == 'RUNNING':
                            #running
                            running.append(inst_id)
                        else:
                            unknown.append(inst_id)
                    else:
                        if all_state['ns_state'] == 'TO_BE_DELETED':
                            to_delete.append(inst_id)
                        elif all_state['ns_state'] == 'DELETED':
                            deleted.append(inst_id)
                        else:
                            to_delete.append(inst_id)
                            logging.warning('status error instance[%s] visible[%r] ns_state[%d]',
                                    inst_id, all_state['visible'], all_state['ns_state'])

                if (required_num_of_replications !=
                        len(running) + len(to_running) + len(deploying) + len(deploy_fail)):
                    instance_error_app[app_id] = {
                        'replications': required_num_of_replications,
                        'running':running, 'to_running':to_running, 'deploying':deploying,
                        'deploy_fail':deploy_fail, 'to_delete':to_delete, 'deleted':deleted,
                        'unknown':unknown}
                elif app_id in instance_error_app:
                    del instance_error_app[app_id]
                    
                if required_num_of_replications != len(running) + len(to_running):
                    running_instance_error_app[app_id] = {
                        'replications': required_num_of_replications,
                        'running':running, 'to_running':to_running, 'deploying':deploying,
                        'deploy_fail':deploy_fail, 'to_delete':to_delete, 'deleted':deleted,
                        'unknown':unknown}
                elif app_id in running_instance_error_app:
                    del running_instance_error_app[app_id]

                total_deploying += len(deploying)
                total_to_running += len(to_running)
                total_deploy_fail += len(deploy_fail)
                total_deploy_fail_ids.extend(deploy_fail)
                total_to_delete += len(to_delete)
                total_deleted += len(deleted)

                #if still need check or not
                if len(deploying) > 0:
                    need_check.append(app_id)
                elif len(deploy_fail) > 0:
                    all_fail.append(app_id)

                #do action in NS
                if len(to_running) > 0:
                    self._transfer_to_running(app_info, to_running)
                if force_delete_move_failed:
                    num_delete = len(to_delete)
                else:
                    num_delete = len(to_delete) - len(deploying) - len(deploy_fail)
                if num_delete > 0:
                    self._transfer_to_delete(app_info, to_delete[:num_delete])

            commit_check_json = {}
            commit_check_json['instance_error'] = {}
            commit_check_json['instance_error']['num'] = len(instance_error_app)
            commit_check_json['instance_error']['apps'] = instance_error_app
            commit_check_json['running_instance_error'] = {}
            commit_check_json['running_instance_error']['num'] = len(running_instance_error_app)
            commit_check_json['running_instance_error']['apps'] = running_instance_error_app

            commit_check_json['success'] = {}
            commit_check_json['success']['instance_num'] = len(all_success_instance)
            commit_check_json['success']['app_num'] = 0
            success_app = {}
            for instance in all_success_instance.iterkeys():
                app_id = instance[:instance.rfind('_')]
                if app_id not in success_app:
                    commit_check_json['success']['app_num'] += 1
                    success_app[app_id]= {'num': 0, 'list': []}
                success_app[app_id]['num'] += 1
                success_app[app_id]['list'].append(instance)
            commit_check_json['success']['apps'] = success_app
            
            commit_check_json['fail'] = {}
            commit_check_json['fail']['instance_num'] = len(all_fail_instance)
            commit_check_json['fail']['app_num'] = 0
            fail_app = {}
            for instance in all_fail_instance.iterkeys():
                app_id = instance[:instance.rfind('_')]
                if app_id not in fail_app:
                    commit_check_json['fail']['app_num'] += 1
                    fail_app[app_id]= {'num': 0, 'list': []}
                fail_app[app_id]['num'] += 1
                fail_app[app_id]['list'].append(instance)
            commit_check_json['fail']['apps'] = fail_app
            
            ret, err = self._write_commit_replications_check_file(
                    replications_check_file, instance_error_app, running_instance_error_app,
                    commit_check_json, commit_check_file)
            if ret != 0:
                logging.warn('write commit replications check file failed,'
                        ' error=[%s], task_instance_id=[%s]',
                        err, self._task_instance_id)
            ret, err = self._write_commit_check_file(success_file, all_success_instance,
                    fail_file, all_fail_instance)
            if ret != 0:
                logging.warn('write result file failed, error=[%s], task_instance_id=[%s]',
                        err, self._task_instance_id)
                
            #if still need check, check the failing ones too
            if len(need_check) > 0:
                need_check.extend(all_fail)
            logging.info("commit: num_app_checked=[%d], num_instance=[%d], num_new_to_running=[%d], num_deploying=[%d], num_deploy_fail=[%d], num_mark_delete=[%d], num_app_still_need_check=[%d], task_instance_id=[%s]", len(all_apps), total_instance, total_to_running, total_deploying, total_deploy_fail, total_deleted, len(need_check), self._task_instance_id)
            logging.info("commit: num_deploy_fail=[%d], instance_ids=[%s], task_instance_id=[%s]", len(total_deploy_fail_ids), ','.join(total_deploy_fail_ids), self._task_instance_id)
            logging.info("commit: num_deploying=[%d], sample_instance_ids=[%s], task_instance_id=[%s]", total_deploying, ','.join(sample_deploying), self._task_instance_id)
            if len(need_check) > 0:
                logging.info("commit: to sleep [%d] seconds waiting for deploying, task_instance_id=[%s]", interval_sec, self._task_instance_id)
                logging.info("commit: you can send signal SKIP_CHECK_COMMIT to continue")
                #check 'SKIP_CHECK_COMMIT' signal every 10 seconds, for (interval_sec/10) times
                skip_signal = ref_task_instance.wait('SKIP_CHECK_COMMIT', 10, (interval_sec / 10))
                if skip_signal is not None:
                    logging.info("commit: get signal SKIP_CHECK_COMMIT")
                    break
            #end of while need_check

        return 0

    def step_commit_delete(
            self,
            app_id_list,
            service_id,
            replications_check_file,
            force_delete_unvisible=False):
        ret, all_apps = self.get_all_app_infos(app_id_list, service_id, scope='added')
        if ret != 0:
            logging.warn("dump all app failed, num_app_to_dump=[%d], task_instance_id=[%s]", len(app_id_list), self._task_instance_id)
            return 1

        running_instance_error_app = {}
        for app_id, app_info in all_apps.iteritems():
            required_num_of_replications = app_info.app_spec["app_attribute"]["replicas"]
            running_replications = []
            unrunning_replications = []
            to_delete = []
            for inst_id, all_state in app_info.instance_state.iteritems():
                if not 'visible' in all_state:
                    all_state['visible'] = True
                if all_state['ns_state'] == 'DELETED':
                    to_delete.append(inst_id)
                elif (not all_state['ns_added']) and all_state['run_state'] == 'DEPLOYFAIL':
		#not in ns zk
                    to_delete.append(inst_id)
                elif (force_delete_unvisible and 'visible' in all_state
                        and all_state['visible'] == False):
                    logging.warn("to delete an instance not labeled DELETED, but unvisible, app_instance_id=[%s], task_instance_id=[%s]", inst_id, self._task_instance_id)
                    to_delete.append(inst_id)

                if all_state['visible']:
                    if (all_state['ns_state'] in ['NEW', 'RUNNING']
                            and all_state['run_state'] == 'RUNNING'):
                        running_replications.append(inst_id)
                    else:
                        unrunning_replications.append(inst_id)

            if required_num_of_replications != len(running_replications):
                running_instance_error_app[app_id] = {
                    "replications":required_num_of_replications,
                    "running":running_replications,
                    'unrunning':unrunning_replications}

            logging.info("commit-delete: delete instance, app_id=[%s], num_instance_to_delete=[%d], instance_to_delete=[%s], task_instance_id=[%s]", app_id, len(to_delete), ','.join(to_delete), self._task_instance_id)
            #delete app or app_instance
            if len(to_delete) == 0:
                logging.info("commit-delete: nothing to delete, app_id=[%s], task_instance_id=[%s]", app_id, self._task_instance_id)
                continue
            ret = self._delete_app_instance(app_info, to_delete)
            logging.info("commit-delete: delete app instance, ret=[%d], app_id=[%s], task_instance_id=[%s]", ret, app_id, self._task_instance_id)
            #the item will be deleted in NS if delete successfully in PD
        ret, err = self._write_commit_delete_replications_check_file(
                replications_check_file, running_instance_error_app)
        if ret != 0:
            logging.warn('write commit delete replication check file failed,'
                    ' error=[%s], task_instance_id=[%s]',
                    err, self._task_instance_id)
        return 0

    def _delete_app(self, app_id):
        bh_app = self._context.bhcli_.get_app(app_id)
        ret, err = bh_app.delete()
        if ret != 0:
            logging.warn("delete app failed, task_instance_id=[%s], app_id=[%s]", self._task_instance_id, app_id)
            return ret
        return 0

    def _delete_app_instance(self, app_info, to_delete):
        bh_app = self._context.bhcli_.get_app(app_info.app_id)
        ret, res = bh_app.delete_instances(to_delete)
        if ret != 0:
            logging.warning("reconfig app resource to delete instance failed, task_instance_id=[%s], app_id=[%s]", self._task_instance_id, app_info.app_id)
            return ret
        return 0
 
  
    def _transfer_to_running(self, app_info, inst_list_to_running):
        #{instance_id:{'run_state':state, 'ns_state':state, 'ns_key':(ip, port)}}
        ips = []
        ports = []
        for inst_id in inst_list_to_running:
            ip, port = app_info.instance_state[inst_id]['ns_key']
            ips.append(ip)
            ports.append(port)

        bh_app = self._context.bhcli_.get_app(app_info.app_id)
        ret, err = bh_app.set_ns_items_tag(ips, ports, 'ns_state', 'RUNNING')
        if ret != 0:
            logging.warn("set state RUNNING failed, app_id=[%s], ips=[%s], ports=[%s], task_instance_id=[%s]", app_info.app_id, ','.join(ips), ','.join(ports), self._task_instance_id)
        return ret

    def _transfer_to_delete(self, app_info, inst_list_to_delete):
        ips = []
        ports = []
        for inst_id in inst_list_to_delete:
            ip, port = app_info.instance_state[inst_id]['ns_key']
            ips.append(ip)
            ports.append(port)

        #label DELETED
        bh_app = self._context.bhcli_.get_app(app_info.app_id)
        ret, err = bh_app.set_ns_items_tag(ips, ports, 'ns_state', 'DELETED')
        if ret != 0:
            logging.warn("set state DELETED failed, app_id=[%s], ips=[%s], ports=[%s], task_instance_id=[%s]", app_info.app_id, ','.join(ips), ','.join(ports), self._task_instance_id)
            return ret
        #set to unvisible
        ret, err = bh_app.set_ns_items_visible(ips, ports, False)
        if ret != 0:
            logging.warn("set instance unvisible, app_id=[%s], ips=[%s], ports=[%s], task_instance_id=[%s]", app_info.app_id, ','.join(ips), ','.join(ports), self._task_instance_id)
            return ret
        return 0


    def calc_seq_id(self, all_apps):
        id_map = {}
        for appid, appinfo in all_apps.iteritems():
            maxid = -1
            for container in appinfo.containers:
                id = int(container["id"]["name"])
                if id > maxid:
                    maxid = id
            id_map[appid] = maxid + 1
        return id_map

    def get_new_seq_id(self, appid, id_map):
        id = id_map[appid]
        id_map[appid] = id + 1
        return id

    #apply 'add' and add step in 'move'
    #TODO: reserve tag in move
    def preview_add_actions(self, action_list, sch_options, all_apps):
        """preview add actions
        Args:
            action_list:
            sch_options:
            all_apps:
        Returns:
            failed count of action
        """
        host_to_preview = {}
        id_map = self.calc_seq_id(all_apps)
        index_media=sch_options["media"]
        for action in action_list:
            if action is None:
                continue

            if action.action != 'ADD' and action.action != 'MOVE':
                continue

            if action.partition.id not in all_apps:
                logging.warn("action is invalid, app not found, skip, action=[%s], \
                        task_instance_id=[%s]", action.to_string(), self._task_instance_id)
                continue
            app_info = all_apps[action.partition.id]
            if not app_info.managed:
                logging.warn("try to handle an unmanaged app, skip, action=[%s], \
                        task_instance_id=[%s]", action.to_string(), self._task_instance_id)
                continue

            if self._is_conflicted(app_info.containers, action.dest_node.id):
                #still exec move in apply del
                logging.warn("action is conflicted while adding, maybe already applied, skip, \
                        action=[%s], task_instance_id=[%s]", 
                        action.to_string(), self._task_instance_id)
                continue

            if action.dest_node.id not in host_to_preview:
                host_to_preview[action.dest_node.id] = []

            container = self._get_container_by_action(action, index_media, False)
            if app_info.goal_resource_spec is not None:
                container["conf"]["resource"]["disks"]["workspace"]["sizeMB"] = \
                        app_info.goal_resource_spec["resource"]["disks"]["workspace"]["sizeMB"]
            else:
                container["conf"]["resource"]["disks"]["workspace"]["sizeMB"] = \
                        app_info.resource_spec["resource"]["disks"]["workspace"]["sizeMB"]
            container["id"]["name"] = str(self.get_new_seq_id(action.partition.id, id_map))
 
            host_to_preview[action.dest_node.id].append(container)
            logging.info("action executed, action=[%s], task_instance_id=[%s]", 
                    action.to_string(), self._task_instance_id)
        #preview_info = {}
        #bh_app = self._context.bhcli_.get_app("")
        #for id, info in host_to_preview.iteritems():
        #    ret, res = bh_app.preview_alloc_instance(id, containers=info)
        #    #since RM did not support changing resource spec, so using resource_spec in AM to generate plan and resource_spec in RM to reconfig
        #    preview_info[id] = [ret, res]
        preview_info = self._context.bhcli_.preview_batch(host_to_preview)
        return preview_info

    #apply 'add' and add step in 'move'
    #TODO: reserve tag in move
    def apply_add_actions(self, action_list, sch_options,
            all_apps, alloc_file, disable_empty_storage=False,
            manual_connect=True, offline_mode=False):
        """apply add actions
        Args:
            action_list:
            sch_options:
            all_apps:
            manual_connect
        Returns:
            failed count of action
        """
        app_to_reconfig = {}
        fail_count = 0
        id_map = self.calc_seq_id(all_apps)
        index_media=sch_options["media"]
        for action in action_list:
            if action == None:
                continue

            if action.action != 'ADD' and action.action != 'MOVE':
                continue

            if disable_empty_storage and len(action.storage) == 0:
                fail_count += 1
                action.tag='Failure'
                logging.warn("action is invalid, storage  not allocated, skip, action=[%s %s], \
                        task_instance_id=[%s]", action.action, action.partition.id, 
                        self._task_instance_id)
                continue

            if action.partition.id not in all_apps:
                fail_count += 1
                action.tag='Failure'
                logging.warn("action is invalid, app not found, skip, action=[%s], \
                        task_instance_id=[%s]", action.to_string(), self._task_instance_id)
                continue
            app_info = all_apps[action.partition.id]
            if not app_info.managed:
                fail_count += 1
                action.tag='Failure'
                logging.warn("try to handle an unmanaged app, skip, action=[%s], \
                        task_instance_id=[%s]", action.to_string(), self._task_instance_id)
                continue

            if self._is_conflicted(app_info.containers, action.dest_node.id):
                fail_count += 1
                #still exec move in apply del
                logging.warn("action is conflicted while adding, maybe already applied, skip, \
                        action=[%s], task_instance_id=[%s]", 
                        action.to_string(), self._task_instance_id)
                continue

            container = self._get_container_by_action(action, index_media, offline_mode)
            if app_info.goal_resource_spec is not None:
                container["conf"]["resource"]["disks"]["workspace"]["sizeMB"] = \
                        app_info.goal_resource_spec["resource"]["disks"]["workspace"]["sizeMB"]
            else:
                container["conf"]["resource"]["disks"]["workspace"]["sizeMB"] = \
                        app_info.resource_spec["resource"]["disks"]["workspace"]["sizeMB"]
            container["id"]["name"] = str(self.get_new_seq_id(action.partition.id, id_map))
 
            app_info.containers.append(container)

            if offline_mode:
                inst_key = container["container_instance_id"]
                inst_state = {}
                inst_state['ns_added'] = True
                inst_state['ns_state'] = 'RUNNING'
                inst_state['run_state'] = 'RUNNING'
                inst_state['visible'] = True
                inst_state['ns_key'] = [container["hostId"], 
                        container['allocatedResource']['port']['range']['first']]
                app_info.instance_state[inst_key] = inst_state

            logging.info("action executed, action=[%s], task_instance_id=[%s]", 
                    action.to_string(), self._task_instance_id)
            app_to_reconfig[app_info.app_id] = app_info
        if offline_mode:
            return fail_count
            #state NEW is by default
        alloc_message = {}
        for id, info in app_to_reconfig.iteritems():
            bh_app = self._context.bhcli_.get_app(id)
            ret, res = bh_app.alloc_instance(containers=info.containers)
            #since RM did not support changing resource spec, so using resource_spec in AM to generate plan and resource_spec in RM to reconfig
            if ret != 0:
                fail_count += self._tag_failure(action_list, id)
                logging.warning("add app instances failed, app_id=[%s]", id)
                alloc_message[id] = res
                continue
            operation_result = json.loads(res)
            batch_operate_result = operation_result["result"]
            if "failed" in batch_operate_result:
                for name, failed in batch_operate_result["failed"].iteritems():
                    logging.info("add app instance failed, app_id=[%s], name=[%s], err=[%s]", 
                            id, name, failed["msg"])
                    alloc_message["%s.%s" % (name, id)] = failed["msg"]
                    for container in info.containers:
                        if container["id"]["name"] == name:
                            fail_count += self._tag_failure(action_list, id, container["hostId"])
            logging.info("add app instance, ret=[%s], res=[%s], app_id=[%s]", ret, res, id)
            time.sleep(0.3)
            if manual_connect and not info.added:
                ret, err = bh_app.set_ns_overlay_on_off(True)
                if ret != 0:
                    fail_count += self._tag_failure(action_list, id)
                    logging.warning("set app overlay on failed, app_id=[%s], \
                            task_instance_id=[%s]", id, self._task_instance_id)
                logging.info("set app overlay on, ret=[%d], app_id=[%s], \
                        task_instance_id=[%s]", ret, id, self._task_instance_id)

        try:
            ref_alloc_file = open(alloc_file, 'w')
            for app_id, result in alloc_message.iteritems(): 
                ref_alloc_file.write("%s: %s" % (app_id, result))
                ref_alloc_file.write('\n')
            ref_alloc_file.close()
        except Exception as e:
            logging.warning('write alloc file failed, file=[%s], error=[%s], \
                    task_instance_id=[%s]', alloc_file, e, self._task_instance_id)

        return fail_count

    def _get_container_by_action(self, action, index_media, offline_mode):
        """get container by action info
        Args:
            action:apply_action
            index_media:index_media
        Returns:
            container
        """
        container = self.get_blank_container()
        container["hostId"] = action.dest_node.id
        container["schedulePolicy"]["hostId"] = action.dest_node.id
        container["conf"]["resource"]["disks"]["workspace"]["mountPoint"]["target"] = \
                "/home/work/search/bs_%d" % action.slot_id
        container["conf"]["resource"]["disks"]["workspace"]["mountPoint"]["source"] = \
                action.dest_node.workspace_path
        container["conf"]["resource"]["disks"]["workspace"]["type"] = \
                action.dest_node.workspace_type
        container["id"]["groupId"]["groupName"] = action.partition.id
        if self._context is not None:
            container["id"]["groupId"]["cluster"] = self._context.matrix_cluster_
        for (path, size) in action.storage:
            storage = {}
            storage["type"] = index_media
            storage["mountPoint"] = {}
            storage["mountPoint"]["source"] = path
            storage["mountPoint"]["target"] = \
                    container["conf"]["resource"]["disks"]["workspace"]\
                    ["mountPoint"]["target"] + "/" + path
            storage["sizeMB"] = size
            storage["numInodes"] = 1000
            container["conf"]["resource"]["disks"]["dataDisks"].append(storage)

        #don't add or delete unplan container, if use reconfig, the property not use
        container['action_for_plan'] = True
        if offline_mode:
            container["allocatedResource"] = {}
            container["allocatedResource"]['port'] = {}
            container['allocatedResource']['port']['range'] = {}
            container['allocatedResource']['port']['range']['first'] = action.service_port
            container["agent_port"] = 8765
            container["failure_domain"] = re.compile("^\d+.\d+.\d+")\
                    .match(container["hostId"]).groups()
            container["idc"] = action.dest_node.real_idc
            container["service_unit"] = action.dest_node.idc
            container["container_instance_id"] = "%s_%d_%010d" % (action.partition.type,
                    action.partition.partition_num, random.randint(100, 10000))
            container["time_to_dead"] = 100

        return container

    def _tag_failure(self, action_list, app_id, hostname=None):
        """
        if action do'nt success, set Failure
            Args:
                action_list: all action_list
                app_id: app_id
                hostname: the action's dest_node
        """
        count = 0
        for action in action_list:
            if action == None:
                continue

            if action.partition.id == app_id:
                if hostname is None or (action.dest_node is not None and \
                        hostname == action.dest_node.id):
                    count += 1
                    action.tag = 'Failure'
        return count

    def apply_del_actions(self, action_list, all_apps, offline_mode=False):
        """to set the del and move src instance to TO_BE_DELETED
            Args:
                action_list:the plan list
                all_apps:all managed apps
                offline_mode:for debug
            Returns:
                the action_list's fail_count
        """
        fail_count = 0
        #{app_id:[[ip_list],[port_list]]}
        app_to_set = {}
        for action in action_list:
            if action == None:
                continue

            if action.action != 'DEL' and action.action != 'MOVE':
                continue
            if action.action == 'MOVE' and action.tag == 'Failure':
                action.tag='Failure'
                logging.warn("action was failed in apply_add step, skip, action=[%s], task_instance_id=[%s]", action.to_string(), self._task_instance_id)
                continue
            app_id = action.partition.id
            if action.partition.id not in all_apps:
                action.tag='Failure'
                logging.warn("action is invalid, app not found, skip, action=[%s], task_instance_id=[%s]", action.to_string(), self._task_instance_id)
                continue
            app_info = all_apps[app_id]
            if not app_info.managed:
                action.tag='Failure'
                logging.warn("try to handle an unmanaged app, skip, action=[%s], task_instance_id=[%s]", action.to_string(), self._task_instance_id)
                continue
            ip = action.src_node.id
            port = app_info.get_port_by_ip(ip)
            if port is None:
                action.tag='Failure'
                logging.warn("get instance port by ip failed, ip=[%s], action=[%s], task_instance_id=[%s]", ip, action.to_string(), self._task_instance_id)
                continue
            if app_id not in app_to_set:
                app_to_set[app_id] = []
                app_to_set[app_id].append([])
                app_to_set[app_id].append([])
            app_to_set[app_id][0].append(ip)
            app_to_set[app_id][1].append(port)
            if offline_mode:
                if app_info.del_inst_state_by_ip_port(ip, port) != True:
                    logging.warn("del ns_state app_id=%s ip=%s port=%s failed!", app_id, ip, port)
                if app_info.del_container_by_ip_port(ip, port) != True:
                    logging.warn("del container app_id=%s ip=%s port=%s failed!", app_id, ip, port)
        if offline_mode:
            return fail_count


        #TODO: set to be delete
        for app_id, ns_keys in app_to_set.iteritems():
            bh_app = self._context.bhcli_.get_app(app_id)
            ret, err = bh_app.set_ns_items_tag(ns_keys[0], ns_keys[1], 'ns_state', 'TO_BE_DELETED')
            if ret != 0:
                logging.warn("set state TO_BE_DELETED failed, app_id=[%s], ips=[%s], ports=[%s], task_instance_id=[%s]", app_id, ','.join(ns_keys[0]), ','.join(ns_keys[1]), self._task_instance_id)
                fail_count += len(ns_keys[0])


        return fail_count

class BsSchedulerTask(object):
    def __init__(self, context, service_id, options, ref_task_inst, scheduler_task_helper, run_mode, app_id=None, app_group_id=None):
        self._task_instance_id = ref_task_inst.id()
        self._context = context
        self._options = options

        self._service_id = service_id
        self.app_id_ = app_id
        self.app_group_id_ = app_group_id


        self._task_instance = ref_task_inst
        self._tool = scheduler_task_helper

        #used in running
        self._all_apps = None
        self._all_macs = None
        self._action_list = None
        self._curr_step = 'begin'
        self._app_to_schedule = []
        self._run_mode = run_mode
        self._run_mode_enum = ["deploy", "restore", "rebalance"]

    def __is_null(self, value):
        return (value is None or len(value) == 0)

    def run(self):
        if self._run_mode not in self._run_mode_enum:
            logging.warn("invalid mode [%s], task_instance_id=[%s]", 
                    self._run_mode, self._task_instance_id)
        #TODO:make a tag on zk
        logging.info("description=[begin to exec task], task_instance_id=[%s]", self._task_instance_id)
        #determine scope, if any app to deploy or not
        app_id_list = []
        if self.__is_null(self.app_group_id_) and self.__is_null(self.app_id_):
            ret, app_id_list = self._context.service_manager_.ListAppByService(self._service_id)
        elif not self.__is_null(self.app_group_id_):
            ret, app_id_list = self._context.service_manager_.ListAppByAppGroup(self._service_id, self.app_group_id_)
        if not self.__is_null(self.app_id_) and self.app_id_ not in app_id_list:
            app_id_list.append(self.app_id_)

        if "step" in self._options and len(self._options["step"]) != 0:
            self._task_instance.set_meta_attr("step", self._options["step"])
        if "dump_file" in self._options and len(self._options["dump_file"]) != 0:
            self._task_instance.set_meta_attr("dump_file", self._options["dump_file"])

        #run state-machine untill finish state
        ret, err_msg = (0, '')
        self._curr_step = self._task_instance.get_step()
        while self._curr_step != 'finish':
            if self._curr_step == 'begin':
                if "skip_resize" in self._options and self._options["skip_resize"] == False:
                    self.go_to_step("resize_resource")
                else:
                    self.go_to_step("dump")

            elif self._curr_step == "resize_resource":
                logging.info("description=[begin to resize_resource], step=[%s], \
                        task_instance_id=[%s]", self._curr_step, self._task_instance_id)
                ret, err_msg = self._tool.step_resize(self._service_id)
                if ret != 0:
                    logging.warn('step resize_resource failed, task_instance_id=[%s]', 
                            self._task_instance_id)
                    self._task_instance.set_error(err_msg)
                logging.info("description=[wait for signal RESIZE_OK], step=[%s], \
                        task_instance_id=[%s]", self._curr_step, self._task_instance_id)
                check_signal = self._task_instance.wait('RESIZE_OK', 10)
                logging.info("description=[finish to resize_resource], step=[%s], \
                        task_instance_id=[%s]", self._curr_step, self._task_instance_id)
                self.go_to_step("dump")

            elif self._curr_step == 'dump':
                #dump snapshot
                logging.info("description=[begin to dump], step=[%s], \
                        task_instance_id=[%s]", self._curr_step, self._task_instance_id)
                dump_file = '%s/dump.%s' % (am_global.task_dir, self._task_instance_id)
                self.go_to_step('dump')
                ret, err_msg, self._all_apps, self._all_macs = \
                        self._tool.step_dump(self._service_id, dump_file, self._options)
                if ret != 0:
                    logging.warn('step dump failed, task_instance_id=[%s]', 
                            self._task_instance_id)
                    self._task_instance.set_error(err_msg)
                    return 1, err_msg
                self._task_instance.set_meta_attr('dump_file', dump_file)

                logging.info("description=[finish to dump], step=[%s], task_instance_id=[%s]", 
                        self._curr_step, self._task_instance_id)
                #next step
                self.go_to_step('generate_plan')

            elif self._curr_step == 'generate_plan':
                logging.info("description=[begin to generate plan], step=[%s], \
                        task_instance_id=[%s]", self._curr_step, self._task_instance_id)
                ret = self.recover_dump()
                if ret != 0: break
                #self._all_apps = self._tool.modify_app_resource_spec(self._all_apps, self._options)

                self._app_to_schedule = []
                for app_id in app_id_list:
                    app_info = self._all_apps[app_id]
                    if self._run_mode == 'deploy':
                        if not app_info.added:
                            self._app_to_schedule.append(app_id)
                    elif self._run_mode == 'restore':
                        #TODO:handle instances with ns_state TO_BE_DELETED
                        num_offline = 0
                        for inst_id, inst_state in app_info.instance_state.iteritems():
                            if inst_state["ns_state"] not in ['NEW', 'RUNNING'] \
                                    or inst_state["run_state"] not in ['RUNNING', 'NEW', \
                                    'REPAIR']:
                                num_offline += 1
                        if app_info.added and num_offline > 0:
                            self._app_to_schedule.append(app_id)
                    elif self._run_mode == 'rebalance':
                        self._app_to_schedule = app_id_list
                        break

                if len(self._app_to_schedule) == 0:
                    logging.info("no app to [%s], just finish task, task_instance_id=[%s], \
                            service_id=[%s], app_group_id=[%s], app_id=[%s]", self._run_mode, 
                            self._task_instance_id, self._service_id, 
                            self.app_group_id_, self.app_id_)
                    ret, err_msg = (0, 'no app to %s' % self._run_mode)
                    self.go_to_step('finish', err_msg)
                    break

                plan_file = '%s/plan.%s' % (am_global.task_dir, self._task_instance_id)
                sim_file = '%s/simulate.%s' % (am_global.task_dir, self._task_instance_id)
                simulate_check_file = ('%s/simulate_check_file.%s' %
                        (am_global.task_dir, self._task_instance_id))
                self._action_list, err_msg = self._tool.step_generate_plan(self._all_apps, 
                    self._all_macs, self._options, plan_file, sim_file, simulate_check_file,
                    self._run_mode, self._app_to_schedule)
                if self._action_list is None:
                    logging.warn("fail to generate plan, task_instance_id=[%s]", 
                            self._task_instance_id)
                    self.go_to_step('finish')
                    break
                self._task_instance.set_meta_attr('plan_file', plan_file)
                self._task_instance.set_meta_attr('simulate_file', sim_file)
                self._task_instance.set_meta_attr('simulate_check_file', simulate_check_file)
                logging.info("description=[finish to generate plan], step=[%s], \
                        task_instance_id=[%s]", self._curr_step, self._task_instance_id)
                #next ste
                if "skip_preview" in self._options and self._options["skip_preview"]:
                    self.go_to_step('apply')
                else:
                    self.go_to_step('preview')

            elif self._curr_step == 'preview':
                ret = self.recover_dump()
                if ret != 0: break
                #self._all_apps = self._tool.modify_app_resource_spec(self._all_apps, self._options)
                if self._action_list is None:
                    plan_file = self._task_instance.get_meta_attr('plan_file')
                    if "action_file" not in self._options or len(self._options['action_file']) == 0:
                        self._options["action_file"] = plan_file
                    self.go_to_step('generate_plan')
                    continue

                if self._run_mode == 'restore' \
                        and 'max_move_action' in self._options \
                        and self._options['max_move_action'] > 0 \
                        and len(self._action_list) > self._options['max_move_action']:
                    logging.warn("fail to apply, length of action_list exceeds max_move_action, \
                            run_mode=[%s], len_action_list=[%d], max_move_action=[%d], \
                            task_instance_id=[%s]", self._run_mode, len(self._action_list), \
                            self._options['max_move_action'], self._task_instance_id)
                    self.go_to_step('finish', 'exceed max_move_action')
                    break
                logging.info("description=[wait for signal PREVIEW], step=[%s], \
                        task_instance_id=[%s]", self._curr_step, self._task_instance_id)
                check_signal = self._task_instance.wait('PREVIEW', 10)
                #preview
                logging.info("description=[begin to preview], step=[%s], task_instance_id=[%s]", \
                        self._curr_step, self._task_instance_id)
                prev_file = '%s/prev.%s' % (am_global.task_dir, self._task_instance_id)
                prev_check_file = ('%s/prev_check_file.%s' %
                        (am_global.task_dir, self._task_instance_id))
                ret, res = self._tool.step_preview_actions(self._action_list, \
                        self._options, self._all_apps, prev_file, prev_check_file)
                if ret != 0:
                    logging.warning("there are actions failed to preview, task_instance_id=[%s]", \
                            self._task_instance_id)
                    self.go_to_step('finish', 'some actions failed to preview')
                self._task_instance.set_meta_attr('preview_file', prev_file)
                self._task_instance.set_meta_attr('preview_check_file', prev_check_file)
                self.go_to_step('apply')

            elif self._curr_step == 'apply':
                ret = self.recover_dump()
                if ret != 0: break
                if self._action_list is None:
                    plan_file = self._task_instance.get_meta_attr('plan_file')
                    if "action_file" not in self._options or len(self._options['action_file']) == 0:
                        self._options["action_file"] = plan_file
                    self.go_to_step('generate_plan')
                    continue

                #wait for signal APPLY
                if 'check_plan' in self._options \
                        and self._options['check_plan'] is not None \
                        and self._options['check_plan'] != "" \
                        and self._options['check_plan']:
                    logging.info("description=[wait for signal APPLY], step=[%s], \
                            task_instance_id=[%s]", self._curr_step, self._task_instance_id)
                    check_signal = self._task_instance.wait('APPLY', 10)
                    #ret_ch = check_signal.get_meta_attr('check_result')
                    #if ret_ch is None or int(ret_ch) != 0:
                    #    logging.warn("fail to generate plan, task_instance_id=[%s]", self._task_instance_id)
                    #    self.go_to_step('finish', 'check result failed')
                    #    continue
                #apply
                logging.info("description=[begin to apply], step=[%s], task_instance_id=[%s]", \
                        self._curr_step, self._task_instance_id)
                disable_empty_storage = self._options.get("disable_empty_storage", True)
                alloc_file = '%s/alloc.%s' % (am_global.task_dir, self._task_instance_id)
                ret = self._tool.step_apply_actions(self._action_list, \
                        self._options, self._all_apps, alloc_file, disable_empty_storage)
                if ret != 0:
                    logging.warning("there are actions failed to apply, task_instance_id=[%s]", \
                            self._task_instance_id)
                    self.go_to_step('finish', 'some actions failed to apply')
                self.go_to_step('commit')

            elif self._curr_step == 'commit':
                logging.info("description=[begin to commit], step=[%s], task_instance_id=[%s]", \
                        self._curr_step, self._task_instance_id)
                inter = 1200
                if "commit_interval_sec" not in self._options:
                    logging.warn("commit_interval_sec not found, using 1200 as default, \
                            task_instance_id=[%s]", self._task_instance_id)
                else:
                    inter = int(self._options["commit_interval_sec"])

                success_file = '%s/success_instance.%s' % (am_global.task_dir, self._task_instance_id)
                fail_file = '%s/fail_instance.%s' % (am_global.task_dir, self._task_instance_id)
                replications_check_file = ('%s/replications_check.%s' %
                    (am_global.task_dir, self._task_instance_id))
                commit_check_file = ('%s/commit_check_file.%s' %
                        (am_global.task_dir, self._task_instance_id))
                self._task_instance.set_meta_attr('success_instance_file', success_file)
                self._task_instance.set_meta_attr('fail_instance_file', fail_file)
                self._task_instance.set_meta_attr('commit_check_file', commit_check_file)

                force_delete_move_failed = self._options.get("force_delete_move_failed", True)
                ret = self._tool.step_commit(
                        self._task_instance, 
                        app_id_list, 
                        self._service_id, 
                        inter, 
                        replications_check_file,
                        success_file, 
                        fail_file,
                        commit_check_file,
                        force_delete_move_failed)
                if ret != 0:
                    logging.warning("failed to commit, task_instance_id=[%s]", self._task_instance_id)
                    check_signal = self._task_instance.wait('COMMIT', 10, inter / 10)
                    continue
                    #self.go_to_step('finish', 'failed to commit')

                self.go_to_step("change_dispatch")

            elif self._curr_step == "change_dispatch":
                logging.info("description=[begin to change_dispatch], step=[%s], \
                        task_instance_id=[%s]", self._curr_step, self._task_instance_id)
                logging.info("description=[finish to change_dispatch], step=[%s], \
                        task_instance_id=[%s]", self._curr_step, self._task_instance_id)
                self.go_to_step("commit_delete")

            elif self._curr_step == "commit_delete":
                #manual by default
                if 'manual_delete' not in self._options or self._options['manual_delete'] is not None or self._options['manual_delete']:
                    logging.info("description=[wait for COMMIT_DELETE], step=[%s], task_instance_id=[%s]", self._curr_step, self._task_instance_id)
                    delete_signal = self._task_instance.wait('COMMIT_DELETE', 10)
                logging.info("description=[begin to commit_delete], step=[%s], task_instance_id=[%s]", self._curr_step, self._task_instance_id)
                self._tool.step_commit_delete(
                        app_id_list, self._service_id, replications_check_file)
                if ret != 0:
                    logging.warning("failed to commit, task_instance_id=[%s]", self._task_instance_id)
                    self.go_to_step('finish', 'failed to commit-delete')

                self.go_to_step('finish')

            elif self._curr_step == 'finish':
                logging.info("description=[finished], step=[%s], task_instance_id=[%s]", self._curr_step, self._task_instance_id)
                continue
            else:
                logging.info("step is invalid, exec from beginning, step=[%s], task_instance_id=[%s]", 
                             self._curr_step, self._task_instance_id)
                self.go_to_step('begin')
      

        return 0,''

    def recover_dump(self):
        if self._all_apps is None or self._all_macs is None:
            logging.info("begin to load dump file, task_instance_id=[%s]", self._task_instance_id)
            ret, self._all_apps, self._all_macs = \
                    self._tool.load_dump_file(self._task_instance.get_meta_attr('dump_file'))
            if ret != 0:
                logging.warn("fail to load dump file, task_instance_id=[%s]", self._task_instance_id)
                self.go_to_step('finish', 'load dump file failed')
                return 1
            logging.info("finish to load dump file, task_instance_id=[%s]", self._task_instance_id)
        return 0

    def go_to_step(self, to_step, err_msg=None):
        self._curr_step = to_step
        self._task_instance.set_step(self._curr_step)
        if err_msg is not None:
            self._task_instance.set_error(err_msg)

    def recover_action_list(self):
        logging.info("begin to load action_list")


