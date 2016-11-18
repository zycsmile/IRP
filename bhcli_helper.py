#!/usr/bin/env python
""" Copyright (c) 2013 Baidu.com, Inc. All Rights Reserved
"""

"""utility to access to beehive shell with bhcli
"""

__author__ = 'guoshaodan01@baidu.com (Guo Shaodan)'

import subprocess
import os
import threading
import Queue
import time
import json
import logging
import tempfile
import math
import stat

import am_global
import am_conf

def _remove_tmp(ret, file_path):
    #delete tmp_file while execute success, else, chmod 555
    if ret != 0:
        os.chmod(file_path, stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
        return
    if am_conf.g_conf.bh_debug == 0:#while debug, don't rm file
        os.remove(file_path)

class BHCmdExcutor(threading.Thread):
    """excute a cmd with different id in queue
    """
    def __init__(self, threadName, bhobj, cmd_list, id_queue):
        super(BHCmdExcutor, self).__init__(name = threadName)
        self.bhobj = bhobj 
        self.cmd_list = cmd_list
        self.id_queue = id_queue
        self.results = {}

    def run(self):
        while not self.id_queue.empty():
            id = self.id_queue.get()
            cmd = []
            cmd.extend(self.cmd_list)
            if self.bhobj.node_type == "machine":
                cmd.append("--hostname=%s" % (id))
            elif self.bhobj.node_type == "resource":
                cmd.append("--container_group_id=%s" % id)
            else:
                cmd.append("--%s_id=%s" % (self.bhobj.node_type, id))

            #will keep trying if rpc failing
            if cmd[0] == "tag":
                ret, output = self.bhobj.exec_cmd_notype(cmd)
                self.results[id] = [ret, output]
            else:
                ret, output = self.bhobj.exec_cmd(cmd)
                self.results[id] = [ret, output]
            self.id_queue.task_done()


class ResourcePreviewExecutor(threading.Thread):
    """bhcli resource preview --gro
        Args:
        Returns:
    """
    def __init__(self, threadName, bh_app, id_queue, containers):
        super(ResourcePreviewExecutor, self).__init__(name = threadName)
        """create Executer
            Args:
            Returns:
        """
        self.bh_app = bh_app
        self.id_queue = id_queue
        self.containers = containers
        self.results = {}

    def run(self):
        """create Executer
            Args:
            Returns:
        """
        while not self.id_queue.empty():
            id = self.id_queue.get()
            container = self.containers[id]

            #will keep trying if rpc failing
            ret, output = self.bh_app.preview_alloc_instance(id, container)
            self.results[id] = [ret, output]
            self.id_queue.task_done()


class ResourceUpdateExecutor(threading.Thread):
    """bhcli resource preview --gro
        Args:
        Returns:
    """
    def __init__(self, threadName, bh_resource, id_queue, cpu, mem, ssd):
        super(ResourceUpdateExecutor, self).__init__(name = threadName)
        """create Executer
            Args:
            Returns:
        """
        self.bh_resource = bh_resource
        self.id_queue = id_queue
        self.cpu = cpu
        self.mem = mem
        self.ssd = ssd
        self.results = {}

    def run(self):
        """create Executer
            Args:
            Returns:
        """
        while not self.id_queue.empty():
            app_id = self.id_queue.get()
            ret, output = self.bh_resource.describe_container_group(app_id)
            if ret != 0:
                self.results[app_id] = [ret, output]
                self.id_queue.task_done()
                continue
            container_group = json.loads(output)
            if "dataSpace" in container_group["container_group_spec"]["resource"]["disks"].keys():
                container_group["container_group_spec"]["resource"]["disks"]["dataSpace"]\
                        ["sizeMB"] = self.ssd
            container_group["container_group_spec"]["resource"]["cpu"]["numCores"] = self.cpu
            container_group["container_group_spec"]["resource"]["memory"]["sizeMB"] = self.mem
            ret, out = self.bh_resource.update_container_group(app_id, container_group)
            if ret != 0:
                self.results[app_id] = [1, 'update_container_group failed']
                self.id_queue.task_done()
                continue
            ret, out = self.bh_resource.update_containers(app_id, self.cpu, self.mem, self.ssd)
            if ret != 0:
                self.results[app_id] = [1, 'update_containers failed']
            else:
                self.results[app_id] = [ret, output]
            self.id_queue.task_done()


class DumpNsAppExecutor(threading.Thread):
    """bhcli naming dump_ns_app --ns_apps='app1,app2'
    """
    def __init__(self, threadName, bh_app, id_queue):
        super(DumpNsAppExecutor, self).__init__(name = threadName)
        self.bh_app = bh_app
        self.id_queue = id_queue
        self.results = {}

    def run(self):
        while True:
            try:
                id_list = self.id_queue.get_nowait()
            except Queue.Empty:
                break

            if len(id_list) == 0:
                self.id_queue.task_done()
                continue

            id_list_len = len(id_list)
            id_list_str = ','.join(id_list)

            ret, output = self.bh_app.dump_ns_app(id_list)
            if ret != 0:
                err = "execute dump_ns_app(%s) failed" % id_list_str
                self.results[id_list[0]] = [ret, err]
                self.id_queue.task_done()
                continue

            try:
                ns_app_dump = json.loads(output)
            except Exception as ex:
                err = "json load result of dump_ns_app(%s) failed: %s" % (id_list_str, ex)
                self.results[id_list[0]] = [-1, err]
                self.id_queue.task_done()
                continue

            app_items_array = ns_app_dump['app_items_array']
            apps = ns_app_dump['apps']
            if len(app_items_array) != id_list_len or len(apps) != id_list_len:
                err = "invalid result of dump_ns_app(%s): item count not matched" % id_list_str
                self.results[id_list[0]] = [-1, err]
                self.id_queue.task_done()
                continue

            for i in range(id_list_len):
                id = id_list[i]
                app = apps[i]
                items = app_items_array[i]['items']
                self.results[id] = [0, app, items]

            self.id_queue.task_done()


class BHFuncExcutor(threading.Thread):
    """execute a specified method with args in queue
    """
    def __init__(self, threadName, ref_func, args_queue):
        """item[0] in the args_queue will be the id of self.results, {id:[ret, output]}
        """
        super(BHFuncExcutor, self).__init__(name = threadName)
        self.func = ref_func
        self.args_queue = args_queue
        self.results = {}

    def run(self):
        while not self.args_queue.empty():
            args = self.args_queue.get()
            id = args[0]
            ret, re_out = self.func(args)
            self.results[id] = [ret, re_out]
            self.args_queue.task_done()


class BHObject(object):
    def __init__(self, node_type, object_id, manager):
        self.node_type = node_type
        self.context = manager
        self.meta_string = None
        self.meta = {}
        self.set_id(object_id)

        self.cluster_argument=("--cluster=%s" % (self.context.cluster_id))

        self.action_list='list'
        self.action_describe='describe'

    def set_id(self, id):
        self.id = id
        if self.node_type == "machine":
            self.id_argument=("--hostname=%s" % (self.id))
        else:
            self.id_argument=("--%s_id=%s" % (self.node_type, self.id))

    def batch_exec_cmd(self, cmd_list, id_list):
        id_queue = Queue.Queue()
        for id in id_list:
            id_queue.put(id)

        thread_queue = []
        for i in range(20):
            t = BHCmdExcutor(('bh_exe_%d' % i), self, cmd_list, id_queue)
            t.start()
            thread_queue.append(t)

        id_queue.join()
        results = {}
        for t in thread_queue:
            results.update(t.results)
        return results


    def exec_cmd(self, cmd_list, retry_times=1, retry_interval_sec=0):
        cmd=[self.node_type]
        cmd.extend(cmd_list)
        return self.exec_cmd_notype(cmd, retry_times, retry_interval_sec)

    def exec_cmd_notype(self, cmd_list, retry_times=1, retry_interval_sec=1):
        ret = 0
        output = None
        
        cmd=[self.context.bhcli]
        cmd.extend(cmd_list)
        cmd.append(self.cluster_argument)

        while retry_times > 0 or ret == am_global.bh_rpc_fail:
            try:
                process = subprocess.Popen(cmd, stdout=subprocess.PIPE, 
                        stderr=subprocess.PIPE)
                stdout, stderr = process.communicate()
                ret = process.poll()
                if ret == 0:
                    output = stdout.rstrip()
                    break
                else:
                    output = stderr.rstrip()
                    retry_times -= 1
                    logging.info("execute cmd failed, will keep trying if ret=[%d], cmd=[%s], \
                            output=[%s]", ret, ' '.join(cmd), output)
                    if retry_interval_sec > 0: time.sleep(retry_interval_sec)
            except Exception as er:
                retry_times -= 1
                logging.info("execute cmd failed, shell open failed")
                if retry_interval_sec > 0: time.sleep(retry_interval_sec)

        logging.info("execute cmd finished, cmd=[%s], ret=[%d]", ' '.join(cmd), ret)
        return (ret, output)


    def list(self):
        cmd = [self.action_list]
        return self.exec_cmd(cmd)

    def describe(self):
        cmd = [self.action_describe, self.id_argument]
        return self.exec_cmd(cmd)

    def describe_batch(self, id_list):
        if len(id_list) == 0:
            return {}
        cmd = [self.action_describe]
        if self.node_type == "resource":
            cmd.append("--include_zombie")
        return self.batch_exec_cmd(cmd, id_list)
    

class Machine(BHObject):
    def __init__(self, slavenode_id, manager):
        super(Machine, self).__init__('machine', slavenode_id, manager)

        self.action_get_capable='get-capable'
        self.action_list_instance='list-instance'
        self.action_batch_describe='batch-describe'

    def list_by_labels(self, tags):
        """bhcli machine list
        Args:
            tags:tag list string with | & !
        Returns:
            exec cmd display
        """
        cmd = [self.action_list, ('--tag=%s' % tags)]
        ret, out = self.exec_cmd(cmd)
        return ret, out

    def batch_describe(self, hosts=None):
        """bhcli machine batch-describe not one by one
        Args:
            hosts: array of hostId
        Returns:
            hostInfos from matrix
        """
        if hosts is None:
            hosts = []

        hostInfos = []

        i_cnt = 0
        tmp_hosts = []
        for host in hosts:
            i_cnt += 1
            tmp_hosts.append(host)
            if i_cnt == 100:
                ret, out = self.batch_describe_once(tmp_hosts)
                tmp_hosts = []
                i_cnt = 0
                if ret == 0:
                    try:
                        macs = json.loads(out)
                        hostInfos.extend(macs["batchOperationResult"]["successfulOperations"])
                    except Exception as e:
                        continue

        if len(tmp_hosts) != 0:
            ret, out = self.batch_describe_once(tmp_hosts)
            if ret == 0:
                try:
                    macs = json.loads(out)
                    hostInfos.extend(macs["batchOperationResult"]["successfulOperations"])
                except Exception as e:
                    tmp_hosts = []

        return 0, hostInfos

    def batch_describe_once(self, hosts=None):
        """bhcli machine batch-describe not one by one
        Args:
            hosts: array of hostId
        Returns:
            exec cmd display
        """
        if hosts is None:
            hosts = []

        try:
            req_file_fd, req_file_path = tempfile.mkstemp(prefix="batch_describe.", \
                    suffix=".tmp", dir=am_global.tmp_dir)
        except Exception as e:
            logging.error("create temp file for cmd parameter filed, object=[%s], \
                    action=[%s], id=[%s], error=[%s]", self.node_type, \
                    self.action_batch_describe, self.id, e)
            return 1, 'create temp file failed'
        req={}
        req["string_list"] = hosts
        os.write(req_file_fd, json.dumps(req))
        os.close(req_file_fd)
        cmd = [self.action_batch_describe, ('--hosts=%s' % req_file_path)]
        ret, out = self.exec_cmd(cmd)
        _remove_tmp(ret, req_file_path)
        return ret, out

    def get_capable(self, and_filters=[], or_filters=[], reserved_time=0):
        try:
            req_file_fd, req_file_path = tempfile.mkstemp(prefix="get_capable_machine.", suffix=".tmp", dir=am_global.tmp_dir)
        except Exception, e:
            logging.error("create temp file for cmd parameter filed, object=[%s], action=[%s], id=[%s], error=[%s]", self.node_type, self.action_get_capable, self.id, e)
            return 1, 'create temp file failed'
        req={}
        req["and_filters"] = and_filters
        req["or_filters"] = or_filters
        req["reserved_time"] = reserved_time
        os.write(req_file_fd, json.dumps(req))
        os.close(req_file_fd)

        cmd = [self.action_get_capable, ('--filter=%s' % req_file_path)]
        ret, out = self.exec_cmd(cmd)
        _remove_tmp(ret, req_file_path)
        return ret, out


class Resource(BHObject):
    """
    Beehive Resource Class
    """
    def __init__(self, container_group_id, manager):
        super(Resource, self).__init__('resource', container_group_id, manager)

        self.action_describe_container_group = 'describe-container-group'
        self.action_update_container_group = 'update-container-group'
        self.action_describe_containers = 'describe-containers'
        self.action_update_containers = 'update-containers'

    def describe_container_group(self, group_id):
        """
        describe_container_group
        """
        cmd = [self.action_describe_container_group, ('--group=%s' % group_id)]
        ret, out = self.exec_cmd(cmd)
        return ret, out

    def describe_containers(self, group_id):
        """
        describe_containers
        """
        cmd = [self.action_describe_containers, ('--group=%s' % group_id)]
        ret, out = self.exec_cmd(cmd)
        return ret, out

    def update_container_group(self, group_id, container_group):
        """
        update_container_group
        """
        try:
            resource_file_fd, resource_file_path = tempfile.mkstemp(
                    prefix="resource_%s_spec." % group_id, suffix=".tmp", dir="../data/tmp")
        except Exception as e:
            logging.error("create temp file for cmd parameter filed, object=[resource], \
                    action=[%s], id=[%s], error=[%s]", self.update_container_group, self.id, e)
            return 1, ""

        os.write(resource_file_fd, json.dumps(container_group['container_group_spec']))
        os.close(resource_file_fd)

        cmd = [self.action_update_container_group, ('--group=%s' % group_id), 
            ('--container_group_spec=%s' % resource_file_path)]
        ret, out = self.exec_cmd(cmd, 3)
        _remove_tmp(ret, resource_file_path)
        return ret, out

    def update_containers(self, group_id, cpu, mem, ssd):
        """
        update_containers
        """
        ret, containers = self.describe_containers(group_id)
        if containers.strip() == "":
            logging.error("app [%s] has no containers", group_id)
            return 1, "describe container error"
        jc = json.loads(containers)
        resource={}
        up_containers = []
        for container in jc["containerInfos"]:
            up_resource = container["conf"]["resource"]
            if cpu != 0:
                up_resource["cpu"]["numCores"] = cpu
            if mem != 0:
                up_resource["memory"]["sizeMB"] = mem
            if ssd != 0:
                if "dataSpace" in up_resource["disks"]:
                    up_resource["disks"]["dataSpace"]["sizeMB"] = ssd
                else:
                    count = len(up_resource["disks"]["dataDisks"])
                    if count == 0:
                        logging.error("app [%s] disk resource is error", group_id)
                        continue
                    perssd = int(math.floor(float(ssd) / count))
                    for disk in up_resource["disks"]["dataDisks"]:
                        disk["sizeMB"] = perssd
            del up_resource["port"]
            conf = {}
            conf["resource"] = up_resource
            up_container={}
            up_container["conf"] = conf
            up_container["id"] = container["id"]
            up_containers.append(up_container)
        resource["containers"] = up_containers
        

        try:
            resource_file_fd, resource_file_path = tempfile.mkstemp(
                    prefix="containers_%s_spec." % group_id, suffix=".tmp", dir="../data/tmp")
        except Exception as e:
            logging.error("create temp file for cmd parameter filed, object=[resource], \
                    action=[%s], id=[%s], error=[%s]", self.update_container_group, self.id, e)
            return 1, ""
            
        os.write(resource_file_fd, json.dumps(resource))
        os.close(resource_file_fd)

        cmd = [self.action_update_containers, ('--containers=%s' % resource_file_path)]
        ret, out = self.exec_cmd(cmd, 3)
        _remove_tmp(ret, resource_file_path)
        return ret, out

class App(BHObject):
    def __init__(self, app_id, manager):
        super(App, self).__init__('app', app_id, manager)

        self.action_add = "add"
        self.action_alloc = "alloc"
        self.action_delete = "delete"
        self.action_set_ns_lock = "set-ns-lock"
        self.action_reconfig = "reconfig"
        self.action_preview = "preview"

        self.subobject_resource = "resource"
        self.subobject_tag = "tag"
        self.subobject_instance = "app-instance"

        self.subobject_naming = "naming"
        self.action_set_ns_overlay = "set_ns_overlay"
        self.action_set_ns_overlay_on_off = "set_ns_overlay_on_off"
        self.action_dump_ns_overlay = "dump_ns_overlay"
        self.action_dump_ns_app = "dump_ns_app"
        self.action_set_ns_items_visible = "set_ns_items_visible"
        self.action_set_ns_items_tag = "set_ns_items_tag"
        self.action_get_ns_tag_items = "get_ns_tag_items"
 
    def get_attribute_batch(self, id_list):
        """get-app_attribute
        Args:
            id_list:batch app
        Returns:
            out: shell display
        """
        if len(id_list) == 0:
            return {}
        cmd = [self.subobject_tag, self.action_get_meta_attr, 
                "--tag_id=null", "--keys=app_spec/app_attribute"]
        return self.batch_exec_cmd(cmd, id_list)   

    def alloc_instance(self, containers=None):
        """bhcli app-instance alloc
        Args:
            containers: array of alloc container
        Returns:
            exec display
        """
        try:
            if containers is not None:
                containers_file_fd, containers_file_path = \
                        tempfile.mkstemp(prefix="%s_containers." % self.id, \
                        suffix=".tmp", dir=am_global.tmp_dir)
        except Exception as e:
            logging.error("create temp file for cmd parameter filed, object=[%s], \
                    action=[%s], id=[%s], error=[%s]", self.subobject_instance, \
                    self.action_alloc, self.id, e)

        cmd = [self.subobject_instance, self.action_alloc, self.id_argument]
        if containers is not None:
            tmp_containers = []
            for container in containers:
                if 'action_for_plan' in container \
                        and container['action_for_plan'] == True:
                    tmp_containers.append(container)
            cmd = [self.subobject_instance, self.action_add, self.id_argument]
            os.write(containers_file_fd, json.dumps({"containers": tmp_containers}))
            os.close(containers_file_fd)
            cmd.append(('--containers=%s' % containers_file_path))

        ret, out = self.exec_cmd_notype(cmd, 3)
        if containers is not None:
            _remove_tmp(ret, containers_file_path)
        return ret, out

    def delete_instances(self, instances=None):
        """bhcli app-instance delete
        Args:
            instances: array of instance_id
        Returns:
            exec display
        """
        if instances is None:
            instances = []
        cmd = [self.subobject_instance, 
                self.action_delete, 
                ('--app_instance_id=%s' % ",".join(instances)), 
                ('--force'), 
                ('--time_to_dead=60'), 
                self.id_argument]
        ret, out = self.exec_cmd_notype(cmd, 3)
        return ret, out

    def preview_alloc_instance(self, dest_node, containers=None):
        """bhcli app-instance preview alloc
        Args:
            containers: array of alloc container
        Returns:
            exec display
        """
        try:
            if containers is None:
                return 0, ""
            else:
                containers_file_fd, containers_file_path = \
                        tempfile.mkstemp(prefix="%s_containers." % dest_node, \
                        suffix=".tmp", dir=am_global.tmp_dir)
        except Exception as e:
            logging.error("create temp file for cmd parameter filed, object=[%s], \
                    action=[%s], id=[%s], error=[%s]", self.subobject_instance, \
                    self.action_alloc, self.id, e)

        tmp_containers = []
        for container in containers:
            if 'action_for_plan' in container \
                    and container['action_for_plan'] == True:
                tmp_containers.append(container)
        cmd = [self.subobject_resource, self.action_preview, "--type=add"]
        os.write(containers_file_fd, json.dumps({"containers": tmp_containers}))
        os.close(containers_file_fd)
        cmd.append(('--containers=%s' % containers_file_path))

        ret, out = self.exec_cmd_notype(cmd, 3)
        _remove_tmp(ret, containers_file_path)

        if ret != 0:
            operation_result = {}
            operation_result["batchOperationResult"] = {}
            failedOperations = list()
            for container in tmp_containers:
                failed_operation = {}
                failed_operation["containerId"] = container["id"]
                failed_operation["errorMessage"] = out
                failedOperations.append(failed_operation)
            operation_result["batchOperationResult"]["failedOperations"] = failedOperations
            return ret, json.dumps(operation_result)

        return ret, out

    def preview_delete_instances(self, containers=None):
        """bhcli app-instance preview delete
        Args:
            containers: array of container_id
        Returns:
            exec display
        """
        if containers is None:
            containers = []
        if len(containers) == 0:
            return 0, ""
        cmd = [self.subobject_resource, 
                self.action_preview, 
                ('--type=del'), 
                ('--container_ids=%s' % ",".join(containers))]
        ret, out = self.exec_cmd_notype(cmd)
        return ret, out

    def reconfig_resource(self, resource_spec, containers=None, async_mode=True, type="reset"):
        try:
            resource_file_fd, resource_file_path = tempfile.mkstemp(prefix="%s_resource_spec." % self.id, suffix=".tmp", dir=am_global.tmp_dir)
            if containers is not None:
                containers_file_fd, containers_file_path = tempfile.mkstemp(prefix="%s_containers." % self.id, suffix=".tmp", dir=am_global.tmp_dir)
        except Exception, e:
            logging.error("create temp file for cmd parameter filed, object=[%s], action=[%s], id=[%s], error=[%s]", self.subobject_resource, self.action_reconfig, self.id, e)

            
        os.write(resource_file_fd, json.dumps(resource_spec))
        os.close(resource_file_fd)

        cmd = [self.subobject_resource, self.action_reconfig, ('--resource_spec=%s' % resource_file_path), self.id_argument]
        if async_mode:
            cmd.append('--async')

        if containers is not None:
            os.write(containers_file_fd, json.dumps({"container_instance" : containers}))
            os.close(containers_file_fd)
            cmd.append(('--containers=%s' % containers_file_path))
            cmd.append(('--reconfig_type=%s' % type))

        ret, out = self.exec_cmd_notype(cmd)
        _remove_tmp(ret, resource_file_path)
        if containers is not None:
            _remove_tmp(ret, containers_file_path)
        return ret, out

    def set_ns_lock(self, num):
        cmd = [self.action_set_ns_lock, self.id_argument, "--ns_lock_num=%d" % num]
        return self.exec_cmd(cmd)

    def add(self, app_spec, min_usable, replicas):
        """bhcli app add
        Args:
            app_spec:app specs
            min_usable:min_usable
            replicas:total_containers
        Returns:
            exec display
        """
        try:
            package_file_fd, package_file_path = tempfile.mkstemp(
                    prefix="package_spec.", suffix=".tmp", dir=am_global.tmp_dir)
            data_file_fd, data_file_path = tempfile.mkstemp(
                    prefix="data_spec.", suffix=".tmp", dir=am_global.tmp_dir)
            resource_file_fd, resource_file_path = tempfile.mkstemp(
                    prefix="resource_spec.", suffix=".tmp", dir=am_global.tmp_dir)
            naming_file_fd, naming_file_path = tempfile.mkstemp(
                    prefix="naming_spec.", suffix=".tmp", dir=am_global.tmp_dir)
        except Exception, e:
            logging.error("create temp file for cmd parameter filed, object=[app], \
                    action=[%s], id=[%s], error=[%s]", self.action_reconfig, self.id, e)

        os.write(package_file_fd, json.dumps(app_spec["package"]))
        os.close(package_file_fd)
        os.write(data_file_fd, json.dumps(app_spec["dynamic_data"]))
        os.close(data_file_fd)
        os.write(resource_file_fd, json.dumps(app_spec["resource"]))
        os.close(resource_file_fd)
        os.write(naming_file_fd, json.dumps(app_spec["naming"]))
        os.close(naming_file_fd)

        cmd = [self.action_add, 
                ('--package_spec=%s' % package_file_path), 
                ('--data_spec=%s' % data_file_path), 
                ('--container_group_spec=%s' % resource_file_path), 
                ('--naming_spec=%s' % naming_file_path), 
                self.id_argument, 
                ("--min_usable=%d" % min_usable), 
                ("--replicas=%d" % replicas)]

        ret, out = self.exec_cmd(cmd)

        _remove_tmp(ret, package_file_path)
        _remove_tmp(ret, data_file_path)
        _remove_tmp(ret, resource_file_path)
        _remove_tmp(ret, naming_file_path)

        return ret, out

    def delete(self, async_mode=True):
        #self.action_delete
        cmd = [self.action_delete, self.id_argument, "--force"]
        if async_mode:
            cmd.append('--async')

        ret, out = self.exec_cmd(cmd)
        return ret, out


    def reconfig_naming(self, naming_spec):
        try:
            naming_file_fd, naming_file_path = tempfile.mkstemp(prefix="naming_spec.", suffix=".tmp", dir=am_global.tmp_dir)
        except Exception, e:
            logging.error("create temp file for cmd parameter filed, object=[%s], action=[%s], id=[%s], error=[%s]", self.subobject_naming, self.action_reconfig, self.id, e)

        os.write(naming_file_fd, json.dumps(naming_spec))
        os.close(naming_file_fd)

        cmd = [self.subobject_naming, self.action_reconfig, ('--naming_spec=%s' % naming_file_path), self.id_argument]

        ret, out = self.exec_cmd_notype(cmd)
        _remove_tmp(ret, naming_file_path)
        return ret, out


    def dump_ns_overlay(self, app_id_list, pretty=False):
        if len(app_id_list) == 0:
            return 1, 'app_id_list is empty'
        app_id_str = ','.join(app_id_list)
        cmd = [self.subobject_naming, self.action_dump_ns_overlay, '--ns_apps=%s' % app_id_str]
        if pretty:
            cmd.append('--pretty')
        ret, out = self.exec_cmd_notype(cmd)
        return ret, out

    def dump_ns_app(self, app_id_list, pretty=False):
        if len(app_id_list) == 0:
            return 1, 'app_id_list is empty'
        app_id_str = ','.join(app_id_list)
        cmd = [self.subobject_naming, self.action_dump_ns_app, '--ns_apps=%s' % app_id_str]
        if pretty:
            cmd.append('--pretty')
        ret, out = self.exec_cmd_notype(cmd, retry_times=2)
        return ret, out

    def set_ns_overlay(self, overlay=None, version=None):
        """set ns overlay
        """
        cmd = [self.subobject_naming, self.action_set_ns_overlay, self.id_argument]

        if overlay is not None:
            try:
                overlay_file_fd, overlay_file_path = tempfile.mkstemp(prefix="overlay.", suffix=".tmp", dir=am_global.tmp_dir)
            except Exception, e:
                logging.error("create temp file for cmd parameter filed, object=[%s], action=[%s], id=[%s], error=[%s]", self.subobject_naming, self.action_reconfig, self.id, e)
            os.write(overlay_file_fd, json.dumps(overlay))
            os.close(overlay_file_fd)
            cmd.append('--overlay_path=%s' % overlay_file_path)
        elif version is not None:
            cmd.append('--overlay_version=%s' % version)

        ret, out = self.exec_cmd_notype(cmd)
        if overlay is not None:
            _remove_tmp(ret, overlay_file_path)

        return ret, out
 
    def set_ns_overlay_on_off(self, on=True):
        if on:
            onoff = 'on'
        else:
            onoff = 'off'
        cmd = [self.subobject_naming, self.action_set_ns_overlay_on_off, self.id_argument, '--overlay_on_off=%s' % onoff]
        ret, out = self.exec_cmd_notype(cmd)
        return ret, out

    def set_ns_items_visible(self, ns_item_ips, ns_item_ports, visible):
        if len(ns_item_ips) != len(ns_item_ports):
            logging.error('count of ip and ports are not equal, count_ip=[%d], count_ports=[%d], %s', len(ns_item_ips), len(ns_item_ports), self.id_argument)
            return 1, "count of ip and ports are not equal"
        if visible:
            vis = 'true'
        else:
            vis = 'false'

        cmd = [self.subobject_naming, self.action_set_ns_items_visible, self.id_argument, '--ns_item_ips=%s' % (','.join(ns_item_ips)), '--ns_item_ports=%s' % (','.join(ns_item_ports)), '--ns_item_visible=%s' % vis]
        ret, out = self.exec_cmd_notype(cmd)
        return ret, out

    def set_ns_items_tag(self, ns_item_ips, ns_item_ports, tag_key, tag_value):
        if len(ns_item_ips) != len(ns_item_ports):
            logging.error('count of ip and ports are not equal, count_ip=[%d], count_ports=[%d], %s', len(ns_item_ips), len(ns_item_ports), self.id_argument)
            return 1, "count of ip and ports are not equal"

        cmd = [self.subobject_naming, self.action_set_ns_items_tag, self.id_argument, '--ns_item_ips=%s' % (','.join(ns_item_ips)), '--ns_item_ports=%s' % (','.join(ns_item_ports)), '--ns_item_tag_key=%s' % tag_key, '--ns_item_tag_value=%s' % tag_value]
        ret, out = self.exec_cmd_notype(cmd)
        return ret, out

    def get_ns_tag_items(self, tag_key, tag_value):
        """
        Returns:
             ret, list in the format of [(ip, port), (ip, port),...]
        """
        cmd = [self.subobject_naming, self.action_get_ns_tag_items, self.id_argument, '--ns_item_tag_key=%s' % tag_key, '--ns_item_tag_value=%s' % tag_value]
        ret, out = self.exec_cmd_notype(cmd)
        if ret != 0:
            return ret, None
        temp_list = out.strip().split()
        result_list = []
        for ip_port_str in temp_list:
            if len(ip_port_str) > 0:
                (ip, port) = ip_port_str.strip().split(':')
                result_list.append((ip, port))
        return ret, result_list

class BHManager:
    def __init__(self, bhcli_process_name, cluster_id):
        self.bhcli = bhcli_process_name
        self.cluster_id = cluster_id

    def get_machine(self, id):
        return Machine(id, self)

    def get_app(self, id):
        return App(id, self)

    def get_resource(self, container_group_id):
        """
        get resource object
        """
        return Resource(container_group_id, self)

    def dump_ns_overlay(self, app_id_list, pretty=False):
        """wrapper of bhcli, naming dump-overlay,
           changing dump to dict in the definition of {app_id:NsOverlay}
        """
        dump_dict = {}
        if len(app_id_list) == 0:
            return dump_dict

        bh_app = self.get_app('')
        ret, dump_str = bh_app.dump_ns_overlay(app_id_list, pretty)

        if ret != 0:
            logging.warn("app dump_ns_overlay failed, ret=[%d], err=[%s]", ret, dump_str)
            return None
        try:
            dump_list = json.loads(dump_str)
        except Exception as e:
            logging.warn("deserialize result of dump_ns_overlay failed, err=[%s], str=[%s]", e, dump_str)
            return None

        dump_dict = {}
        i = 0
        while i < len(dump_list['apps']):
            app_id = dump_list['apps'][i]['app_id']
            dump_dict[app_id] = dump_list['overlays'][i]
            i += 1

        return dump_dict

    def preview_batch(self, host_to_preview, thread_count=20):
        """resource preview batch
            Args:
            Returns:
        """
        if len(host_to_preview.keys()) == 0:
            return {}
        id_queue = Queue.Queue()
        
        for mac_id in host_to_preview.keys():
            id_queue.put(mac_id)

        thread_queue = []
        for i in range(thread_count):
            bh_app = self.get_app('')
            t = ResourcePreviewExecutor(('resource_preview_executor_%d' % i), 
                    bh_app, id_queue, host_to_preview)
            t.start()
            thread_queue.append(t)

        id_queue.join()
        results = {}
        for t in thread_queue:
            results.update(t.results)
        return results

    def update_resource_batch(self, type_app_list, cpu, mem, ssd, thread_count=20):
        """
        update resource batch by app list in one type
        """
        if len(type_app_list) == 0:
            return {}
        id_queue = Queue.Queue()
        for app_id in type_app_list:
            id_queue.put(app_id)

        thread_queue = []
        for i in range(thread_count):
            bh_resource = self.get_resource('')
            t = ResourceUpdateExecutor(('resource_update_executor_%d' % i), bh_resource, 
                    id_queue, cpu, mem, ssd)
            t.start()
            thread_queue.append(t)

        id_queue.join()
        results = {}
        for t in thread_queue:
            results.update(t.results)
        return results

    def dump_ns_app_batch(self, id_list, batch_count=100, thread_count=20):
        """wrapper of bhcli, naming dump-app, in batch
           result is { app_id : [ret, err], app_id : [ret, app, items] }
        """
        if len(id_list) == 0:
            return {}

        # put id into queue
        id_queue = Queue.Queue()
        batch_list = []
        for id in id_list:
            batch_list.append(id)
            if len(batch_list) >= batch_count:
                id_queue.put(batch_list)
                batch_list = []
        if len(batch_list) > 0:
            id_queue.put(batch_list)

        thread_queue = []
        for i in range(thread_count):
            bh_app = self.get_app('')
            t = DumpNsAppExecutor(('dump_ns_app_executor_%d' % i), bh_app, id_queue)
            t.start()
            thread_queue.append(t)

        id_queue.join()
        results = {}
        for t in thread_queue:
            results.update(t.results)
        return results

    def set_ns_overlay(self, app_id_list, version, overlays=None):
        """set overlay if version is different
        Returns:
            ret, error_msg(app_id with error)
        """
        dump_overlays = self.dump_ns_overlay(app_id_list)
        if dump_overlays is None:
            logging.warn("dump ns overlay failed")
            return 1, 'dump ns overlay failed'
        logging.info('dump ns overlays successfully, num_app=[%d]', len(app_id_list))
        app_id_to_set = []
        for app_id, ol in dump_overlays.iteritems():
            if 'version' not in ol:
                logging.warn('overlay formate error, overlay=[%s]', json.dumps(ol))
            if version == ol['version']:
                continue
            app_id_to_set.append(app_id)
        logging.info('determine app to set by overlay version, num_app=[%d], goal_version=[%s]', len(app_id_to_set), version)

        if overlays is None:
            results = self._set_ns_overlay_batch(app_id_to_set, None, version)
            err_id = self._check_results(results)
            if len(err_id) > 0:
                err_msg = ','.join(err_id)
                logging.warn("set ns overlay failed, err_ids=[%s]", err_msg)
                return 1, 'set ns overlay failed, err_ids=[%s]' % err_msg
            logging.info("overlay is not user-specified, "
                    "set ns overlay successfully, version=[%s]", version)
        else:
            for app_id, ol in overlays.iteritems():
                ol['version'] = version
            results = self._set_ns_overlay_batch(app_id_to_set, overlays, None)
            err_id = self._check_results(results)
            if len(err_id) > 0:
                err_msg = ','.join(err_id)
                logging.warn("set ns overlay failed with overlays, err_ids=[%s]", err_msg)
                return 1, 'set ns overlay failed, err_ids=[%s]' % err_msg
            logging.info("overlay is user-specified, "
                    "set ns overlay successfully, version=[%s]", version)
        return 0, ''


    def _check_results(self, results):
        err_id = []
        for id, rets in results.iteritems():
            if rets[0] != 0:
                err_id.append(id)
        return err_id


    def _set_ns_overlay_batch(self, app_id_list, overlays=None, version=None):
        args_queue = Queue.Queue()
        for id in app_id_list:
            args = [id, None, None]
            if overlays is not None and id in overlays:
                args[1] = overlays[id]
            elif version is not None:
                args[2] = version
            args_queue.put(args)

        thread_queue = []
        for i in range(20):
            t = BHFuncExcutor(('bh_func_exe_%d' % i), self._set_overlay_single, args_queue)
            t.start()
            thread_queue.append(t)

        args_queue.join()
        results = {}
        for t in thread_queue:
            results.update(t.results)
        return results

    def _set_overlay_single(self, args_list):
        app_id, overlay, version = args_list
        bh_app = self.get_app(app_id)
        return bh_app.set_ns_overlay(overlay, version)


#wrappers of bhcli
def describe_app(bhcli, app_id):
    """wrapper of bhcli, app describe
    """
    bh_app = bhcli.get_app(app_id)
    ret, desc_str = bh_app.describe()
    if ret != 0:
        logging.warn("app describe failed, maybe is not existed, app_id=[%s]", app_id)
        return None
    try:
        desc = json.loads(desc_str)
    except Exception as e:
        logging.warn("deserialize app description failed, app_id=[%s], err=[%s]", app_id, e)
        return None
    return desc


