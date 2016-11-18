#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" Utility that makes placements for index partitions into a cluster. 
"""

__author__ = 'guoshaodan01@baidu.com (Guo Shaodan)'

import sys
import os
import json
import getopt
import re
import heapq
import math
import logging

import am_global
import am_log

LOG = None

def set_log():
    global LOG
    #init scheduler logger
    log_scheduler_file_path = os.path.join(am_global.log_dir, 'scheduler.log')
    am_log.setup_logger("SCHEDULER_LOG", log_scheduler_file_path)
    LOG = logging.getLogger("SCHEDULER_LOG")
    LOG.info("init scheduler log successfully")


(cpu_index, mem_index, flash_index) = (0, 1, 2)
(fs_fraction, fs_dist, divisor_smooth) = (2, 1, 1)
#TODO: global variable is not safe for multi-threading
default_options =  {
    "index_cross_idc": [],
    "run_mode":"rebalance",
    "slavenode_list" : [],
    "partition_list" : [],
    "output_format" : "bash",
    "group_guide" : {},
    "stable_index_types" : {},
    "allow_move_group" : False,
    "output_index_distr" : {},
    "index_conflict_to_avoid" : {},
    "break_up_dist" : 8,
    "cpu_rate" : 100,
    "media" : "disk",
    "storage_style" : "single",
    "cpu_idle_cross_real_idc" : 0.0,
    "ratio_cross_real_idc" :  0.1,
    "debug" : False,
    "NUM_BS_SLOT_PER_NODE" : 200,
    "BS_SERVICE_BASE_PORT" : 11680,
    "BS_PORT_RANGE" : 20,
    "exclude_error_machine" : True
}
 

def smooth_float(origin, num_fraction, round_dist):
    if origin == 0:
        return origin
    origin = int(origin * pow(10, num_fraction+1))
    round_dist = round_dist * 10
    rem = origin % round_dist
    rrem = round_dist - rem
    if rem >= rrem:
        return (origin + rrem)
    else:
        return (origin - rem)

def oops(fmt, *args):
    '''encounter unexpected error, print msg and exit '''
    fmt+='\n'
    sys.stderr.write(fmt % args)
    sys.exit(1)

new_partition_meta = {
    'state': 'NEW',
    'visible': True,
    'run_state': 'NEW',
    'service_port': '',
    'work_path': ''
}

def is_new_and_running(meta):
    """ judge instance is new and running
    """
    return (('visible' in meta and meta['visible'])
            and ('state' in meta and (meta['state'] in ['NEW', 'RUNNING']))
            and ('run_state' in meta and meta['run_state'] in ['NEW', 'RUNNING', 'REPAIR']))

class IndexPartition:
    """ IndexPartition class.

        Attributes:
            id: partition id. (vip_1, se_2, etc.)
            type: index type. (vip, se, dnews, etc.)
            demond_resource: const vector to describe how much resource needed by the partition
                cpu: number of cpu core needed by this partition
                mem: number of memory resoured needed by this partition. a float number in MB
                size: occupied space of this partition. a float number in MB.
            dominate_share: dominate share of resources
            dominate_index: indicate the dominate resource
            resource_consumed: vector
            replication_factor: how many replicas of this partition is expected.
            nodes: nodes at which replicas of this partition are placed.
            context: IndexPlacementMananger instance.
    """

    def __init__(self, id, type, layer, cpu, mem, size, replication_factor, group, context):
        self.id = id
        self.type = type
        self.layer = layer
        self.resource_demand = (cpu, mem, size)
        self.dominate_index = cpu_index #for index, the dominate resource is always cpu
        self.resource_consumed = [0, 0, 0]
        self.replication_factor = replication_factor
        self.group = group  #the name refereb by external scripts, change it carefully
        self.context = context
        self.nodes = {}
        self.replica_new_and_running = 0
        #self.partition_num = int(id.rsplit('_')[-1])
        self.partition_num = int(id.split('_')[-1])
        self.min_usable = None
        self.to_ignore=False
        self.dominate_resource = cpu_index
        self.can_cross_real_idc = False
        self.is_lightweight = False

    def set_min_usable(self, usable):
        self.min_usable = usable

    def slavenode(self, node_id):
        """ Returns the node instance if this partition is hosted on node_id, 
            or returns None if not.
        """
        if node_id in self.nodes:
            return self.context.get_slavenode(node_id)
        else:
            return None

    def slavenodes(self):
        """ Returns list of the node instance if this partition is hosted on it, 
            or returns [] if not any.
        """
        return [self.slavenode(iter_id) for iter_id in self.nodes]

    def replica_count(self):
        """ Returns actual replica count.  """
        return len(self.nodes)

    def replica_count_of_new_and_running(self):
        """ TO_BE_DELETED replicas are omitted in replica count. 
        >>> hjz=IndexPartition(None,None,None,None,None,None,None)
        >>> hjz.nodes={"a1":"NEW", "a2":"NEW", "a3":"NEW", \
                "b1":"RUNNING", "b2":"RUNNING", \
                "c":"TO_BE_DELETED"}
        >>> hjz.replica_count_of_new_and_running()
        5
        """
        return len([i for i in self.nodes.values() if is_new_and_running(i)])

    def place_at_node(self, node_id, meta):
        """ Place a partition at node with node_id. 
            A partition cannot be placed more than one time at a same node.
        """
        if node_id in self.nodes:
            LOG.warning('partition [%s] already placed at node [%s]', self.id, node_id)
            return False
        self.get_index_group().place_partition(node_id)
        self.nodes[node_id] = meta
        LOG.info('partition [%s:online=%s] placed at node [%s], replica_count_NR [%d], node resource remain[%s]',
                self.id, str(is_new_and_running(meta)), node_id, self.replica_count_of_new_and_running(),
                self.slavenode(node_id).resource_remain_tostring())
        #resource vector updated at place function in Class SlaveNode
        return True

    def displace_from_node(self, node_id):
        """ Displace a partition from node with node_id.  """
        if node_id not in self.nodes:
            LOG.warning('partition [%s] not exist at node [%s]', self.id, node_id)
            return False
        self.get_index_group().displace_partition(node_id)
        meta = self.nodes[node_id]
        tmp_str = self.slavenode(node_id).resource_remain_tostring()
        del self.nodes[node_id]
        LOG.info('partition [%s:online=%s] remove from node [%s], replica_count_NR [%d], node resource remain [%s]',
                self.id, str(is_new_and_running(meta)), node_id, self.replica_count_of_new_and_running(),
                tmp_str)
        #resource vector updated at displace function in Class SlaveNode
        return True
    
    def get_index_group(self):
        return self.context.get_index_group(self.group)
    def get_real_idc(self):
        return self.get_index_group().get_real_idc()
  
    def to_string(self):
        return ', '.join(self.nodes)

class Storage:
    def __init__(self, path, size):
        self.path = path
        self.size = size
    def to_string(self):
        return "%s:%d" % (self.path, self.size)


class SlaveNode:
    """ SlaveNode class. 

        Attributes:
            id: Search node id. (use hostname commonly)
            resource: resource of the machine
            resource_remain: remained resource
                cpu: number of cpu core
                mem: capacity of memory
                capacity: Disk space of search node. only refer to SSD/Flash for simplicity.
            failure_domain: failure domain this node belongs to.
            partitions: partitions which placed at this node.
            context: IndexPlacementMananger instance.
    """

    def __init__(self, id, cpu, mem, capacity, storage_list, failure_domain, online, real_idc, 
                idc, workspace_path, workspace_type, context, agent_available=True, 
                reserve_mem_percent=0):
        self.id = id
        if online == 'offline':
            self.resource_total = (0, 0, 0)
            self.resource_remain = [0, 0, 0]
        else:#online/error
            self.resource_total = (cpu, mem, capacity)
            self.resource_remain = [cpu, mem, capacity]
            #give system reserve mem
            if reserve_mem_percent >= 0 and reserve_mem_percent < 1.0:
                self.resource_remain[mem_index] = self.resource_total[mem_index] * \
                    (1 - reserve_mem_percent)
        self.resource_by_zombie = [0, 0, 0]
        self.cpu_occupied_layer = {}
        self.cpu_occupied_layer['layer1'] = 0
        self.cpu_occupied_layer['layer2'] = 0

        self.resource_consumed_layer = {}
        self.failure_domain = failure_domain
        self.partitions = {}
        self.partitions_new_and_running = []
        self.partitions_offline = []
        #online/offline/error
        #error means: online but on which there is an offline container
        self.online = online
        self.real_idc = real_idc
        self.idc = idc
        self.context = context
        self.balance_failure_tag = False
        self.max_slot_id = -1
        self.occupied_slots = {}

        self.storage = storage_list

        self.workspace_path = workspace_path
        self.workspace_type = workspace_type
        self.agent_available = agent_available
        self.lightweight_inst_num = 0

    def resource_remain_tostring(self):
        return '(cpu:%d mem:%d ssd:%d)' %(self.resource_remain[0],
                self.resource_remain[1], self.resource_remain[2])

    def mark_offline(self):
        if self.online == 'offline':
            return
        else:
            self.online = 'offline'
            self.resource_total = (0, 0, 0)
            self.resource_remain = [0, 0, 0]

    def mark_error(self):
        if self.online == 'online':
            self.online = 'error'

    def occupy_one_slot(self):
        slot_id = 0
        while slot_id < self.context.options["NUM_BS_SLOT_PER_NODE"]:
            if slot_id not in self.occupied_slots:
                break
            slot_id += 1

        if slot_id == self.context.options["NUM_BS_SLOT_PER_NODE"]:
            LOG.warning('allocate slot on slavenode [%s] failed, no more slot to place', self.id)
            return (-1, -1)
        self.occupied_slots[slot_id] = None
        service_port = self.context.options["BS_SERVICE_BASE_PORT"] + slot_id*self.context.options["BS_PORT_RANGE"]

        return (slot_id, service_port)

    def add_zombie_from_bh_container(self, ref_partition, container, storage_only=True):
        if ref_partition != None:
            ref_domain = self.context.failure_domain(self.failure_domain)
            ref_real_idc = self.get_real_idc()
            idx = 0
            while idx < len(ref_partition.resource_demand):
                if idx != flash_index and storage_only:
                    idx += 1
                    continue
                rei = ref_partition.resource_demand[idx]
                self.resource_remain[idx] -= rei
                ref_partition.resource_consumed[idx] += rei
                ref_domain.resource_remain[idx] -= rei
                ref_real_idc.resource_remain[idx] -= rei
                #this zombie is not the offline instance, so it's not the resource_by_zombie
                #which can be return to the actual_resource_remain
                #self.resource_by_zombie[idx] += rei
                idx += 1

        begin_port = container["allocatedResource"]["port"]["range"]["first"]
        #TODO: maybe the port range of zombie is more larger than BS_PORT_RANGE
        slot_id = (begin_port - self.context.options["BS_SERVICE_BASE_PORT"]) \
                / self.context.options["BS_PORT_RANGE"]
        LOG.info("slot occupied by zombie id=[%s], slot_id=[%d], port=[%d]", \
                container["id"], slot_id, begin_port)
        self.occupied_slots[slot_id] = None
        if self.max_slot_id < slot_id:
            self.max_slot_id = slot_id

        path_str = container["allocatedResource"]["disks"]["workspace"]["mountPoint"]["target"]
        try:
            slot_id = int(path_str[path_str.rfind("_")+1:])
        except:
            LOG.info("work_path is not bs type, skipped, work_path=[%s], id=[%s]", \
                    path_str, ref_partition.id)
            return 0
        LOG.info("slot occupied by zombie id=[%s], slot_id=[%d], work_path=[%s]", \
                container["id"], slot_id, path_str)
        self.occupied_slots[slot_id] = None
        if self.max_slot_id < slot_id:
            self.max_slot_id = slot_id


    def place_index_partition(self, partition_id, meta, rollback=False):
        if partition_id in self.partitions:
            oops('partition [%s] already placed at node [%s]', partition_id, self.id)
            LOG.warning('partition [%s] already placed at node [%s]', partition_id, self.id)
            return False

        slot_id = -1
        service_port = -1
        if "service_port" in meta and meta["service_port"] != "":
            service_port = int(meta["service_port"])
        if "work_path" in meta and len(meta["work_path"]) > 0:
            path_str = meta["work_path"]
            slot_id = int(path_str[path_str.rfind("_")+1:])
            self.occupied_slots[slot_id] = None
            if self.max_slot_id < slot_id:
                self.max_slot_id = slot_id
        meta["service_port"] = service_port
        meta["slot_id"] = slot_id

        ref_partition = self.context.get_index_partition(partition_id)
        if not ref_partition.to_ignore:
            self.partitions[partition_id] = meta
        #else, for stable partition, consider resource only

        if is_new_and_running(meta):
            self.partitions_new_and_running.append(ref_partition)
            if ref_partition.is_lightweight:
                self.lightweight_inst_num += 1
        else:
            self.partitions_offline.append(ref_partition)
        LOG.debug('partition [%s] placed at node [%s], node resource remain [%s]',
                partition_id, self.id, self.resource_remain_tostring())

        self.cpu_occupied_layer[ref_partition.layer] += ref_partition.resource_demand[cpu_index]
        if self.online ==  'offline':
            return True
        #update all resource vectors
        ref_domain = self.context.failure_domain(self.failure_domain)
        if ref_partition.id not in ref_domain.index_partitions:
            ref_domain.index_partitions[ref_partition.id] = 0
        ref_domain.index_partitions[ref_partition.id] += 1
        if ref_partition.layer not in self.resource_consumed_layer:
            self.resource_consumed_layer[ref_partition.layer] = [0, 0, 0]
        #DEL rollback mode, DEL only release CPU resource, rollback ADD action only allocate CPU resource
        if rollback:
            for i, rei in enumerate(ref_partition.resource_demand):
                if (i == cpu_index):
                    self.resource_remain[i] -= rei
                    ref_partition.resource_consumed[i] += rei
                    ref_domain.resource_remain[i] -= rei
                    self.resource_consumed_layer[ref_partition.layer][i] += rei
                else:
                    continue
            return True

        for i, rei in enumerate(ref_partition.resource_demand): 
            self.resource_remain[i] -= rei
            ref_partition.resource_consumed[i] += rei
            ref_domain.resource_remain[i] -= rei
            self.resource_consumed_layer[ref_partition.layer][i] += rei
            #resource of real_idc is update when place group in it

        return True

    def displace_index_partition(self, partition_id, reserve_zombie=True, rollback=False):
        """remove index partition from this slave node
        Args:
            reserve_zombie: whether record this zombie's ssd and mem resource to
                resource_consumed_by_zombie
                if True: means actual remove index_partition, add its ssd and mem resource
                to resource_consumed_by_zombie
                else False: means just release it's cpu resource, do not add its ssd and mem
                resource to resource_consumbed_by_zombie, which will be added when call
                add_zombie(consume_resource=False)
                why has this param? for rebalance steps: DEL->execute->ADD->execute->add_zombie
        """
        if partition_id not in self.partitions:
            LOG.warning('partition [%s] not found at node [%s]', partition_id, self.id)
            return False

        ref_partition = self.context.get_index_partition(partition_id)
        meta = self.partitions[partition_id]
        if ref_partition in self.partitions_new_and_running:
            self.partitions_new_and_running.remove(ref_partition)
        if ref_partition in self.partitions_offline:
            self.partitions_offline.remove(ref_partition)


        del self.partitions[partition_id]

        ref_partition = self.context.get_index_partition(partition_id)
        self.cpu_occupied_layer[ref_partition.layer] -= ref_partition.resource_demand[cpu_index]
        if self.online ==  'offline':
            return True
        #update all resource vectors
        ref_partition = self.context.get_index_partition(partition_id)
        ref_domain = self.context.failure_domain(self.failure_domain)
        if ref_partition.id not in ref_domain.index_partitions or ref_domain.index_partitions[ref_partition.id] == 0:
            LOG.warning('index partition count data error in failure_domain [%s], [%s]', ref_domain.id, ref_partition.id)
        else:
            ref_domain.index_partitions[ref_partition.id] -= 1

        #ADD rollback, ADD allocate CPU/SSD/MEM resource, rollback action release all resrouce
        if rollback:
            for i, rei in enumerate(ref_partition.resource_demand):
                self.resource_remain[i] += rei
                ref_partition.resource_consumed[i] -= rei
                ref_domain.resource_remain[i] += rei
                self.resource_consumed_layer[ref_partition.layer][i] -= rei
            return True

        for i, rei in enumerate(ref_partition.resource_demand): 
            if (i == flash_index) and reserve_zombie: #flash resource will not be free after delete partition
                self.resource_by_zombie[i] += rei
                continue
            elif (i == mem_index) and reserve_zombie:
                self.resource_by_zombie[i] += rei
                continue
            elif (i == cpu_index):
                self.resource_remain[i] += rei
                ref_partition.resource_consumed[i] -= rei
                ref_domain.resource_remain[i] += rei
                self.resource_consumed_layer[ref_partition.layer][i] -= rei
            else:
                pass

        return True
    
    def add_zombie(self, ref_partition, meta=None, consume_resource=True):
        """add zombie instance to this slavenode
        
        Args:
            ref_partition: IndexPartition of the zombie instance to be added
            meta: meta info for the instance
            consume_resource: whether this zombie partition consume ssd resource
        """
        ref_domain = self.context.failure_domain(self.failure_domain)
        if consume_resource:
            rei = ref_partition.resource_demand[flash_index]
            self.resource_remain[flash_index] -= rei
            ref_partition.resource_consumed[flash_index] += rei
            ref_domain.resource_remain[flash_index] -= rei
            if ref_partition.layer not in self.resource_consumed_layer:
                self.resource_consumed_layer[ref_partition.layer] = [0, 0, 0]
            self.resource_consumed_layer[ref_partition.layer][flash_index] += rei
            self.get_real_idc().resource_remain[flash_index] -= rei
            self.resource_by_zombie[flash_index] += rei
            self.resource_remain[mem_index] -= ref_partition.resource_demand[mem_index]
            self.resource_by_zombie[mem_index] += ref_partition.resource_demand[mem_index]
        else:
            self.resource_by_zombie[flash_index] += ref_partition.resource_demand[flash_index]
            self.resource_by_zombie[mem_index] += ref_partition.resource_demand[mem_index]
        slot_id = -1
        if meta != None and ("work_path" in meta) and len(meta["work_path"]) > 0:
            path_str = meta["work_path"]
            slot_id = int(path_str[path_str.rfind("_")+1:])
            self.occupied_slots[slot_id] = None
            if self.max_slot_id < slot_id:
                self.max_slot_id = slot_id



    def get_service_state(self, partition_id):
        if partition_id not in self.partitions:
            LOG.warning('partition [%s] not found at node [%s]', partition_id, self.id)
            return False
        return self.partitions[partition_id]["state"]

    def index_partitions_of_new_and_running(self):
        return self.partitions_new_and_running

    def index_partitions_of_offline(self):
        return self.partitions_offline

    def index_partition_count_of_offline(self):
        return len(self.partitions_offline)

    def is_offline_by_index_partition_id(self, partition_id):
        partition_meta = self.meta_of_index_partition(partition_id)
        return (partition_meta is not None and 'run_state' in partition_meta \
                and partition_meta['run_state'] not in ["RUNNING", "NEW", "REPAIR"])

    def meta_of_index_partition(self, partition_id):
        if partition_id not in self.partitions:
            return None
        return self.partitions[partition_id]

    def index_partitions(self):
        return [self.context.get_index_partition(id) 
                    for id in self.partitions]

    def has_index_partition_of_new_and_running(self, partition_id):
        return (partition_id in self.partitions and (self.partitions[partition_id]['state']=='NEW' or self.partitions[partition_id]['state']=='RUNNING'))

    def index_partitions_by_id(self, partition_id):
        return [self.context.get_index_partition(id) 
                    for id in self.partitions 
                    if id == partition_id]

    def index_partitions_by_type_of_new(self, type):
        return [self.context.get_index_partition(id) 
                    for id in self.partitions 
                    if self.context.get_index_partition(id).type == type and (self.partitions[id]['state']=='NEW')]
 
    def index_partitions_by_type_of_running(self, type):
        return [self.context.get_index_partition(id) 
                    for id in self.partitions 
                    if self.context.get_index_partition(id).type == type and (self.partitions[id]['state']=='RUNNING')]
   
    def index_partitions_by_type_of_new_and_running(self, type):
        return [iter_partition 
                    for iter_partition in self.partitions_new_and_running 
                    if iter_partition.type == type ]


    def get_failure_domain(self):
        return self.context.failure_domain(self.failure_domain)
    def get_real_idc(self):
        return self.context.get_real_idc(self.real_idc)

    def to_string(self):
        return ', '.join(self.partitions)

    def cpu_scale_layer(self, layer):
        if layer == 'layer1':
            return (float(self.cpu_occupied_layer['layer1']) / (divisor_smooth+self.cpu_occupied_layer['layer2']))
        else:
            return (float(self.cpu_occupied_layer[layer]) / (divisor_smooth+self.cpu_occupied_layer['layer1']))
       
    def cpu_idl(self, smooth):
        idl = (float(self.resource_remain[cpu_index]) / (divisor_smooth+self.resource_total[cpu_index]))
        if smooth:
            return smooth_float(idl, fs_fraction, fs_dist)
        else:
            return idl

    def cpu_idl_with(self, ref_partition, smooth):
        idl = (float(self.resource_remain[cpu_index] - ref_partition.resource_demand[cpu_index]) / (divisor_smooth+self.resource_total[cpu_index]))
        if smooth:
            return smooth_float(idl, fs_fraction, fs_dist)
        else:
            return idl

    def cpu_idl_without(self, ref_partition, smooth):
        idl = (float(self.resource_remain[cpu_index] + ref_partition.resource_demand[cpu_index]) / (divisor_smooth+self.resource_total[cpu_index]))
        if smooth:
            return smooth_float(idl, fs_fraction, fs_dist)
        else:
            return idl

    def cpu_idl_layer_with(self, ref_partition, smooth):
        cpu_consume = 0
        if ref_partition.layer in self.resource_consumed_layer:
            cpu_consume = self.resource_consumed_layer[ref_partition.layer][cpu_index]
        idl = (float(self.resource_total[cpu_index] - cpu_consume - ref_partition.resource_demand[cpu_index]) / (divisor_smooth+self.resource_total[cpu_index]))
        if smooth:
            return smooth_float(idl, fs_fraction, fs_dist)
        else:
            return idl

    def cpu_idl_layer_without(self, ref_partition, smooth):
        cpu_consume = self.resource_consumed_layer[ref_partition.layer][cpu_index]
        idl = (float(self.resource_total[cpu_index] - cpu_consume + ref_partition.resource_demand[cpu_index]) / (divisor_smooth+self.resource_total[cpu_index]))
        if smooth:
            return smooth_float(idl, fs_fraction, fs_dist)
        else:
            return idl
    def actual_resource_remain(self, resource_idx):
        return (self.resource_remain[resource_idx] + self.resource_by_zombie[resource_idx])
    def actual_resource_remain_with(self, resource_idx, ref_partition):
        return (self.resource_remain[resource_idx] + self.resource_by_zombie[resource_idx] - ref_partition.resource_demand[resource_idx])

    def get_candidate_partitions(self, index):
        """find candidate partitions to eliminate ssd or memory negative resource
        Args:
            index: mem_index or flash_index
        Returns:
            candidates: a list of candidate partitions
        """
        if self.actual_resource_remain(index) >= 0:
            LOG.info("Node[%s] get_candidate_partitions resource_remain[%d]"
            "is enough(>0), not candidate partition", self.id, index)
            return None
        sorted_partitions = []
        for ref_partition in self.partitions_new_and_running:
            # here we all adjust lightweight partition
            sorted_partitions.append((ref_partition.resource_demand[index],
                ref_partition.resource_demand[cpu_index], ref_partition))

        if len(sorted_partitions) == 0:
            LOG.info("Nodep[%s] get_candidate_partition failed, none candidate partitions",
                self.id)
            return None
        sorted_partitions.sort()
        candidate_partitions = []
        last_accumulate_resource = 0
        while True:
            for resource_demand, cpu_demand, iter_partition in sorted_partitions:
                if self.actual_resource_remain(index) + last_accumulate_resource + \
                    resource_demand >= 0:
                    candidate_partitions.append(iter_partition)
                    return candidate_partitions
            last_accumulate_resource += sorted_partitions[-1][0]
            candidate_partitions.append(sorted_partitions[-1][2])
            del sorted_partitions[-1]
            if len(sorted_partitions) == 0:
                break
        LOG.info("Node[%s] get_candidate_partitions failed, all New and Running"
            " Partitions has no enough resource[%d], move none", self.id, index)
        return None

class FailureDomain:
    A_VERY_LARGE_NUMBER = 10.0 ** 8
    def __init__(self, id, context):
        self.id = id
        self.nodes = {}
        self.index_partitions = {}
        self.context = context
        #cpu, memory, flash
        self.resource_total = [0, 0, 0]
        self.resource_remain = [0, 0, 0]

    def add_slavenode(self, node_id):
        if node_id in self.nodes:
            LOG.warning('failure domain [%s] already has node [%s]', self.id, node_id)
            return False

        self.nodes[node_id] = None
        ref_node = self.context.get_slavenode(node_id)
        for i, rei in enumerate(ref_node.resource_total): 
            self.resource_total[i] += rei
            self.resource_remain[i] += ref_node.resource_remain[i]
        return True

    def slavenode(self, node_id):
        if node_id in self.nodes:
            return self.context.get_slavenode(node_id)
        else:
            return None

    def slavenode_count(self):
        return len(self.nodes)

    def index_partition_count(self):
        node_list = [self.context.get_slavenode(id) for id in self.nodes]
        count = 0
        for node in node_list:
            count += len(node.index_partitions())
        return count

    def index_partition_count_by_id(self, partition_id):
        if partition_id not in self.index_partitions:
            return 0
        else:
            return self.index_partitions[partition_id]

    def index_partition_count_by_type_of_new_and_running(self, partition_type):
        node_list = [self.context.get_slavenode(id) for id in self.nodes]
        count = 0
        for node in node_list:
            count += len(node.index_partitions_by_type_of_new_and_running(partition_type))
        return count

    def cpu_idl(self, smooth):
        idl = (float(self.resource_remain[cpu_index]) / (divisor_smooth+self.resource_total[cpu_index]))
        if smooth:
            return smooth_float(idl, fs_fraction, fs_dist)
        else:
            return idl

    def cpu_idl_with(self, ref_partition, smooth):
        idl = (float(self.resource_remain[cpu_index] - ref_partition.resource_demand[cpu_index]) / (divisor_smooth+self.resource_total[cpu_index]))
        if smooth:
            return smooth_float(idl, fs_fraction, fs_dist)
        else:
            return idl

    def cpu_idl_without(self, ref_partition, smooth):
        idl = (float(self.resource_remain[cpu_index] + ref_partition.resource_demand[cpu_index]) / (divisor_smooth+self.resource_total[cpu_index]))
        if smooth:
            return smooth_float(idl, fs_fraction, fs_dist)
        else:
            return idl


class IndexGroup:
    def __init__(self, id, context):
        self.id = id
        self.partitions = {}
        self.real_idc = None
        self.context = context
        self.resource_demand = [0, 0, 0]
        self.cpu_occupied_layer = {}
        self.cpu_occupied_layer['layer1'] = 0
        self.cpu_occupied_layer['layer2'] = 0
        self.dominate_resource = cpu_index
        self.count_instance = 0
        self.real_idc_count_map = {}
        self.no_real_idc_yet = 'no_real_idc_yet'

    def place_at_real_idc(self, real_idc):
        self.real_idc = real_idc

    def displace_from_real_idc(self, real_idc):
        if (self.real_idc != real_idc):
            LOG.warning('displace_from_real_idc: displace index group %s from real_idc %s failed, index group is not placed at it', self.id, real_idc)
        self.real_idc = None
    def add_index_partition(self, ref_partition):
        self.partitions[ref_partition.id] = None
        for i, rei in enumerate(ref_partition.resource_demand):
            self.count_instance += ref_partition.replication_factor
            self.resource_demand[i] += rei*ref_partition.replication_factor
        self.cpu_occupied_layer[ref_partition.layer] += ref_partition.resource_demand[cpu_index]
        if self.count_instance > 0 and self.resource_demand[cpu_index] / self.count_instance <= 10:
            self.dominate_resource = flash_index
        else:
            self.dominate_resource = cpu_index

    def get_all_index_partitions(self):
        return [self.context.get_index_partition(part_id) for part_id in self.partitions.iterkeys()]
    def get_real_idc(self):
        return self.context.get_real_idc(self.real_idc)

    def determine_real_idc(self):
        self.count_instance = 0
        #self.real_idc = None
        self.real_idc_count_map = {}
        if self.real_idc != None:
            self.real_idc_count_map[self.real_idc] = 0
        self.real_idc_count_map[self.no_real_idc_yet] = 0
        for partition_id in self.partitions.iterkeys():
            ref_partition = self.context.get_index_partition(partition_id)
            count_node = 0
            for ref_node in ref_partition.slavenodes():
                count_node+=1
                real_idc_id = ref_node.real_idc
                if real_idc_id not in self.real_idc_count_map:
                    self.real_idc_count_map[real_idc_id] = 0
                self.real_idc_count_map[real_idc_id] += 1
                self.count_instance += 1
            self.real_idc_count_map[self.no_real_idc_yet] += ref_partition.replication_factor - count_node

        max = -1
        for real_idc_id,n in self.real_idc_count_map.iteritems():
            if max < n:
                max = n
                if real_idc_id != self.no_real_idc_yet:
                    self.real_idc = real_idc_id
        if self.real_idc == None:
            LOG.info("determine real_idc [%s] for index_group [%s], which contains [0] instances in main real_idc and [%d] not allocated instances", self.real_idc, self.id, self.real_idc_count_map[self.no_real_idc_yet])
        else:
            LOG.info("determine real_idc [%s] for index_group [%s], which contains [%d] instances in main real_idc and [%d] not allocated instances", self.real_idc, self.id, self.real_idc_count_map[self.real_idc], self.real_idc_count_map[self.no_real_idc_yet])
    def displace_partition(self, node_id):
        if len(self.real_idc_count_map) == 0:
            self.determine_real_idc()
        ref_node = self.context.get_slavenode(node_id)
        real_idc_id = ref_node.real_idc
        self.real_idc_count_map[real_idc_id] -= 1
        self.real_idc_count_map[self.no_real_idc_yet] += 1

    def place_partition(self, node_id):
        if len(self.real_idc_count_map) == 0:
            self.determine_real_idc()
        ref_node = self.context.get_slavenode(node_id)
        real_idc_id = ref_node.real_idc
        self.real_idc_count_map[self.no_real_idc_yet] -= 1
        if real_idc_id not in self.real_idc_count_map:
            self.real_idc_count_map[real_idc_id] = 0
        self.real_idc_count_map[real_idc_id] += 1

    def can_cross_real_idc(self):
        if len(self.real_idc_count_map) == 0:
            self.determine_real_idc()

        if self.dominate_resource != cpu_index:
            return True

        #consider no_real_idc_yet  not in main real idc
        if self.real_idc not in self.real_idc_count_map:
            self.real_idc_count_map[self.real_idc] = 0
        ret = ((float(self.count_instance - self.real_idc_count_map[self.real_idc]) / (self.count_instance+0.001)) < self.context.options["ratio_cross_real_idc"])

        return ret
 
class RealIDC:
    def __init__(self, id, context):
        self.id = id
        self.index_groups = {}
        self.slavenodes = {}
        self.context = context
        self.resource_total = [0, 0, 0]
        self.resource_remain = [0, 0, 0]

        self.cpu_occupied_layer = {}
        self.cpu_occupied_layer['layer1'] = 0
        self.cpu_occupied_layer['layer2'] = 0
        self.balance_failure_tag = False

    def place_index_group(self, ref_group):
        self.index_groups[ref_group.id] = None
        ref_group.place_at_real_idc(self.id)
        for i, rei in enumerate(ref_group.resource_demand):
            self.resource_remain[i] -= rei
        self.cpu_occupied_layer['layer1'] += ref_group.cpu_occupied_layer['layer1']
        self.cpu_occupied_layer['layer2'] += ref_group.cpu_occupied_layer['layer2']

    def displace_index_group(self, ref_group, with_zombie=True):
        if ref_group.id not in self.index_groups:
            LOG.warning('displace_index_group: displace index group %s from real_idc %s failed, index group is not placed at it', ref_group.id, self.id)
        ref_group.displace_from_real_idc(self.id)
        del self.index_groups[ref_group.id]
        for i, rei in enumerate(ref_group.resource_demand):       
            if (i == flash_index and with_zombie): #flash resource will not be free after delete partition
                continue
            elif (i == mem_index):
                continue
            elif (i == cpu_index):
                self.resource_remain[i] += rei
            else:
                pass
        self.cpu_occupied_layer['layer1'] -= ref_group.cpu_occupied_layer['layer1']
        self.cpu_occupied_layer['layer2'] -= ref_group.cpu_occupied_layer['layer2']


    def add_slavenode(self, ref_node):
        self.slavenodes[ref_node.id] = ref_node
        for i, rei in enumerate(ref_node.resource_total):              
            self.resource_total[i] += rei
            self.resource_remain[i] += ref_node.resource_remain[i]

    def cpu_scale_layer(self, layer):
        if layer == 'layer1':
            return (float(self.cpu_occupied_layer['layer1']) / (divisor_smooth+self.cpu_occupied_layer['layer2']))
        else:
            return (float(self.cpu_occupied_layer[layer]) / (divisor_smooth+self.cpu_occupied_layer['layer1']))
 
    #smooth by round
    def cpu_idl(self, smooth):
        idl = (float(self.resource_remain[cpu_index]) / (divisor_smooth+self.resource_total[cpu_index]))
        if smooth:
            return smooth_float(idl, fs_fraction, fs_dist)
        else:
            return idl

    def cpu_idl_with(self, ref_group, smooth):
        idl = (float(self.resource_remain[cpu_index] - ref_group.resource_demand[cpu_index]) / (divisor_smooth+self.resource_total[cpu_index]))
        if smooth:
            return smooth_float(idl, fs_fraction, fs_dist)
        else:
            return idl

    def cpu_idl_without(self, ref_group, smooth):
        idl = (float(self.resource_remain[cpu_index] + ref_group.resource_demand[cpu_index]) / (divisor_smooth+self.resource_total[cpu_index]))
        if smooth:
            return smooth_float(idl, fs_fraction, fs_dist)
        else:
            return idl

    def get_index_groups(self):
        return [self.context.get_index_group(iter_group_id) for iter_group_id in self.index_groups.iterkeys()]

    def get_tail_groups(self, top=0.1):
        """
            generate tail top slavenodes
            Return: list, elemnt: (groupid, cpu_consumed in top slave nodes)
        """
        #get top_num top tail slavenodes
        top_num = int(len(self.slavenodes) * top)
        slavenode_list = []
        for slavenode in self.slavenodes.itervalues():
            if slavenode.online != 'offline':
                slavenode_list.append((slavenode.cpu_idl(smooth=False), slavenode.id))
        slavenode_list.sort()
        top_tail_slavenodes = []
        for index in range(0, top_num):
            slavenode_id = slavenode_list[index][1]
            top_tail_slavenodes.append(self.slavenodes[slavenode_id])
        #get groups consume sum on these slavenodes
        top_cpu_groups = {}
        for slavenode in top_tail_slavenodes:
            for partition in slavenode.partitions_new_and_running:
                if partition.group in top_cpu_groups:
                    top_cpu_groups[partition.group] += partition.resource_demand[cpu_index]
                else:
                    top_cpu_groups[partition.group] = partition.resource_demand[cpu_index]
        top_groups = [(cpu, group_id) for group_id, cpu in top_cpu_groups.iteritems()]
        top_groups.sort(reverse=True)
        #get top-consumed groups
        top_slavenode_cpu = 0
        for slavenode in top_tail_slavenodes:
            top_slavenode_cpu += slavenode.resource_total[cpu_index]
        return [(group_id, cpu / (top_slavenode_cpu * 1.0)) for cpu, group_id in top_groups]

class IndexPlacementAction:
    def __init__(self, action, partition, src_node, dest_node):
        self.action = action
        self.partition = partition
        self.src_node = src_node
        self.dest_node = dest_node
        self.slot_id = None
        self.service_port = None
        self.ori_meta = None
        if src_node != None and partition.id in src_node.partitions:
            self.ori_meta = src_node.partitions[partition.id]
        self.storage = []
        self.tag = None

    def execute(self, reserve_zombie = True):
        ret1, ret2, ret3, ret4 = [False, False, True, True]
        if self.action == 'ADD':
            ret1 = self.dest_node.place_index_partition(self.partition.id, new_partition_meta)
            ret2 = self.partition.place_at_node(self.dest_node.id, new_partition_meta)
        elif self.action == 'DEL':
            ret1 = self.src_node.displace_index_partition(self.partition.id, reserve_zombie)
            ret2 = self.partition.displace_from_node(self.src_node.id)
        elif self.action == 'MOVE':
            ret1 = self.src_node.displace_index_partition(self.partition.id, reserve_zombie)
            ret2 = self.partition.displace_from_node(self.src_node.id)
            ret3 = self.dest_node.place_index_partition(self.partition.id, new_partition_meta)
            ret4 = self.partition.place_at_node(self.dest_node.id, meta=new_partition_meta)
            LOG.info("executin %s: %s, %s, %s, %s", self.to_string(), ret1, ret2, ret3, ret4)
        else:
            pass
        if self.action != 'MOVE':
            LOG.info("executin %s: %s, %s", self.to_string(), ret1, ret2)
        return ret1 and ret2 and ret3 and ret4

    def occupy_slot(self):
        if self.action == 'ADD' or self.action == 'MOVE':
            (self.slot_id, self.service_port) = self.dest_node.occupy_one_slot()
            if self.slot_id == -1 or self.service_port == -1:
                LOG.warning("allocate slot for paritition [%s] on slavenode [%s] failed", self.partition.id, self.dest_node.id)
                return False
            LOG.info("allocate slot for partition [%s] on slavenode [%s] succeed, slot=[%d], service_port=[%d]", self.partition.id, self.dest_node.id, self.slot_id, self.service_port)
            return True
        else:
            return True

    def rollback(self, reserve_zombie = False):
        ret1, ret2, ret3, ret4 = [False, False, True, True]
        if self.action == 'ADD':
            ret1 = self.dest_node.displace_index_partition(self.partition.id, reserve_zombie, rollback=True)
            ret2 = self.partition.displace_from_node(self.dest_node.id)
        elif self.action == 'DEL':
            if self.ori_meta == None:
                return False
            ret1 = self.src_node.place_index_partition(self.partition.id, self.ori_meta, rollback=True)
            ret2 = self.partition.place_at_node(self.src_node.id, self.ori_meta)
        elif self.action == 'MOVE':
            if self.ori_meta == None:
                return False
            ret1 = self.dest_node.displace_index_partition(self.partition.id, reserve_zombie, rollback=True)
            ret2 = self.partition.displace_from_node(self.dest_node.id)
            ret3 = self.src_node.place_index_partition(self.partition.id, self.ori_meta)
            ret4 = self.partition.place_at_node(self.src_node.id, meta=self.ori_meta, rollback=True)
            LOG.info("rollback %s: %s, %s, %s, %s", self.to_string(), ret1, ret2, ret3, ret4)
        else:
            pass
        if self.action != 'MOVE':
            LOG.info("rollback %s: %s, %s", self.to_string(), ret1, ret2)
        return ret1 and ret2 and ret3 and ret4

    def to_string(self):
        str = "%s %s" % (self.action, self.partition.id)
        if (self.action == 'ADD'):
            str += ' %s' % (self.dest_node.id)
        if (self.action == 'DEL'):
            str += ' %s' % (self.src_node.id)
        if (self.action == 'MOVE'):
            str += ' %s %s' % (self.src_node.id, self.dest_node.id)
        if (self.slot_id != None):
            str += ' %d %d' % (self.slot_id, self.service_port)
        return str
    
    def to_json(self):
        json_str = '{"action":"%s", "index_partition_id":"%s""' % (self.action, self.partition.id)
        if (self.action == 'ADD'):
            json_str += ', "dest_slavenode_id":"%s"' % (self.dest_node.id)
        if (self.action == 'DEL'):
            json_str += ', "src_slavenode_id":"%s"' % (self.src_node.id)
        if (self.action == 'MOVE'):
            json_str += ', "src_slavenode_id":"%s", "dest_slavenode_id":"%s"' % (self.src_node.id, self.dest_node.id)
        json_str += '}'
        return json_str


class DRFReplicatePolicy:
    def __init__(self, context):
        self.context = context
        self.add_list = {}
        self.break_up_dist = 0

    def allocate_index_group(self, ref_group):
        scheduled_action_list = []
        if ref_group.real_idc != None:
            LOG.info("group [%s] has already been allocated to real_idc [%s], skipped in allocate_group", ref_group.id, ref_group.real_idc)
            return 0, scheduled_action_list

        reserved_real_idc = None
        if ref_group.id in self.context.options["group_guide"]:
            reserved_real_idc = self.context.options["group_guide"][ref_group.id]
            if reserved_real_idc == "stay":
                LOG.info("group [%s] cannot be allocated due to 'stay' config", ref_group.id)
                return 0, scheduled_action_list

        ref_real_idc = None
        if reserved_real_idc == None:
            heap = [self.__real_idc_sorting_tuple(iter_real_idc, ref_group)
                    for iter_real_idc in self.context.real_idcs.itervalues() if not self.__real_idc_filter(iter_real_idc, ref_group)]
            if len(heap) == 0:
                self.context.output_action_list()
                LOG.info("to allocate group %s failed, %s", ref_group.id, str(ref_group.resource_demand))
                for iter_real_idc in self.context.real_idcs.itervalues():
                    LOG.info("%s, %s", iter_real_idc.id, str(iter_real_idc.resource_remain))

                LOG.error("no valid candidate real_idc to place group %s", ref_group.id)
                return 1, None
            heap.sort() 
            ref_real_idc = heap[-1][-1]
        else:
            LOG.info("allocate group [%s] to real_idc [%s] due to config", ref_group.id, reserved_real_idc)
            ref_real_idc = self.context.get_real_idc(reserved_real_idc)
            if ref_real_idc == None:
                oops("assign %s to %s, but real idc is not existed", ref_group.id, reserved_real_idc)
                
        ref_real_idc.place_index_group(ref_group)

        LOG.info('allocate index_group [%s] to real_idc [%s], real_idc=[%s]' % (ref_group.id, ref_real_idc.id, ref_group.real_idc))
        return 0, scheduled_action_list

    def rebalance_real_idc(self):
        scheduled_action_list = []
        if (self.context.allocation_mode == self.context.ALLOCATION):
            return scheduled_action_list

        while True:
            max_real_idc = self.__get_max_load_real_idc()
            if max_real_idc == None:
                return scheduled_action_list
            cand_groups = [self.__index_group_sorting_tuple_del(max_real_idc, iter_group) for iter_group in max_real_idc.get_index_groups() if not self.__index_group_filter(max_real_idc, iter_group)]
            if self.context.options["debug"]:
                print "max_real_idc:", max_real_idc.id
                print "number of candidate group", len(cand_groups)

            if len(cand_groups) == 0:
                if self.context.options["debug"]:
                    debug_heap = [self.__index_group_sorting_tuple_del(max_real_idc, iter_group) for iter_group in max_real_idc.get_index_groups()]
                    debug_heap.sort()
                    for item in debug_heap:
                        print self.__index_group_filter(max_real_idc, item[-1]), item
                    print "rebalance between real idc end because no cand_groups"
                max_real_idc.balance_failure_tag = True
                continue
                #return scheduled_action_list
            cand_groups.sort()
            if self.context.options["debug"]:
                print 'cand_groups:'
                for item in cand_groups:
                    print item
 
            idx = len(cand_groups) - 1
            while True:
                ref_group = cand_groups[idx][-1]
                if idx == 0 or cand_groups[idx][-1].dominate_resource != cpu_index:
                    if self.context.options["debug"]:
                        print "inner break", "idx:", idx, "group dominate_resource", cand_groups[idx][-1].dominate_resource
                    max_real_idc.balance_failure_tag = True
                    break
                if self.context.options["debug"]:
                    print "index group", ref_group.id, "real_idc:", ref_group.real_idc, "picked, dr:", ref_group.dominate_resource, "resource demand", ref_group.resource_demand
                    print cand_groups[idx]

                action_list = self.rebalance_index_group(ref_group)
                if self.context.options["debug"]:
                    print "rebalance between real idc end"

                if len(action_list) > 0:
                    max_real_idc.balance_failure_tag = False
                    scheduled_action_list.extend(action_list)
                    break
                else:
                    max_real_idc.balance_failure_tag = True
                    idx -= 1
                    continue
                #return scheduled_action_list
        return scheduled_action_list
            
 
    def rebalance_index_group(self, ref_group):
        scheduled_action_list = []
        if ref_group.real_idc == None:
            return scheduled_action_list
        if self.context.options["debug"]:
            print "rebalance index-group begin"
 
        orig_real_idc = self.context.get_real_idc(ref_group.real_idc)
        heap = [self.__real_idc_sorting_tuple(iter_real_idc, ref_group)
                for iter_real_idc in self.context.real_idcs.itervalues() if not self.__real_idc_filter(iter_real_idc, ref_group)]
        if self.context.options["debug"]:
            print "number of candidate real idc", len(heap)

        if len(heap) == 0:
            if self.context.options["debug"]:
                debug_heap = [self.__real_idc_sorting_tuple(iter_real_idc, ref_group) for iter_real_idc in self.context.real_idcs.itervalues()]
                debug_heap.sort()
                for item in debug_heap:
                    print self.__real_idc_filter(item[-1], ref_group), item
                print "rebalance index-group end"
            return scheduled_action_list
        heap.sort() 
        ref_real_idc = heap[-1][-1]
        if self.context.options["debug"]:
            for item in heap:
                print item
            print ('MOVE %s from %s to %s' % (ref_group.id, orig_real_idc.id, ref_real_idc.id))
            print "rebalance index-group end"

        for ref_part in ref_group.get_all_index_partitions():
            for node_id in ref_part.nodes:
                ref_node = ref_part.slavenode(node_id)
                state = ref_node.get_service_state(ref_part.id)
                if state == 'NEW' or  state == 'RUNNING':
                    action = IndexPlacementAction('DEL', ref_part, ref_node, None)
                    scheduled_action_list.append(action)
        for action in scheduled_action_list:
            if False == action.execute():
                oops('action %s execute failed', action.to_string())

        orig_real_idc.displace_index_group(ref_group)
        ref_real_idc.place_index_group(ref_group)
        LOG.info('move index_group [%s] from real_idc [%s] to real_idc [%s]' % (ref_group.id, orig_real_idc.id, ref_real_idc.id))
        #place at dest real_idc
        partition_heap = []
        all_partitions = ref_group.get_all_index_partitions()
        for partition in all_partitions:
            if (not partition.to_ignore) and partition.replication_factor > partition.replica_count_of_new_and_running():
                heapq.heappush(partition_heap, ((not partition.layer == "layer1"), -partition.resource_demand[cpu_index], partition.id, partition))
        heapq.heappush(partition_heap, (True, 1, "None", None))
        (is_layer1, max_ds, par_id, partition) = heapq.heappop(partition_heap) 
        while partition != None:
            (ret, tmp_actions) = self.allocate_index_partition(partition, None, can_cross_real_idc=True)
            if ret == False:
                scheduled_action_list.extend(tmp_actions)
                idx = 1
                #rollback
                while idx <= len(scheduled_action_list):
                    if False == scheduled_action_list[-idx].rollback():
                        oops('action %s rollback failed', scheduled_action_list[-idx].to_string())
                    idx+=1
                ref_real_idc.displace_index_group(ref_group, False)
                orig_real_idc.place_index_group(ref_group)
 
                LOG.warning('move %s from real_idc %s to %s failed, give up this movement', ref_group.id, orig_real_idc.id, ref_real_idc.id)
                scheduled_action_list=[]
                break
            elif len(tmp_actions) > 0:
                scheduled_action_list.extend(tmp_actions)
            (is_layer1, max_ds, part_id, partition) = heapq.heappop(partition_heap)


        return scheduled_action_list

    #will allocate all replica if allocate_once is not indicated
    def allocate_index_partition(self, ref_partition, ref_orig_node, allocate_once=False, 
            can_cross_real_idc=False, fail_if_little_idle=False, cpu_diff_control=True,
            orig_offline=False):
        """increase replica to fullfill replication_factor
        !!!print error and exit, if no enough slot to place instance
        Args:
            ref_partition:the index to alloc
            ref_orig_node:the index's now placed_node
            allocate_once:if alloc once(True) or alloc to replicas(False)
            fail_if_little_idle:
                True:expect little idle nodes
                False:allow all nodes
            orig_offline:True:place at the first satisfy node
        Returns:
            True or False and alloc action list
        """
        LOG.info("allocate_index_partition, index_partition=[%s], allocate_once=[%s], can_cross_real_idc=[%s], fail_if_little_idle=[%s], cpu_diff_control=[%s]", ref_partition.id, str(allocate_once), str(can_cross_real_idc), str(fail_if_little_idle), str(cpu_diff_control))
        scheduled_action_list = []
        ref_real_idc = ref_partition.get_real_idc()
        ref_group = ref_partition.get_index_group()
        main_real_idc_to_avoid = self.real_idc_to_avoid(ref_partition.group)
        while ref_partition.replication_factor - ref_partition.replica_count_of_new_and_running() > 0:
            if ref_partition.dominate_resource != cpu_index:
                heap = [self.__slavenode_sorting_tuple_add(iter_node, ref_partition) 
                    for iter_node in self.context.slavenodes.itervalues() 
                        if (iter_node.real_idc != main_real_idc_to_avoid 
                        and (not self.__slavenode_filter(iter_node, ref_partition))
                        )]
            elif ref_partition.can_cross_real_idc == True:
                heap = [self.__slavenode_sorting_tuple_add(iter_node, ref_partition)
                    for iter_node in self.context.slavenodes.itervalues()
                        if (iter_node.real_idc != main_real_idc_to_avoid
                        and (not self.__slavenode_filter(iter_node, ref_partition))
                        )]

            elif can_cross_real_idc == True:
                heap = [self.__slavenode_sorting_tuple_add(iter_node, ref_partition)
                    for iter_node in self.context.slavenodes.itervalues()
                        if (iter_node.real_idc != main_real_idc_to_avoid
                        and (not self.__slavenode_filter(iter_node, ref_partition))
                        )]
            else:
                heap = [self.__slavenode_sorting_tuple_add(iter_node, ref_partition) 
                        for iter_node in self.context.slavenodes.itervalues() 
                            if ((iter_node.real_idc == ref_real_idc.id or
                                (ref_orig_node is not None and
                                ref_orig_node.real_idc == iter_node.real_idc))
                                and not self.__slavenode_filter(iter_node, ref_partition)
                                and (not fail_if_little_idle or
                                (iter_node.cpu_idl_with(ref_partition, False) >=
                                self.context.options["cpu_idle_cross_real_idc"]))
                            )]

                if ref_group.can_cross_real_idc():
                    LOG.info('try to cross real idc, partition_id=[%s]', ref_partition.id)
                    if len(heap) == 0 or max(heap)[-1].cpu_idl(False) < self.context.options["cpu_idle_cross_real_idc"]:
                        heap_ex = [self.__slavenode_sorting_tuple_add(iter_node, ref_partition) 
                            for iter_node in self.context.slavenodes.itervalues() 
                            if iter_node.real_idc != ref_real_idc.id 
                            and not self.__slavenode_filter(iter_node, ref_partition) 
                            and (ref_orig_node == None or ref_orig_node.id == iter_node.id or ((not cpu_diff_control) or ref_orig_node.cpu_idl_with(ref_partition, True) < iter_node.cpu_idl_with(ref_partition, True))) 
                            and (not fail_if_little_idle or (iter_node.cpu_idl_with(ref_partition, False) >= self.context.options["cpu_idle_cross_real_idc"]))
                            ]
                        heap.extend(heap_ex)
 
            if len(heap) == 0:
                LOG.error('place partition %s failed, number of candidate machine %d, can_cross_real_idc=[%s], group_can_cross_idc=[%s]',
                        ref_partition.id, len(heap), str(can_cross_real_idc), str(ref_group.can_cross_real_idc()))
                LOG.info("partition_id=[%s], resource_demand=[%s], main_idc=[%s], group=[%s]",
                        ref_partition.id, str(ref_partition.resource_demand), ref_real_idc.id, ref_group.id)
                LOG.warning('no valid candidate slavenode to place partition [%s] at real_idc [%s]' % (ref_partition.id, ref_real_idc.id))
                return (False, scheduled_action_list)

            heap.sort()
            i = len(heap) -1
            while i >= 0:
                max_vec = heap[i]
                i -= 1
                ref_node = max_vec[-1]
                ref_domain = ref_node.get_failure_domain()
                if ref_orig_node is None or \
                    cpu_diff_control == False or \
                    (ref_orig_node.online == 'offline' and ref_node.online == 'online') or \
                    (ref_orig_node.id != ref_node.id and orig_offline) or \
                    (ref_orig_node.cpu_idl_with(ref_partition, True) < ref_node.cpu_idl_with(ref_partition, True)):
                        '''²»´æÔÚ×ÊÔ´ÎÊÌâ,×ßÅÐ¶ÏÊÇ·ñfailure_domainÁ÷³Ì'''
                        if ref_domain == False:
                            pass
                        action = IndexPlacementAction('ADD', ref_partition, None, ref_node)
                        scheduled_action_list.append(action)
                        if False == action.execute():
                            LOG.info('action %s execute failed', action.to_string())
                        break
                else:
                    '''×ÊÔ´²»×ã£¬³¢ÊÔ½»»»ÊµÀý'''
                    min_cpu_idle = min(ref_orig_node.cpu_idl_with(ref_partition, True), ref_node.cpu_idl(True))
                    switch_partition = None
                    cpu_idle = -1.0
                    for partition in ref_node.index_partitions_of_new_and_running():
                        tmp_ref_node_cpu_idle = ref_node.cpu_idl_without(partition, True) + ref_node.cpu_idl_with(ref_partition, True) - ref_node.cpu_idl(True)
                        tmp_cpu_idle = min(ref_orig_node.cpu_idl_with(partition, True), tmp_ref_node_cpu_idle)
                        if not self.__slavenode_filter(ref_orig_node, partition) and \
                            (ref_orig_node.get_real_idc() == ref_node.get_real_idc() or \
                            partition.can_cross_real_idc) and \
                            partition.type != ref_partition.type:
                                if tmp_cpu_idle > min_cpu_idle + 15 and tmp_cpu_idle > cpu_idle:
                                    cpu_idle = tmp_cpu_idle
                                    switch_partition = partition
                    if switch_partition == None:
                        continue
                    else:
                        LOG.info('[%s:%s] and [%s:%s] exchange, cpu idle before exchange[%s:%.2f][%s:%.2f], cpu idle after exchange[%s:%.2f][%s:%.2f]',
                                ref_orig_node.id, ref_partition.id, ref_node.id, switch_partition.id,
                                ref_orig_node.id, ref_orig_node.cpu_idl_with(ref_partition, True),
                                ref_node.id, ref_node.cpu_idl(True),
                                ref_orig_node.id, ref_orig_node.cpu_idl_with(switch_partition, True),
                                ref_node.id, ref_node.cpu_idl_without(switch_partition, True) + ref_node.cpu_idl_with(ref_partition, True) - ref_node.cpu_idl(True))
                        action = IndexPlacementAction('MOVE', switch_partition, ref_node, ref_orig_node)
                        scheduled_action_list.append(action)
                        if False == action.execute():
                            LOG.info('action %s execute failed', action.to_string())

                        action = IndexPlacementAction('ADD', ref_partition, None, ref_node)
                        scheduled_action_list.append(action)
                        if False == action.execute():
                            LOG.info('action %s execute failed', action.to_string())
                        break

            if allocate_once:
                 break
        return (True if len(scheduled_action_list) > 0 else False, scheduled_action_list)

    def deallocate_index_partition(self, ref_partition):
        '''decrease replica to meet replication_factor
        '''
        scheduled_action_list = []
        ref_real_idc = ref_partition.get_real_idc()
        if ref_partition.replica_count_of_new_and_running() == 0:
            return scheduled_action_list
        if self.context.options["debug"]:
            print ref_partition.id,ref_partition.replica_count_of_new_and_running()
            print ref_partition.slavenodes()
        while ref_partition.replication_factor - ref_partition.replica_count_of_new_and_running() < 0:
            heap = [self.__slavenode_sorting_tuple_del(iter_node, ref_partition) 
                    for iter_node in ref_partition.slavenodes()]

            heap.sort()
            ref_node = heap[-1][-1]

            action = IndexPlacementAction('DEL', ref_partition, ref_node, None)

            LOG.info('partition %s displaced from %s' % (ref_partition.id, ref_node.id))
            scheduled_action_list.append(action)
            if False == action.execute():
                oops('action %s execute failed', action.to_string())

        return scheduled_action_list

    def real_idc_to_avoid(self, group_id):
        if self.context.options["index_conflict_to_avoid"] == None or len(self.context.options["index_conflict_to_avoid"]) == 0 or group_id not in self.context.options["index_conflict_to_avoid"]['index_group']:
            return None
        max = 0
        real_idc_id = None
        for k,v in self.context.options["index_conflict_to_avoid"]['index_group'][group_id].iteritems():
            if max < v:
                real_idc_id = k
                max = v
        return real_idc_id

    def __get_node_with_least_lightweight_inst(self, ref_patition):
        least_lightweight_inst_num = 99999
        dest_node = None
        for iter_node in self.context.slavenodes.itervalues():
            if not self.__enough_resource(ref_patition.resource_demand, iter_node.resource_remain) \
                    or ref_patition.id in iter_node.partitions:
                continue
            if iter_node.lightweight_inst_num < least_lightweight_inst_num:
                dest_node = iter_node
                least_lightweight_inst_num = iter_node.lightweight_inst_num
        return dest_node

    def scatter_lightweight_inst(self):
        """
        lightweight partition shoulde be scattered within scope of LIDC
        """
        scheduled_action_list = []
        mac_num = 0
        inst_num = 0
        for iter_node in self.context.slavenodes.itervalues():
            if iter_node.online != 'offline':
                mac_num += 1
            inst_num += iter_node.lightweight_inst_num
        max_inst_per_mac = math.ceil(1.0 * inst_num / mac_num) + 1
        LOG.info('got %d machines, got %s lightweight instances, %d lightweight instances per machine', mac_num, inst_num, max_inst_per_mac)

        for iter_node in self.context.slavenodes.itervalues():
            while iter_node.lightweight_inst_num > max_inst_per_mac:
                move_anything = False
                LOG.info('scatter node %s, with %d lightweight insts', iter_node.id, iter_node.lightweight_inst_num)
                for iter_partition in iter_node.index_partitions_of_new_and_running():
                    dest_node = None
                    LOG.debug('scatter node %s, partition=%s', iter_node.id, iter_partition.id)
                    if not iter_partition.is_lightweight:
                        LOG.debug('scatter node %s failed, partition %s is not lightweight', iter_node.id, iter_partition.id)
                        continue
                    dest_node = self.__get_node_with_least_lightweight_inst(iter_partition)
                    if iter_partition is None or dest_node is None or iter_node == dest_node:
                        LOG.info('scatter node %s failed, find new node failed, partition=%s', iter_node.id, iter_partition.id)
                        continue
                    del_action = IndexPlacementAction('DEL', iter_partition, iter_node, None)
                    if False == del_action.execute(False):
                        LOG.info('scatter node %s failed, del partition=%s failed', iter_node.id, iter_partition.id)
                        continue
                    add_action = IndexPlacementAction('ADD', iter_partition, None, dest_node)
                    if False == add_action.execute(False):
                        LOG.info('scatter node %s failed, add partition=%s failed', iter_node.id, iter_partition.id)
                        continue
                    move_anything = True
                    move_action = IndexPlacementAction('MOVE', iter_partition, iter_node, dest_node)
                    scheduled_action_list.append(move_action)

                    iter_node.add_zombie(iter_partition, consume_resource = False)
                    iter_node.lightweight_inst_num -= 1
                    dest_node.lightweight_inst_num += 1
                if not move_anything:
                    LOG.warning('scatter node %s failed, cannot move any partition', iter_node.id)
                    break
        return scheduled_action_list

    def __eliminate_negative_resource(self, slave_node, resource_index, scheduled_action_list):
        """eliminate negative resource for a slave node on resource_index resource
        Args:
            slave_node: slave node to eliminate negative resource
            resource_index: flash_index or mem_index
            scheduled_action_list: actions to extend
        Return:
            Nothing
        """
        candidate_partitions = slave_node.get_candidate_partitions(index=resource_index)
        if candidate_partitions is not None and len(candidate_partitions) > 0:
            for iter_partition in candidate_partitions:
                LOG.info('try to eliminate negative %d resource by partition[%s]',
                    resource_index,
                    iter_partition.id)
                remove_action = IndexPlacementAction('DEL', iter_partition, slave_node, None) 
                if remove_action.execute(False) == False:
                    oops('action %s execute failed', remove_action.to_string())
                (ret, allocate_actions) = self.allocate_index_partition(
                    iter_partition,
                    slave_node,
                    allocate_once = True,
                    can_cross_real_idc = True,
                    fail_if_little_idle = True,
                    cpu_diff_control = False)
                if ret == True:
                    i = 0
                    while i < len(allocate_actions) - 1:
                        if allocate_actions[i].action != 'MOVE':
                            oops("invalid type [%s] get from allocation process"
                                " when move %s, MOVE expected",
                                allocate_actions[i].to_string(), iter_partition.id)
                        scheduled_action_list.append(allocate_actions[i])
                        i += 1
                    add_action = allocate_actions[-1]
                    if add_action.action != 'ADD':
                        oops("invalid type [%s] get from allocation "
                            "process when move %s, ADD expected",
                            allocate_actions[i].to_string(),
                            iter_partition.id)
                    if slave_node.id != add_action.dest_node.id:
                        action = IndexPlacementAction(
                            'MOVE',
                            iter_partition,
                            slave_node,
                            add_action.dest_node)
                        slave_node.add_zombie(iter_partition, consume_resource=False)
                        #do not need to execute
                        scheduled_action_list.append(action)
                        LOG.info("")
                    elif len(allocate_actions) == 1:
                        oops("error happens, move from-to the same slavenode")

                else:
                    if False == remove_action.rollback():
                        oops('action %s rollback failed', remove_action.to_string())
            #end for
        else:
            LOG.warning("Node[%s] eliminate negative resource failed, no candidate partition")

    def rebalance_nodes_mem_and_flash(self, ref_real_idc=None):
        """try to eliminate negative ssd and memory actual resource remain slavenodes
        Args:
            ref_real_idc: which real_idc to eliminate negative resource
        Return:
            ret: sucess or fail, bool
            action_list: action list
        """
        LOG.info("try to eliminate negative ssd and memory actual resource remain slavenodes")
        scheduled_action_list = []
        idc_range = ref_real_idc if ref_real_idc is not None else self.context
        for iter_node in idc_range.slavenodes.itervalues():
            #ssd and mem resource is enough
            if iter_node.online == 'offline' or \
                (iter_node.actual_resource_remain(mem_index) >= 0 and \
                iter_node.actual_resource_remain(flash_index) >= 0):
                continue
            LOG.info("Node[%s] to elimiate negative ssd and memory", iter_node.id)
            if iter_node.actual_resource_remain(mem_index) < 0 and \
                iter_node.actual_resource_remain(flash_index) >= 0:
                self.__eliminate_negative_resource(iter_node, mem_index, scheduled_action_list)
            elif iter_node.actual_resource_remain(flash_index) < 0 and \
                iter_node.actual_resource_remain(mem_index) >= 0:
                self.__eliminate_negative_resource(iter_node, flash_index, scheduled_action_list)
            else:
                LOG.info("Node[%s] very bad slavenode, flash and memory both less than zero",
                    iter_node.id)
                #resotre mem
                self.__eliminate_negative_resource(iter_node, mem_index, scheduled_action_list)
                #restore flash
                if iter_node.actual_resource_remain(flash_index) < 0:
                    self.__eliminate_negative_resource(
                        iter_node,
                        flash_index,
                        scheduled_action_list)
        return scheduled_action_list


    def rebalance_nodes(self, ref_real_idc, nodes_to_clear=None, fail_if_little_idle=False, restore_only=False):
        '''move index partitions between nodes to rebalance
        '''
        scheduled_action_list = []
        for iter_node in self.context.slavenodes.itervalues():
            iter_node.balance_failure_tag = False

        num_loop = 0
        max_loop_num = 0
        if ref_real_idc is None:
            max_loop_num = len(self.context.slavenodes)
        else:
            max_loop_num = len(ref_real_idc.slavenodes)
        if max_loop_num > 3:
            max_loop_num = max_loop_num / 3
        while True:
            num_loop += 1
            if (num_loop > max_loop_num):
                LOG.info("up to max_loop_num, break rebalance, max_loop_num=[%d], real_idc=[%s]",
                        max_loop_num, ref_real_idc.id if ref_real_idc is not None else  "None")
                break
            #select a partition to move
            max_node = self.__get_max_load_slavenode(ref_real_idc, nodes_to_clear)
            #stop condition: try every node 
            if max_node == None:
                LOG.info("get no max node for rebalance, real_idc=[%s]",
                        ref_real_idc.id if ref_real_idc is not None else "None")
                break
            LOG.info("try to rebalance slavenode, max_node=[%s], state=[%s], real_idc=[%s]",
                max_node.id, max_node.online, max_node.real_idc)

            if restore_only:
                #move offline index partition only
                cand_partition = [self.__index_partition_sorting_tuple_del(max_node, iter_partition, 1)\
                        for iter_partition in max_node.index_partitions_of_offline()\
                        if not iter_partition.to_ignore and not iter_partition.is_lightweight]
            else:
                #try to move cand partition from max to min
                cand_partition = [self.__index_partition_sorting_tuple_del(max_node, iter_partition, 1)\
                        for iter_partition in max_node.index_partitions_of_new_and_running()\
                        if not iter_partition.to_ignore and not iter_partition.is_lightweight]

            max_node.balance_failure_tag = True
            if len(cand_partition) == 0:
                continue
            cand_partition.sort()

            if self.context.options["debug"]:
                print "rebalance begin"
                print max_node.id, max_node.online, max_node.cpu_idl(True)
                for item in cand_partition:
                    print item

            LOG.info("try to rebalance cpu of slavenode [%s] with [%d] candidate index_partition", max_node.id, len(cand_partition))
            part_index = len(cand_partition)-1

            while part_index >= 0:
                (ret, allocate_actions) = (False, None)
                ref_partition = cand_partition[part_index][-1]
                #will be deleted from slavenode later, check at first
                partition_meta = max_node.meta_of_index_partition(ref_partition.id)
                LOG.info("try to rebalance slavenode by index_partition, slavenode=[%s],index_partition=[%s], partition_run_state=[%s], partition_ns_state=[%s],dominate_resource=[%d]",
                        max_node.id, ref_partition.id, 
                        partition_meta['run_state'], partition_meta['state'],
                        ref_partition.dominate_resource)
                if max_node.online == 'online' and ref_partition.dominate_resource != cpu_index:
                    max_node.balance_failure_tag = True
                    if self.context.options["debug"]:
                        print "inner break: no cpu partition left"
                    LOG.info("max_node is online and no cpu-type index partitin left, break rebalance loop on the node, slavenode=[%s]", max_node.id)
                    break
                   
                #remove from max_node temporarily
                remove_action = IndexPlacementAction('DEL', ref_partition, max_node, None)
                if False == remove_action.execute(False):
                    oops('action %s execute failed', remove_action.to_string())

                #allocate_actions = self.allocate_index_partition(ref_partition, max_node.id, allocate_once=True)
                #if index_partition is offline, do not consider cpu diff between dest_node and orignal_node
                orig_offline = cand_partition[part_index][0]
                (ret, allocate_actions) = self.allocate_index_partition(ref_partition, max_node,
                    allocate_once=True, can_cross_real_idc=False,
                    fail_if_little_idle=fail_if_little_idle, orig_offline=orig_offline)
                if self.context.options["debug"]:
                    print "alloacte_index_partition returns"
                    print ret
                    print [iter.to_string() for iter in allocate_actions]
                if ret == True:
                    add_action = allocate_actions[-1]
                    if len(allocate_actions) > 1 or add_action.dest_node.id != max_node.id:
                        if self.context.options["debug"]:
                            print "inner break: allocate sucessfully"
                        max_node.balance_failure_tag = False
                        break
                    #else move back to max node, try next partition
                else: #if ret == False:
                    #add back
                    if False == remove_action.rollback():
                        oops('action %s rollback failed', remove_action.to_string())
                if self.context.options["debug"]:
                    print "inner loop: try next partition"
                part_index -= 1
                #do not need to add back to max_node
                #end of inner loop
            if self.context.options["debug"]:
                print "inner_loop break, max_node.balance_failure_tag", max_node.balance_failure_tag
 
            if max_node.balance_failure_tag:
                continue
            i = 0
            #the MOVE action(s) before last ADD, due to swithing in allocate_index_parition
            while i < len(allocate_actions)-1:
                if allocate_actions[i].action != 'MOVE':
                    oops("invalid type [%s] get from allocation process when move %s, MOVE expected", allocate_actions[i].to_string(), ref_partition.id);
                scheduled_action_list.append(allocate_actions[i])
                i += 1
            add_action = allocate_actions[-1]
            if add_action.action != 'ADD':
                oops("invalid type [%s] get from allocation process when move %s, ADD expected", allocate_actions[i].to_string(), ref_partition.id);
            if max_node.id != add_action.dest_node.id:
                action = IndexPlacementAction('MOVE', ref_partition, max_node, add_action.dest_node)
                max_node.add_zombie(ref_partition, consume_resource=False)
                #do not need to execute
                scheduled_action_list.append(action)
            elif len(allocate_actions) == 1:
                oops("error happens, move from-to the same slavenode")
            if self.context.options["debug"]:
                print "rebalance end"
            #end of while True

        return scheduled_action_list

    def rebalance_nodes_flash(self, ref_real_idc, restore_only):
        '''move index partitions between nodes to rebalance
        '''
        scheduled_action_list = []
        for iter_node in self.context.slavenodes.itervalues():
            iter_node.balance_failure_tag = False

        num_loop = 0
        if len(ref_real_idc.slavenodes) >= 3:
            max_loop_num = len(ref_real_idc.slavenodes) / 3
        else:
            max_loop_num = 1

        max_node = None
        while True:
            num_loop += 1
            if (num_loop > max_loop_num):
                LOG.info("up to max_loop_num, break rebalance, max_loop_num=[%d], real_idc=[%s]", max_loop_num, ref_real_idc.id)
                break
            #select a partition to move
            max_node = self.__get_max_load_slavenode_flash(ref_real_idc)
            #stop condition: try every node whose load larger than average in idc
            if max_node == None:
                LOG.info("get no max node for rebalance, real_idc=[%s]", ref_real_idc.id)
                break

            LOG.info("begin try to rebalance slavenode, max_node=[%s], state=[%s], real_idc=[%s], (%d, %d)", \
                        max_node.id, \
                        max_node.online, \
                        ref_real_idc.id, \
                        -max_node.index_partition_count_of_offline(), \
                        max_node.actual_resource_remain(flash_index))
            if restore_only:
                #move offline index partition only
                cand_partition = [self.__index_partition_sorting_tuple_del(max_node, iter_partition, 1)\
                        for iter_partition in max_node.index_partitions_of_offline()\
                        if not iter_partition.to_ignore and not iter_partition.is_lightweight]
            else:
                #try to move cand partition from max to min
                #consider parition whose dominate_resource == flash_index first; if the remain ssd is less than 0, all partitions are considered
                cand_partition = [self.__index_partition_sorting_tuple_del_flash(max_node, iter_partition, 1)\
                        for iter_partition in max_node.index_partitions_of_new_and_running()
                        if not iter_partition.to_ignore and not iter_partition.is_lightweight]

            max_node.balance_failure_tag = True
            if len(cand_partition) == 0:
                if self.context.options["debug"]:
                    print "no partition to move on %s" % max_node.id
                continue

            cand_partition.sort()

            if max_node.actual_resource_remain(flash_index) > 0 and cand_partition[-1][-1].dominate_resource == cpu_index:
                if self.context.options["debug"]:
                    print "no flash-bound partition on %s, and its actual flash is enough" % max_node.id
                continue

            if self.context.options["debug"]:
                print "rebalance flash begin"
                print max_node.id, max_node.online, max_node.resource_remain[flash_index], max_node.actual_resource_remain(flash_index)
                for item in cand_partition:
                    print item

            part_index = len(cand_partition)-1
            LOG.info("try to rebalance flash of slavenode [%s] with [%d] candidate index_partition", max_node.id, len(cand_partition))
            
            while part_index >= 0:
                (ret, allocate_actions) = (False, None)
                ref_partition = cand_partition[part_index][-1]
                partition_meta = max_node.meta_of_index_partition(ref_partition.id)
                LOG.info("try to rebalance slavenode by index_partition, slavenode=[%s], \
                        index_partition=[%s], partition_run_state=[%s], partition_ns_state=[%s], \
                        dominate_resource=[%d]", max_node.id, ref_partition.id, 
                        partition_meta['run_state'], partition_meta['state'], 
                        ref_partition.dominate_resource)
                #remove from max_node temporarily
                remove_action = IndexPlacementAction('DEL', ref_partition, max_node, None)
                if False == remove_action.execute(False):
                    oops('action %s execute failed', remove_action.to_string())

                is_error = cand_partition[part_index][0]
                #allocate_actions = self.allocate_index_partition(ref_partition, max_node.id, allocate_once=True)
                (ret, allocate_actions) = self.allocate_index_partition(ref_partition, max_node, 
                        allocate_once=True, can_cross_real_idc=False, orig_offline=is_error)

                if self.context.options["debug"]:
                    print "alloacte_index_partition returns"
                    print ret
                    print [iter.to_string() for iter in allocate_actions]
                if ret == True:
                    add_action = allocate_actions[-1]
                    if len(allocate_actions) > 1 or add_action.dest_node.id != max_node.id:
                        if self.context.options["debug"]:
                            print "inner break: allocate sucessfully"
                        max_node.balance_failure_tag = False
                        break
                    #else move back to max node, try next partition
                else: #if ret == False:
                    #add back
                    if False == remove_action.rollback():
                        oops('action %s rollback failed', remove_action.to_string())
                if self.context.options["debug"]:
                    print "inner break: try next partition"
                part_index -= 1
                #do not need to add back to max_node
                #end of inner loop
            if self.context.options["debug"]:
                print "inner_loop break, max_node.balance_failure_tag", max_node.balance_failure_tag
 
            if max_node.balance_failure_tag:
                continue
            i = 0
            #the MOVE action(s) before last ADD, due to swithing in allocate_index_partition
            while i < len(allocate_actions)-1:
                if allocate_actions[i].action != 'MOVE':
                    oops("invalid type [%s] get from allocation process when move %s, MOVE expected", allocate_actions[i].to_string(), ref_partition.id);
                scheduled_action_list.append(allocate_actions[i])
                i += 1
            add_action = allocate_actions[-1]
            if add_action.action != 'ADD':
                oops("invalid type [%s] get from allocation process when move %s, ADD expected", allocate_actions[i].to_string(), ref_partition.id);
            if max_node.id != add_action.dest_node.id:
                action = IndexPlacementAction('MOVE', ref_partition, max_node, add_action.dest_node)
                max_node.add_zombie(ref_partition, consume_resource=False)
                #do not need to execute
                scheduled_action_list.append(action)
            elif len(allocate_actions) == 1:
                oops("error happens, move from-to the same slavenode")
            if self.context.options["debug"]:
                print "rebalance end"
            #end of while True

        return scheduled_action_list

    def __enough_resource(self, demand, remain, times = 1):
#        return True
        for i, di in enumerate(demand):
            if (di*times) > remain[i]:
                return False
        return True

    def __real_idc_filter(self, ref_real_idc, ref_group):
        if len(self.context.options["index_conflict_to_avoid"]) != 0 and ref_group.id in self.context.options["index_conflict_to_avoid"]:
            if ref_real_idc.id == self.real_idc_to_avoid(ref_group.id):
                return True
        if not self.__enough_resource(ref_group.resource_demand, ref_real_idc.resource_remain, 1):
            return True
        elif ref_group.real_idc != None:
            if ref_group.real_idc == ref_real_idc.id:
                return True
            curr_real_idc = self.context.get_real_idc(ref_group.real_idc)
            cpu_diff_before = math.fabs(curr_real_idc.cpu_idl(True) - ref_real_idc.cpu_idl(True))
            cpu_diff_after = math.fabs(curr_real_idc.cpu_idl_without(ref_group, True) - ref_real_idc.cpu_idl_with(ref_group, True))
            if cpu_diff_before <= cpu_diff_after:
                return True

        return False

    def __real_idc_sorting_tuple(self, ref_real_idc, ref_group):
        average_resource_demand = []
        average_resource_remain = []
        for i, di in enumerate(ref_group.resource_demand):
            average_resource_demand.append(float(di)/len(ref_group.partitions))
            average_resource_remain.append(ref_real_idc.resource_remain[i]/len(ref_real_idc.slavenodes))
        avarage_enough = self.__enough_resource(average_resource_demand, average_resource_remain, 1)
 
        if ref_group.dominate_resource == cpu_index:
            return (avarage_enough, ref_real_idc.cpu_idl_with(ref_group, True), ref_real_idc.resource_remain[flash_index], len(ref_real_idc.slavenodes), ref_real_idc.id, ref_real_idc)
        elif ref_group.dominate_resource == mem_index: 
            return (avarage_enough, average_resource_remain[mem_index], ref_real_idc.resource_remain[mem_index], ref_real_idc.resource_remain[flash_index], ref_real_idc.cpu_idl_with(ref_group, True), len(ref_real_idc.slavenodes), ref_real_idc.id, ref_real_idc)
        elif ref_group.dominate_resource == flash_index: 
            return (avarage_enough, average_resource_remain[flash_index], ref_real_idc.resource_remain[flash_index], ref_real_idc.resource_remain[mem_index], ref_real_idc.cpu_idl_with(ref_group, True), len(ref_real_idc.slavenodes), ref_real_idc.id, ref_real_idc)
 
        else:
            oops("dominate_resource %d is not defined", ref_group.dominate_resource)


    def __index_group_filter(self, ref_real_idc, ref_group):
        reserved_real_idc = None
        if ref_group.id in self.context.options["group_guide"]:
            reserved_real_idc = self.context.options["group_guide"][ref_group.id]
            if reserved_real_idc == "stay":
                return True
            if reserved_real_idc == ref_real_idc.id:
                return True
        return False

    #define a sorting tuple of index_groups to select to move
    def __index_group_sorting_tuple_del(self, ref_real_idc, ref_group):
        global cpu_index
        maybe_conflict = False
        if self.context.options["index_conflict_to_avoid"] != None and ref_group.id in self.context.options["index_conflict_to_avoid"]:
            if self.context.get_real_idc(self.context.options["index_conflict_to_avoid"][ref_group.id]) != None:
                maybe_conflict = True
        #print "============", ref_group.id, ref_group.dominate_resource, cpu_index
        #print "+++++", not ref_group.dominate_resource == cpu_index
        #print "+++++", not int(ref_group.dominate_resource) == int(cpu_index)
        return (
                ref_group.dominate_resource == cpu_index,     #prefer cpu-bound index-group
                not maybe_conflict,                                   #True > False
                round(ref_group.resource_demand[cpu_index]/(ref_group.resource_demand[flash_index]+1), 2), #ratio of cpu/ssd
                round(ref_group.resource_demand[cpu_index]/(ref_group.resource_demand[mem_index]+1), 2), #ratio of cpu/meme
                -ref_group.resource_demand[cpu_index],                #cpu demand
                -ref_group.resource_demand[mem_index],          #size
                -ref_group.resource_demand[flash_index],
                ref_group.id,
                ref_group)               
       
    
    def __slavenode_filter(self, ref_node, ref_partition, count_failure_domain=False):
        #LOG.info("slavenode filter, slavenode_id=[%s]", ref_node.id)
        ref_domain = ref_node.get_failure_domain()
        #online
        if ref_node.online=='offline':
            if self.context.options["debug"]:
                LOG.info("slavenode filtered out by state [offline], slavenode_id=[%s]", ref_node.id)
            return 1
        if len(self.context.options["index_conflict_to_avoid"]) != 0 and ref_node.real_idc in self.context.options["index_conflict_to_avoid"]['index_partition'][ref_partition.id]['real_idc']:
            if self.context.options["debug"]:
                LOG.info("slavenode filtered out by conf index_conflict_to_avoid, slavenode_id=[%s]", ref_node.id)
            return 2
        #no matter if NEW, RUNNING, TO_BE_DELETE
        #if len(ref_node.index_partitions_by_id_of_new_and_running(ref_partition.id)) > 0:
        if len(ref_node.index_partitions_by_id(ref_partition.id)) > 0:
            if self.context.options["debug"]:
                LOG.info("slavenode filtered out by same index_partition on salvenode, slavenode_id=[%s]", ref_node.id)
            return 3
        #failure domain constraint is not implemented here for cpu-bound and mem-bound service
        if count_failure_domain and ref_domain.index_partition_count_by_id(ref_partition.id) > 0:
            if self.context.options["debug"]:
                LOG.info("slavenode filtered out by same index_partition in failure_domain, slavenode_id=[%s]", ref_node.id)
            return 4
        #enough_resource
        if not self.__enough_resource(ref_partition.resource_demand, ref_node.resource_remain):
            if self.context.options["debug"]:
                LOG.info("slavenode filtered out by not enough resource, demand=%s, remain=%s, slavenode_id=[%s]", ref_partition.resource_demand, ref_node.resource_remain, ref_node.id)
            return 5
        #break up continous index partitions
        if self.context.options['debug']:
            if self.context.options["debug"]:
                LOG.info("index_type[%s], continue_index_type[%s]", ref_partition.type, self.context.options['continue_index_type'])

        if ref_partition.type in self.context.options['continue_index_type']:
            for iter_part in ref_node.index_partitions_by_type_of_new_and_running(ref_partition.type):
                if abs(ref_partition.partition_num - iter_part.partition_num) < self.break_up_dist:
                    if self.context.options["debug"]:
                        LOG.info("slavenode filtered out by break up distance, slavenode_id=[%s]", ref_node.id)
                    return 6
        #exclude_error_machine = true by default
        if "exclude_error_machine" not in self.context.options or self.context.options["exclude_error_machine"] == True:
            if ref_node.online == 'error':
                if self.context.options["debug"]:
                    LOG.info("slavenode filtered out by state [error], slavenode_id=[%s]", ref_node.id)
                return 7
        if not ref_node.agent_available:
            if self.context.options["debug"]:
                LOG.info("slavenode filtered out by agent-unavaiable, slavenode_id=[%s]", ref_node.id)
            return 8

        return False

    def __slavenode_sorting_tuple_add(self, ref_node, ref_partition):
        ref_domain = ref_node.get_failure_domain()
        if ref_partition.dominate_resource == cpu_index:
            return (ref_node.cpu_idl_with(ref_partition, True),               #cpu_idl
                ref_node.cpu_idl_layer_with(ref_partition, True),         #cpu_idl considering only the same layer
                ref_domain.cpu_idl_with(ref_partition, True),                   #cpu idl of domain
                -len(ref_node.index_partitions_by_type_of_new_and_running(ref_partition.type)), #num of same index-type
                ref_node.resource_remain[mem_index],                #free memory
                ref_node.resource_remain[flash_index],              #free flash
                ref_domain.index_partition_count_by_id(ref_partition.id) == 0, #to keep the failure domain constraint, 'not for True > Flase'
                ref_node.id,                                            #id, make result stable.
                ref_node)                                               #object to return.
        elif ref_partition.dominate_resource == mem_index:
            return (ref_node.resource_remain[mem_index],                #free memory
                ref_node.resource_remain[flash_index],              #free flash
                ref_node.cpu_idl_with(ref_partition, True),               #cpu_idl
                ref_node.cpu_idl_layer_with(ref_partition, True),         #cpu_idl considering only the same layer
                ref_domain.cpu_idl_with(ref_partition, True),                   #cpu idl of domain
                -len(ref_node.index_partitions_by_type_of_new_and_running(ref_partition.type)), #num of same index-type
                ref_domain.index_partition_count_by_id(ref_partition.id) == 0, #to keep the failure domain constraint
                ref_node.id,                                            #id, make result stable.
                ref_node)                                               #object to return.
        elif ref_partition.dominate_resource == flash_index:
            return (ref_node.resource_remain[flash_index],              #free flash
                    -ref_node.resource_remain[cpu_index]/(ref_node.resource_remain[flash_index]+1), #cpu/flash, the less the better
                    ref_node.resource_remain[mem_index],                #free memory
                    ref_node.cpu_idl_with(ref_partition, True),         #cpu_idl
                    ref_node.id,                                            #id, make result stable.
                    ref_node)                                               #object to return.
        else:
            oops("dominate_resource %d is not defined", ref_partition.dominate_resource)




    def __slavenode_sorting_tuple_del(self, ref_node, ref_partition):
        """for deleting, add node_filter result in front of sorting tuple for adding
        """
        return (self.__slavenode_filter(ref_node, ref_partition),   #True > False
                -ref_node.cpu_idl_with(ref_partition, True),               #cpu_idl
                -ref_node.cpu_idl_layer_with(ref_partition, True),         #cpu_idl considering only the same layer
                len(ref_node.index_partitions_by_type_of_new_and_running(ref_partition.type)), #num of same index-type
                #hash((node_usage, node.id))%20011,                  #different replica with different order, to get rid of (a1b1, a2b2)
                -ref_node.resource_remain[flash_index],              #free flash
                -ref_node.resource_remain[mem_index],                #free memory
                ref_node.id,                                            #id, make result stable.
                ref_node)                                               #object to return.

    #define a sorting tuple of index_partitions to select to move
    def __index_partition_sorting_tuple_del(self, ref_node, ref_partition, maxmin):
        ref_idc = ref_node.get_real_idc()
        scale_node = ref_node.cpu_scale_layer(ref_partition.layer)
        scale_idc = ref_idc.cpu_scale_layer(ref_partition.layer)
        return (ref_node.is_offline_by_index_partition_id(ref_partition.id),
                -ref_partition.dominate_resource,
                scale_node > scale_idc,
                self.__slavenode_filter(ref_node, ref_partition),         #violation True > False
                maxmin*ref_partition.resource_demand[cpu_index],                #cpu demand
                -ref_partition.resource_demand[flash_index],              #size
                ref_partition.id,
                ref_partition)               
    def __index_partition_sorting_tuple_del_flash(self, ref_node, ref_partition, maxmin):
        ref_idc = ref_node.get_real_idc()
        return (ref_node.is_offline_by_index_partition_id(ref_partition.id),
                self.__slavenode_filter(ref_node, ref_partition),         #violation True > False
                ref_partition.dominate_resource != cpu_index,            #cpu type bs got a low priority, True > False
                ref_partition.resource_demand[flash_index],              #size
                ref_partition.id,
                ref_partition)               
        
    def __get_max_load_real_idc(self):
        heap = [(iter_real_idc.cpu_idl(True), -len(iter_real_idc.slavenodes), iter_real_idc.id, iter_real_idc) for iter_real_idc in self.context.real_idcs.itervalues() if not iter_real_idc.balance_failure_tag]
        heap.sort()
        if self.context.options["debug"]:
            print "select max load real idc:"
            for item in heap:
                print item
        if len(heap) == 0:
            return None
        return heap[0][-1]

    def __get_max_load_slavenode(self, ref_real_idc, node_to_clear=None):
        if node_to_clear != None and len(node_to_clear) > 0:
            LOG.info("determine max load slavenode in node_to_clear, real_idc=[%s]",
                ref_real_idc.id if ref_real_idc is not None else "None")
            heap = [(iter_node.cpu_idl(True), iter_node.id, iter_node)
                for iter_node in node_to_clear
                    if (ref_real_idc is None or iter_node.real_idc == ref_real_idc.id)
                        and (not iter_node.balance_failure_tag) and
                        len(iter_node.index_partitions_of_new_and_running()) > 0]
        else:
            LOG.info("determine max load slavenode in real_idc, real_idc=[%s]",
                ref_real_idc.id if ref_real_idc is not None else "None")
            if ref_real_idc is not None:
                for iter_node in ref_real_idc.slavenodes.itervalues(): 
                    if iter_node.balance_failure_tag:
                        LOG.debug("filtered out by balance_failure_tag, slavenode=[%s], "
                            "real_idc=[%s]", iter_node.id, ref_real_idc.id)
                heap = [(iter_node.cpu_idl(True), iter_node.id, iter_node)
                    for iter_node in ref_real_idc.slavenodes.itervalues()
                        if ((not iter_node.balance_failure_tag) and
                            iter_node.online != 'offline')]
            else:
                heap = [(iter_node.cpu_idl(True), iter_node.id, iter_node)
                    for iter_idc in self.context.real_idcs.itervalues()
                        for iter_node in iter_idc.slavenodes.itervalues()
                            if ((not iter_node.balance_failure_tag) and
                                iter_node.online != 'offline')]

        if len(heap) == 0:
            return None
        max_vec = min(heap)
        return max_vec[-1]

    def __get_max_load_slavenode_flash(self, ref_real_idc):
        heap = [(-iter_node.index_partition_count_of_offline(), iter_node.actual_resource_remain(flash_index), -len(iter_node.index_partitions_of_new_and_running()), iter_node.id, iter_node) for iter_node in ref_real_idc.slavenodes.itervalues() if ((not iter_node.balance_failure_tag) and len(iter_node.index_partitions_of_new_and_running()) > 0 and iter_node.resource_total[flash_index] > 0)]
        if len(heap) == 0:
            return None
        max_vec = min(heap)
        return max_vec[-1]

    def __get_average_partition_count(self):
        node_count = float(len(self.context.slavenodes))
        if node_count == 0:
            return 
        partition_count = 0
        for partition in self.context.index_partitions.values():
            partition_count += len(partition.nodes)
        average_partition_count = partition_count / node_count

        return average_partition_count

    def __get_average_partition_count_by_type(self):
        node_count = float(len(self.context.slavenodes))
        if node_count == 0:
            return 
        partition_count_by_type = {}
        for partition in self.context.index_partitions.values():
            if partition.type not in partition_count_by_type:
                partition_count_by_type[partition.type] = 0
            partition_count_by_type[partition.type] += 1

        average_partition_count_by_type = {}
        for partition_type in partition_count_by_type.keys():
            average_partition_count_by_type[partition_type] = partition_count_by_type[partition_type] / node_count

        return average_partition_count_by_type

class IndexPlacementManager:
    def __init__(self, options):
        self.options = options

        #parameters
        self.index_partitions = {}
        self.index_groups = {}
        self.slavenodes = {}
        self.failure_domains = {}
        self.real_idcs = {}
        self.zombie_app = {}

        self.partition_to_node_map = {}
        self.node_to_partition_map = {}

        self.replicate_policy = DRFReplicatePolicy(context=self)

        self.action_list = []
        self.output_index_distr_file = None

        #cpu, memory, flash
        self.resource_total = [0,0,0]
        self.re_store = re.compile(r"^(?P<value>\d+(.\d+)?)\s*(?P<unit>M|MB|G|GB)?$", re.IGNORECASE)
        self.output_format = 'bash'

        #allocation/restore/rebalance
        #rebalance by default
        self.ALLOCATION = 'allocation'
        self.RESTORE = 'restore'
        self.REBALANCE = 'rebalance'
        self.allocation_mode = self.REBALANCE

        #None means not specifed
        self.index_partition_to_fix = None
        self.slavenode_to_fix = None
        LOG.info("IndexPlacmentMnager created")

    def normalize_mac_cpu(self):
        """
        decay poor ability cpu type
        """
        tmp_list = [node.resource_total[cpu_index] for node in self.slavenodes.values() if node.resource_total[cpu_index] > 0]
        tmp_list.sort()
        if len(tmp_list) <= 0:
            return -1
        norm_cpu = tmp_list[int(len(tmp_list) * 0.8)]
        LOG.info("normalized cpu value is [%d]", norm_cpu)
        for key, value in self.slavenodes.iteritems():
            if value.resource_total[cpu_index] <= 0 or \
                    value.resource_total[cpu_index] >= norm_cpu:
                continue
            ori_value = value.resource_total[cpu_index]
            value.resource_total = (
                    int(ori_value * ((1.0 * ori_value / norm_cpu) ** (1.0 / 3))),
                    value.resource_total[1], value.resource_total[2]
            )
            value.resource_remain[cpu_index] = value.resource_total[cpu_index]
            LOG.info("normalized node [%s] cpu [%d] to [%d], resource=[%s]",
                key, ori_value, value.resource_total[cpu_index], str(value.resource_total))
        return 0

    def create_zombie_app_from_bh_spec(self, id, container_group_spec, replic):
        """create zb app by app spec
        Args:
            id:app_id
            cntainer_group_spec:app_spec
            replic:replicas
        Returns:
            created app
        """
        if id in self.zombie_app:
            LOG.warning('zombie app [%s] already exists', id)
            return None
        size = 0
        if "dataSpace" in container_group_spec["resource"]["disks"]:
            size = container_group_spec["resource"]["disks"]["dataSpace"]["sizeMB"]
        elif "dataDisks" in container_group_spec["resource"]["disks"]:
            for data_disk in container_group_spec["resource"]["disks"]["dataDisks"]:
                if "ssd" == data_disk["type"]:
                    size += int(data_disk["sizeMB"])
        cpu = int(container_group_spec["resource"]["cpu"]["numCores"])
        mem = int(container_group_spec["resource"]["memory"]["sizeMB"])
        group_id = 'zombie'

        #if group_id not in self.index_groups:
        #    new_group = IndexGroup(group_id, self)
        #    self.index_groups[group_id] = new_group
        new_zombie = IndexPartition(id, type='zombie', layer='zombie', 
                cpu=cpu, mem=mem, size=size, 
                replication_factor=replic,
                group='zombie',
                context=self)
        self.zombie_app[id] = new_zombie
        LOG.info('zombie app [%s] created, (%f, %f, %f, %d)', id, cpu, mem, size, replic)

        return new_zombie

    def create_index_partition_from_bh_spec(self, id, container_group_spec, \
            replic, group_id, index_layer):
        """create app from beehive spec info
        Args:
            id: app_id
            container_group_spec: app_spec
            replic: replicas
            group_id: attrbutes/group_id
            index_layer: layer
        Returns:
            created app
        """
        if id in self.index_partitions:
            LOG.warning('index partition [%s] already exists', id)
            return None

        size = 0
        index_type = id[:id.rfind("_")]
        if "dataSpace" in container_group_spec["resource"]["disks"]:
            size = container_group_spec["resource"]["disks"]["dataSpace"]["sizeMB"]
        elif "dataDisks" in container_group_spec["resource"]["disks"]:
            for data_disk in container_group_spec["resource"]["disks"]["dataDisks"]:
                if self.options["media"] == data_disk["type"]:
                    size += int(data_disk["sizeMB"])
        #if size == 0:
        #    LOG.warning('media option is invalid, %s', self.options["media"])
        #    return None

        #size in resouce_spec has already consider 2 times of its actual size

        cpu = int(container_group_spec["resource"]["cpu"]["numCores"]) \
                if "resource" in container_group_spec and \
                "cpu" in container_group_spec["resource"] and \
                "numCores" in container_group_spec["resource"]["cpu"] \
                else 0
        mem = int(container_group_spec["resource"]["memory"]["sizeMB"]) \
                if "resource" in container_group_spec and \
                "memory" in container_group_spec["resource"] and \
                "sizeMB" in container_group_spec["resource"]["memory"] \
                else 0

        if group_id not in self.index_groups:
            new_group = IndexGroup(group_id, self)
            self.index_groups[group_id] = new_group
        new_partition = IndexPartition(id, type=index_type, layer=index_layer, 
                cpu=cpu, mem=mem, size=size, 
                replication_factor=replic,
                group=group_id,
                context=self)
        if 'bs_all' in self.options['index_cross_idc'] or \
            index_type in self.options['index_cross_idc']:
            LOG.warning("index partiion %s type %s has no idc limitation.",
                id, index_type)
            new_partition.can_cross_real_idc = True
        if cpu <= 10:
            new_partition.dominate_resource = flash_index
            if size <= 50:
                new_partition.is_lightweight = True
        else:
            new_partition.dominate_resource = cpu_index
        self.index_partitions[id] = new_partition
        ref_group = self.index_groups[group_id]
        ref_group.add_index_partition(new_partition)

        LOG.info('index partition [%s] created, (%f, %f, %f, %d)', id, cpu, mem, size, replic)

        return new_partition


    def create_index_partition(self, id, meta):
        if id in self.index_partitions:
            LOG.warning('index partition [%s] already exists', id)
            return None

        key = 'size'
        size = self.__mb_to_gb(meta, key)
        if size == None:
            LOG.warning('[%s] of partition [%s] is not specified, setting to 0', key, id)
            size = 0
        elif size == -1:
            LOG.warning('bad format of [%s] of partition [%s], setting to 0', key, id) 
            size = 0
        #bs service need occupy 2 times of its actual size
        size = size * 2

        key = 'normalized_cpu'
        cpu = 0
        if key in meta:
            cpu = int(meta[key])
        else:
            LOG.warning('[%s] of partition [%s] is not specified, setting to 0', key, id)

        key = 'memory'
        mem = self.__mb_to_gb(meta, key)
        if mem == None:
            LOG.warning('[%s] of partition [%s] is not specified, setting to 0', key, id)
            mem = 0
        elif mem == -1:
            LOG.warning('bad format of [%s] of partition [%s], setting to 0', key, id) 
            mem = 0

        group_id = meta['group']
        if group_id not in self.index_groups:
            new_group = IndexGroup(group_id, self)
            self.index_groups[group_id] = new_group
        if type(meta['replication_factor']) is dict:
            replic=1
        else:
            replic=int(meta['replication_factor']) 
        new_partition = IndexPartition(id, type=meta['index_type'], layer=meta['index_layer'], 
                cpu=cpu, mem=mem, size=size, 
                replication_factor=replic,
                group=group_id,
                context=self)
        if not None==meta.get("min_usable"):
            new_partition.set_min_usable(meta["min_usable"])
        if cpu <= 10:
            if self.options["debug"]:
                print "set dr of", id, "of", group_id, mem_index, "cpu", cpu
            new_partition.dominate_resource = flash_index
            if size <= 50:
                new_partition.is_lightweight = True
        else:
            if self.options["debug"]:
                print "set dr of", id, "of", group_id, cpu_index, "cpu", cpu
            new_partition.dominate_resource = cpu_index
        self.index_partitions[id] = new_partition
        ref_group = self.index_groups[group_id]
        ref_group.add_index_partition(new_partition)

        LOG.info('index partition [%s] created', id)

        return new_partition

    def add_slavenode_slots(self, app_id, app_info):
        """
        add slave node slots
        """
        for container in app_info.containers:
            node_id = container["hostId"]
            work_path = container["allocatedResource"]["disks"]["workspace"]["mountPoint"]["target"]
            service_port = container["allocatedResource"]["port"]["range"]["first"]
            container_instance_id = container["id"]["groupId"]["groupName"] + \
                    "_" + str(container["id"]["name"]).zfill(10)
            
            ref_node = self.get_slavenode(node_id)
            if ref_node is None:
                LOG.warning('slavenode is not existed, hostname [%s], contain_instance_id [%s]',
                            node_id, container_instance_id)
                continue        
            path_str = work_path
            try:
                slot_id = int(path_str[path_str.rfind("_")+1:])
            except:
                LOG.info("work_path is not bs type, skipped, work_path=[%s], id=[%s]",
                    path_str, app_id)
                return 0
            ref_node.occupied_slots[slot_id] = None
            if ref_node.max_slot_id < slot_id:
                ref_node.max_slot_id = slot_id

    def create_slavenode_from_bh_spec(self, spec, resource_remain=None):
        id = spec["id"]
        failure_domain = id[:id.rfind(".")]
        if id in self.slavenodes:
            LOG.warning('search node [%s] already exists', id)
            return None
        if "idc" not in spec or "state" not in spec:
            LOG.warning('search node [%s] has no idc or state', id)
            spec["idc"] = "null"
            spec["state"] = 3

        real_idc = spec["idc"]
        if "NET_CLUSTER" not in spec["hostDetails"]:
            LOG.warning('search node [%s] has no NET_CLUSTER', id)
#            return None
        #else:
        #    real_idc = real_idc + "_" + spec["hostDetails"]["NET_CLUSTER"]

        disk_paths = []
        workspace_type = "home"
        for disk in spec["totalResource"]["disks"]["disks"]:
            if (disk["type"] == workspace_type and disk["available"] == True):
                disk_paths.append(disk["mountPoint"])

        workspace_candidates = ["/home", "/home/", "/home/work", "/home/work/"]
        workspace_usable = list(set(workspace_candidates) & set(disk_paths))
        workspace_path_selected = "/home"
        if len(workspace_usable) > 0:
            workspace_usable.sort()
            workspace_usable.reverse()
            workspace_path_selected = workspace_usable[0]
        if spec["state"] == 2:
            spec["state"] = "online"
        else:
            spec["state"] = "offline"
        if spec["freezed"] != False:
            spec["state"] = "offline"

        if spec['state'] == 'online' and len(workspace_usable) <= 0:
            LOG.warning('node [%s] lack of workspace_disk', id)
            return None

        all_sto = []
        all_sto_str = []
        if self.options["media"] == "ssd":
            cap = 0
            for disk in spec["totalResource"]["disks"]["disks"]:
                if disk["type"] == "ssd" and disk["available"] == True:
                    cap += disk["sizeMB"]
        elif self.options["media"] == "disk":
            cap = 0
            for disk in spec["totalResource"]["disks"]["disks"]:
                if disk["type"] == "disk" and disk["available"] == True:
                    path = disk["mountPoint"]
                    if path == "/" or path == "/home" or path == "/home/":
                        continue
                    cap += disk["sizeMB"]
        else:
            LOG.warning('media option is invalid, %s', self.options["media"])
            return None

        #since the storage is not maintained in place_on func, use resource_remain
        if self.options["media"] == "ssd":
            for disk in spec["freeResource"]["disks"]["disks"]:
                if disk["type"] == "ssd" and disk["available"] == True \
                            and disk["sizeMB"] != 0:
                    sto = Storage(disk["mountPoint"], disk["sizeMB"])
                    all_sto.append(sto)
                    all_sto_str.append(sto.to_string())
        elif self.options["media"] == "disk":
            for disk in spec["freeResource"]["disks"]["disks"]:
                if disk["type"] == "disk" and disk["available"] == True \
                            and disk["sizeMB"] != 0:
                    path = disk["mountPoint"]
                    if path == "/" or path == "/home" or path == "/home/":
                        continue
                    sto = Storage(path, disk["sizeMB"])
                    all_sto.append(sto)
                    all_sto_str.append(sto.to_string())
        else:
            LOG.warning('media option is invalid, %s', self.options["media"])
            return None


        cpu = spec["totalResource"]["cpu"]["numCores"]
        mem = spec["totalResource"]["memory"]["sizeMB"]
        agent_ava = True
        if "agent_available" in spec:
            agent_ava = spec["agent_available"]
        reserve_mem_percent = self.options["reserve_mem_percent"] \
            if "reserve_mem_percent" in self.options else 0
        new_node = SlaveNode(id, 
                cpu=cpu, mem=mem, capacity=cap, storage_list = all_sto,
                failure_domain=failure_domain, 
                online=spec['state'], 
                real_idc=real_idc,
                idc=spec["idc"], 
                workspace_path=workspace_path_selected, 
                workspace_type=workspace_type, 
                context=self, agent_available = agent_ava,
                reserve_mem_percent = reserve_mem_percent)
        
        self.slavenodes[id] = new_node
        LOG.info('search node [%s] created, (%f, %f, %f), storage=[%s]', id, cpu, mem, cap, ','.join(all_sto_str))
        
        for i, bi in enumerate(new_node.resource_total): self.resource_total[i] += bi
 
        # classify new node to its failure domain
        self.__classify_to_failure_domains(new_node)
        self.__classify_to_real_idcs(new_node)

        return new_node

       

    def create_slavenode(self, id, meta, cpu_rate):
        if id in self.slavenodes:
            LOG.warning('search node [%s] already exists', id)
            return None

        flash = self.__mb_to_gb(meta, 'flash_capacity')
        if flash == None:
            LOG.warning('flash_capacity of slavenode [%s] is not specified, setting to 0', id)
            flash = 0
        elif flash == -1:
            LOG.warning('bad format of flash_capacity of slavenode [%s], setting to 0', id) 
            flash = 0

        cpu_key = 'cpu_capacity'
        if  cpu_key in meta:
            cpu = int(meta[cpu_key])
            cpu = cpu * (cpu_rate/100.0)
        else:
            LOG.warning('cpu_capacity of slavenode [%s] is not specified, setting to 0', id)
            cpu = 0
        
        mem = self.__mb_to_gb(meta, 'memory_capacity')
        if mem == None:
            LOG.warning('memory_capacity of slavenode [%s] is not specified, setting to 0', id)
            mem = 0
        elif mem == -1:
            LOG.warning('bad format of memory_capacity of slavenode [%s], setting to 0', id) 
            mem = 0
        #TODO:buffer for BC, should not be hard coded
        mem = mem - 1

        new_node = SlaveNode(id, 
                cpu=cpu, mem=mem, capacity=flash,
                failure_domain=meta['failure_domain'], 
                online=meta['online_state'], 
                real_idc=meta["real_idc"],
                idc=meta["idc"], 
                workspace_path='/home',
                workspace_type='home',
                context=self)
        self.slavenodes[id] = new_node
        LOG.info('search node [%s] created', id)
        
        for i, bi in enumerate(new_node.resource_total): self.resource_total[i] += bi
 
        # classify new node to its failure domain
        self.__classify_to_failure_domains(new_node)
        self.__classify_to_real_idcs(new_node)

        return new_node

    def add_zombie(self, node_id, zombie_list):
        ref_node = self.get_slavenode(node_id)
        if ref_node == None:
            LOG.warning('slavenode %s is not existed, but list in zombie', node_id)
        for part_id in zombie_list:
            ref_part = self.get_index_partition(part_id)
            if ref_part == None:
                LOG.warning('index partition %s is not existed, but list in zombie', part_id)
            ref_node.add_zombie(ref_part)


    def unbind_index_partition_from_slavenode(self, index_partition_id, slavenode_id, with_zombie=True):
        index_partition = self.get_index_partition(index_partition_id)
        slavenode = self.get_slavenode(slavenode_id)

        action = IndexPlacementAction('DEL', index_partition, slavenode, None)

        action.execute(with_zombie)
        self.action_list.append(action)

    def unbind_index_group_and_partitions(self, index_group_id, with_zombie=True):
        ref_group = self.get_index_group(index_group_id)
        if ref_group == None:
            oops('try to unbind a group not existed %s', index_group_id)

        all_parts = ref_group.get_all_index_partitions()
        for ref_part in all_parts:
            all_nodes = ref_part.nodes.keys()
            for node_id in all_nodes:
                self.unbind_index_partition_from_slavenode(ref_part.id, node_id, with_zombie)

        ref_real_idc = ref_group.get_real_idc()
        if ref_real_idc != None:
            ref_real_idc.displace_index_group(ref_group, with_zombie)

    def unbind_bad_index_group_and_partitions(self, index_group_id):
        ref_group = self.get_index_group(index_group_id)
        if ref_group == None:
            oops('try to unbind a group not existed %s', index_group_id)

        reserved_real_idc = None
        if index_group_id in self.options["group_guide"]:
            reserved_real_idc = self.options["group_guide"][index_group_id]

        all_parts = ref_group.get_all_index_partitions()
        for ref_part in all_parts:
            all_nodes = ref_part.slavenodes()
            for ref_node in all_nodes:
                if reserved_real_idc != None and ref_node.real_idc == reserved_real_idc:
                    continue
                self.unbind_index_partition_from_slavenode(ref_part.id, ref_node.id)
                #update resource and binding info from real idc for bad group
                ref_real_idc = ref_node.get_real_idc()
                for i, rei in enumerate(ref_part.resource_demand): 
                    if (i == flash_index): #flash resource will not be free after delete partition
                        continue
                    elif (i == mem_index):
                        continue
                    elif (i == cpu_index):
                        ref_real_idc.resource_remain[i] += rei
                    else:
                        pass
                if ref_group.id in ref_real_idc.index_groups:
                    del ref_real_idc.index_groups[ref_group.id]
        ref_group.real_idc = reserved_real_idc

    def place_zombie_instance(self, ref_zombie, container):
        node_id = container["hostId"]
        ref_node = self.get_slavenode(node_id)
        if ref_node == None:
            LOG.info("slavenode %s is not existed, skipped", node_id)
            return
        ref_node.add_zombie_from_bh_container(ref_zombie, container, False)

    def place_index_partition_from_bh_container(self, ref_partition, container, state):
        node_id = container["hostId"]
        ref_node = self.get_slavenode(node_id)
        if ref_node == None:
            LOG.warning('slavenode is not existed, hostname [%s], partition [%s]', node_id, ref_partition.id)
            return None
        
        if not 'visible' in state:
            state["visible"] = True
            logging.warning("instance:%s@%s visible not set, set to default:True" % \
                (ref_partition.id, node_id))

        meta = {
                "state":state["ns_state"],
                "visible":state["visible"],
                "run_state":state["run_state"],
                "service_port":container["allocatedResource"]["port"]["range"]["first"], 
                "work_path":container["allocatedResource"]["disks"]\
                        ["workspace"]["mountPoint"]["target"]}
        ref_node.place_index_partition(ref_partition.id, meta)
        #mark node.online to error if there is any offline container on it
        if (state["run_state"] not in ["RUNNING", "NEW", "REPAIR", "DEPLOYFAIL"]) \
                and ref_node.online == 'online':
            ref_node.mark_error()
        ref_partition.place_at_node(node_id, meta)

        ref_group = self.get_index_group(ref_partition.group)
        if ref_partition.to_ignore:
            self.options["group_guide"][ref_group.id] = 'stay'
        ref_real_idc = ref_node.get_real_idc()
        if ref_group.real_idc == None:
            ref_real_idc.place_index_group(ref_group)
        #for stable partition, also stay its corresponding group
        elif ref_group.real_idc != ref_real_idc.id and not ref_partition.to_ignore:
            return ref_group.id
        return None

           
    def place_index_partition(self, ref_partition, ref_node, placement_instance):
        if placement_instance["state"] == "TO_BE_DELETED":
            ref_node.add_zombie(ref_partition, placement_instance)
            return None

        ref_node.place_index_partition(ref_partition.id, placement_instance)
        ref_partition.place_at_node(ref_node.id, placement_instance)

        ref_group = self.get_index_group(ref_partition.group)
        if ref_partition.to_ignore:
            self.options["group_guide"][ref_group.id] = 'stay'
        ref_real_idc = ref_node.get_real_idc()
        if ref_group.real_idc == None:
            ref_real_idc.place_index_group(ref_group)
        #for stable partition, also stay its corresponding group
        elif ref_group.real_idc != ref_real_idc.id and not ref_partition.to_ignore:
            return ref_group.id
        return None

    def get_index_partition(self, id):
        if id in self.index_partitions:
            return self.index_partitions[id]
        else:
            return None
    
    def get_index_group(self, id):
        if id in self.index_groups:
            return self.index_groups[id]
        else:
            return None

    def get_slavenode(self, id):
        if id in self.slavenodes:
            return self.slavenodes[id]
        else:
            return None

    def failure_domain(self, id):
        if id in self.failure_domains:
            return self.failure_domains[id]
        else:
            return None

    def get_real_idc(self, id):
        if id in self.real_idcs:
            return self.real_idcs[id]
        else:
            return None


    def __classify_to_failure_domains(self, node):
        domain_id = node.failure_domain
        if domain_id not in self.failure_domains:
            self.failure_domains[domain_id] = FailureDomain(domain_id, context=self)
        self.failure_domains[domain_id].add_slavenode(node.id)

    def __classify_to_real_idcs(self, node):
        real_idc_id = node.real_idc
        if real_idc_id not in self.real_idcs:
            self.real_idcs[real_idc_id] = RealIDC(real_idc_id, context=self)
        self.real_idcs[real_idc_id].add_slavenode(node)

    def allocate_index_groups_maxmax(self):
        group_heap = []
        for ref_group in self.index_groups.itervalues():
            #if real idc of group is different from which in g_group_guide, move it
            if ref_group.id in self.options["group_guide"]:
                reserved_real_idc = self.options["group_guide"][ref_group.id]
                if reserved_real_idc != "stay" and reserved_real_idc != "" and ref_group.real_idc != reserved_real_idc: 
                    LOG.warning('move index_group %s from real_idc %s to %s, due to config file', ref_group.id, ref_group.real_idc, reserved_real_idc)
                    #reserved_real_idc and current_real_idc are considered in called func
                    self.unbind_bad_index_group_and_partitions(ref_group.id)
            #if real idc of group is the same with which in g_index_conflict, move it
            conflict_real_idc = self.replicate_policy.real_idc_to_avoid(ref_group.id)
            if conflict_real_idc != None and ref_group.real_idc == conflict_real_idc:
                if self.allocation_mode == self.ALLOCATION:
                    LOG.warning('index_group %s has node conflict with the other idc in real_idc %s, but will not move in allocation mode', ref_group.id, ref_group.real_idc)
                else:
                    LOG.warning('move index_group %s from real_idc %s due to confliction config file', ref_group.id, ref_group.real_idc)
                    self.unbind_bad_index_group_and_partitions(ref_group.id)

            if ref_group.real_idc == None:
                i_res = [-r for r in ref_group.resource_demand]
                heapq.heappush(group_heap, (-ref_group.resource_demand[cpu_index], ref_group.id, ref_group))
        #TODO: use higher version PYTHON and then heappushpop
        heapq.heappush(group_heap, (1, "None", None))
        (max_ds, group_id, ref_group) = heapq.heappop(group_heap) 
        while ref_group != None:
            ret, tmp_actions = self.replicate_policy.allocate_index_group(ref_group)
            if ret != 0:
                return 1
            if len(tmp_actions) > 0:
                self.action_list.extend(tmp_actions)
            (max_ds, group_id, ref_group) = heapq.heappop(group_heap)
        return 0

    def handle_offline_partition(self):
        """rebalance offline slavenode and partitions first
            Args:None
            Returns:ips
        """
        total_ip = []
        done_ip = []
        fail_ip = []
        offline_mac = [iter_node 
                for iter_node in self.slavenodes.itervalues() 
                if iter_node.online == 'offline' or len(iter_node.partitions_offline) > 0]
        offline_mac.sort(lambda x, y: cmp(len(y.partitions_offline), len(x.partitions_offline)))
        total_ip = [iter_node.id for iter_node in offline_mac]

        for iter_node in offline_mac:
            '''
            Èç¹û»úÆ÷Îªoffline,ÊµÀýÈ«DELµô, ·ñÔòÖ»É¾³ýofflineÊµÀý
            '''
            partitions = [iter_partition for iter_partition in iter_node.partitions_offline]
            if iter_node.online == 'offline':
                for iter_partition in iter_node.partitions_new_and_running:
                    partitions.append(iter_partition)
            allocate_succ = True
            for iter_partition in partitions:
                del_action = IndexPlacementAction('DEL', iter_partition, iter_node, None)
                if del_action.execute(False) == False:
                    LOG.info('handle offline node %s error, del partition=%s failed', 
                            iter_node.id, iter_partition.id)
                    allocate_succ = False
                    continue
                ret, tmp_actions = True, []
                if (not iter_partition.to_ignore) and iter_partition.replication_factor > \
                        iter_partition.replica_count_of_new_and_running():
                    ret, tmp_actions = self.replicate_policy.allocate_index_partition(
                            iter_partition, None,
                            can_cross_real_idc=True,
                            fail_if_little_idle=False)
                    if ret == False:
                        LOG.info('handle offline node %s error, add partition=%s failed, \
                                actions=[%s]', iter_node.id, iter_partition.id, tmp_actions)
                        self.action_list.extend(tmp_actions)
                        self.output_action_list()
                        del_action.rollback()
                        allocate_succ = False
                        continue
                    '''»úÆ÷²»Îªoffline£¬±»±ê¼ÇÎªerror£¬²»»áÍùÉÏÃæ·ÅÊµÀý£¬±£ÏÕÆð¼û£¬»¹ÊÇ¿¼ÂÇÖÐ¼ä×´Ì¬'''
                if iter_node.online != 'offline':
                    iter_node.add_zombie(iter_partition, consume_resource=False)
                self.action_list.append(del_action)
                self.action_list.extend(tmp_actions)
                LOG.info('handle offline node %s succ, partition=%s', 
                        iter_node.id, iter_partition.id)
            if allocate_succ:
                done_ip.append(iter_node.id)
            else:
                fail_ip.append(iter_node.id)

        return True, done_ip, total_ip, fail_ip

    #allocate all replica
    def rebalance_index_partitions_maxmax(self):
        """rebalance index part which total_container>replics
            rebalance offline node first
            Args:
            Returns:
        """
        ret, done_ip, total_ip, fail_ip = self.handle_offline_partition()
        if ret == True:
            LOG.info('handle offline node succ, count of total ip[%d], count of done ip[%d]', 
                    len(total_ip), len(done_ip))
        else:
            LOG.info('handle offline node fail, count of total ip[%d], count of done ip[%d]', 
                    len(total_ip), len(done_ip))
            LOG.info('handle offline node fail, fail ip list[%s]', str(fail_ip))
        candidates_list = self.index_partitions.values()
        if self.allocation_mode == self.ALLOCATION and (self.index_partition_to_fix is not None \
                or self.slavenode_to_fix is not None):
            LOG.info("in allocation mode, and index partition specified, num_partition=[%d]", 
                    len(self.index_partition_to_fix))
            candidates_list = self.index_partition_to_fix.values()
        if len(candidates_list) == 0:
            return

        #reducing column
        partition_heap = []
        for partition in candidates_list:
            if (not partition.to_ignore) and partition.replication_factor < partition.replica_count_of_new_and_running():
                if self.options["debug"]:
                    print "to del:", partition.id, partition.replication_factor, partition.replica_count_of_new_and_running()
                heapq.heappush(partition_heap, (-partition.resource_demand[cpu_index], partition.id, partition))
        heapq.heappush(partition_heap, (1, "None", None))
        (max_ds, part_id, partition) = heapq.heappop(partition_heap)
        while partition != None:
            tmp_actions = self.replicate_policy.deallocate_index_partition(partition)
            if len(tmp_actions) > 0:
                self.action_list.extend(tmp_actions)
            (min_ds, part_id, partition) = heapq.heappop(partition_heap)

        #adding column of layer1
        partition_heap = []
        for partition in candidates_list:
            if (not partition.to_ignore) and partition.replication_factor > partition.replica_count_of_new_and_running() and partition.layer == "layer1":
                heapq.heappush(partition_heap, (-partition.resource_demand[cpu_index], partition.id, partition))
        #TODO: use higher version PYTHON and then heappushpop
        heapq.heappush(partition_heap, (1, "None", None))
        (max_ds, par_id, partition) = heapq.heappop(partition_heap) 
        fail_if_little_idle = (self.allocation_mode==self.ALLOCATION) or (self.allocation_mode==self.RESTORE)
        while partition != None:
            (ret, tmp_actions) = self.replicate_policy.allocate_index_partition(
                    partition, None, 
                    can_cross_real_idc=False, 
                    fail_if_little_idle=fail_if_little_idle)
            if ret == False:
                self.action_list.extend(tmp_actions)
                self.output_action_list()
                oops("failed to allocate index partitions, %s", partition.id)
            if len(tmp_actions) > 0:
                self.action_list.extend(tmp_actions)
            (max_ds, part_id, partition) = heapq.heappop(partition_heap)
        #adding column of other layer(s)
        partition_heap = []
        for partition in candidates_list:
            if (not partition.to_ignore) and partition.replication_factor > partition.replica_count_of_new_and_running():
                heapq.heappush(partition_heap, (-partition.resource_demand[cpu_index], partition.id, partition))
        #TODO: use higher version PYTHON and then heappushpop
        heapq.heappush(partition_heap, (1, "None", None))
        (max_ds, par_id, partition) = heapq.heappop(partition_heap) 
        while partition != None:
            (ret, tmp_actions) = self.replicate_policy.allocate_index_partition(
                    partition, None, 
                    can_cross_real_idc=False, 
                    fail_if_little_idle=fail_if_little_idle)
            if ret == False:
                self.action_list.extend(tmp_actions)
                self.output_action_list()
                oops("failed to allocate index partitions, %s", partition.id)
            if len(tmp_actions) > 0:
                self.action_list.extend(tmp_actions)
            (max_ds, part_id, partition) = heapq.heappop(partition_heap)


    def rebalance_between_real_idcs(self):
        tmp_actions = self.replicate_policy.rebalance_real_idc()
        if len(tmp_actions) > 0:
            self.action_list.extend(tmp_actions)

    def rebalance_between_nodes(self):
        nodes_to_clear = None
        allocate_or_restore = self.allocation_mode != self.REBALANCE
        if allocate_or_restore:
            if self.index_partition_to_fix == None and self.slavenode_to_fix == None:
                LOG.info("put offline and error slavenode into nodes_to_clear")
                nodes_to_clear = [iter_node for iter_node in self.slavenodes.itervalues() if iter_node.online in ('offline', 'error')]
            elif self.slavenode_to_fix != None:
                nodes_to_clear = self.slavenode_to_fix.values()
                for iter_node in nodes_to_clear:
                    iter_node.mark_offline()
            if self.options["debug"]:
                print "allocation mode, index_partitions: %d, slavenodes %d, nodes_to_clear %d" % (len(self.index_partition_to_fix), len(self.slavenode_to_fix), len(nodes_to_clear))

            if len(nodes_to_clear) == 0:
                return
        tmp_actions = self.replicate_policy.rebalance_nodes_mem_and_flash()
        if len(tmp_actions) > 0:
            self.action_list.extend(tmp_actions)

        tmp_actions = self.replicate_policy.scatter_lightweight_inst()
        if len(tmp_actions) > 0:
            self.action_list.extend(tmp_actions)

        #rebalance cpu
        #only balance specified nodes in allocation mode due to the offline machine
        tmp_actions = self.replicate_policy.rebalance_nodes(ref_real_idc = None,
            nodes_to_clear = nodes_to_clear,
            fail_if_little_idle = allocate_or_restore,
            restore_only = (self.allocation_mode == self.RESTORE))
        if len(tmp_actions) > 0:
            self.action_list.extend(tmp_actions)

    def apply_extend_actions(self, action_str_list):
        action_list = []
        failed_list = []
        for action_str in action_str_list:
            action_str.strip()
            if action_str.startswith('#'):
                continue
            action_items = action_str.split()
            if len(action_items) < 3:
                continue

            ref_partition = self.get_index_partition(action_items[1])
            action = None
            if ref_partition is None:
                LOG.error('index_partition is not found, index_partition_id=[%s]', action_items[1])
                failed_list.append(action_str)
                continue
            if action_items[0] == 'ADD':
                ref_node = self.get_slavenode(action_items[2])
                if ref_node is None:
                    LOG.error('slavenode is not found, slavenode_id=[%s]', action_items[2])
                    failed_list.append(action_str)
                    continue
                action = IndexPlacementAction(action_items[0], ref_partition, None, ref_node)
            elif action_items[0] == 'MOVE':
                src_node = self.get_slavenode(action_items[2])
                if src_node is None:
                    LOG.error('slavenode is not found, slavenode_id=[%s]', action_items[2])
                    failed_list.append(action_str)
                    continue
                dest_node = self.get_slavenode(action_items[3])
                if dest_node is None:
                    LOG.error('slavenode is not found, slavenode_id=[%s]', action_items[3])
                    failed_list.append(action_str)
                    continue
                action = IndexPlacementAction(action_items[0], ref_partition, src_node, dest_node)
            elif action_items[0] == 'DEL':
                src_node = self.get_slavenode(action_items[2])
                if src_node is None:
                    LOG.error('slavenode is not found, slavenode_id=[%s]', action_items[2])
                    failed_list.append(action_str)
                    continue
                action = IndexPlacementAction(action_items[0], ref_partition, src_node, None)
            else:
                LOG.error('action type not supported, action_type=[%s]', action_items[0])
                failed_list.append(action_str)
                continue

            if False == action.execute():
                LOG.error('action %s execute failed', action.to_string())
                failed_list.append(action_str)
                continue
            action_list.append(action)
 
        if len(action_list) > 0:
            self.action_list.extend(action_list)
        return failed_list

    def allocate(self, partition_list=None, slavenode_list=None):
        #allocate index-group
        #deallocate index-partition
        #allocate index-partition
        #clear offline machine
        self.replicate_policy.break_up_dist = self.options["break_up_dist"]
        self.allocation_mode = self.ALLOCATION
        if slavenode_list != None:
            self.slavenode_to_fix = {}
            for iter_nid in slavenode_list:
                if iter_nid in self.slavenodes:
                    self.slavenode_to_fix[iter_nid] = self.slavenodes[iter_nid]
                else:
                    oops("slavenode specified to clear is not existed, %s", iter_nid)
        if partition_list != None:
            self.index_partition_to_fix = {}
            for iter_pid in partition_list:
                if iter_pid in self.index_partitions:
                    self.index_partition_to_fix[iter_pid] = self.index_partitions[iter_pid]
                else:
                    LOG.warning("index partition specified to fix is not existed, skipped, partition_id=[%s]", iter_pid)

        ret = self.allocate_index_groups_maxmax()
        if ret != 0:
            LOG.error("allocate index-group failed")
            return 1
        self.rebalance_index_partitions_maxmax()
        return 0
     
    def restore(self):
        self.replicate_policy.break_up_dist = self.options["break_up_dist"]
        self.allocation_mode = self.RESTORE
        self.rebalance_between_nodes()
        return 0

    def rebalance(self, break_up_dist):
        #allocate
        #rebalance index-group
        #rebalance between nodes
        self.replicate_policy.break_up_dist = break_up_dist
        self.allocation_mode = self.REBALANCE
        self.rebalance_between_nodes()
        return 0

    def generate(self, allow_move_group, break_up_dist, allocate_only, slavenode_list, partition_list):
        self.replicate_policy.break_up_dist = break_up_dist
        self.allocation_mode = self.REBALANCE
        if allocate_only:
            self.allocation_mode = self.ALLOCATION
        if slavenode_list != None and len(slavenode_list) > 0:
            self.slavenode_to_fix = {}
            for iter_nid in slavenode_list:
                if iter_nid in self.slavenodes:
                    self.slavenode_to_fix[iter_nid] = self.slavenodes[iter_nid]
                else:
                    oops("slavenode specified to clear is not existed, %s", iter_nid)
        if partition_list != None and len(partition_list) > 0:
            self.index_partition_to_fix = {}
            for iter_pid in partition_list:
                if iter_pid in self.index_partitions:
                    self.index_partition_to_fix[iter_pid] = self.index_partitions[iter_pid]
                else:
                    oops("index partition specified to fix is not existed, %s", iter_pid)


        self.allocate_index_groups_maxmax()
        if allow_move_group:
            self.rebalance_between_real_idcs()

        self.rebalance_index_partitions_maxmax()
        self.rebalance_between_nodes()
        return 0

    def allocate_slots(self):
        for action_item in self.action_list:
            if action_item == None:
                continue
            ret = action_item.occupy_slot()
            if not ret:
                oops("allocate slot failed for partition [%s] on slavenode [%s]", action_item.partition.id, action_item.dest_node.id);

    def allocate_storage(self):
        for action_item in self.action_list:
            if action_item is None:
                continue
            if action_item.action == 'DEL':
                continue
            if action_item.dest_node is None:
                LOG.warning("invalid action, no dest_node [%s %s]", action_item.action, action_item.partition.id)
            if int(action_item.partition.resource_demand[flash_index]) == 0:
                continue

            #single storage partition
            if self.options["storage_style"] == "single":
                max_size = 0
                i = 0
                pick = 0
                while i < len(action_item.dest_node.storage):
                    if max_size < action_item.dest_node.storage[i].size:
                        max_size = action_item.dest_node.storage[i].size
                        pick = i
                    i += 1
                action_item.storage.append((action_item.dest_node.storage[pick].path, action_item.partition.resource_demand[flash_index]))
                action_item.dest_node.storage[pick].size -= action_item.partition.resource_demand[flash_index]
                LOG.info("allocate index parition [%s] to path [%s] on slavenode [%s], size [%d]", action_item.partition.id, action_item.storage[0][0], action_item.dest_node.id, action_item.storage[0][1])
            #sharing in all storage partitins
            elif self.options["storage_style"] == "share":
                if len(action_item.dest_node.storage) == 0:
                    LOG.warning("allocate index_partition [%s] to slavenode [%s], but there is no storage",action_item.partition.id, action_item.dest_node.id)
                    continue
                sto_size = int(math.ceil(float(action_item.partition.resource_demand[flash_index]) / len(action_item.dest_node.storage)))
                i = 0
                while i < len(action_item.dest_node.storage):
                    action_item.storage.append((action_item.dest_node.storage[i].path, sto_size))
                    action_item.dest_node.storage[i].size -= sto_size
                    i += 1
                LOG.info("equally dividing index partition [%s] to [%d] ssds on slavenode [%s] with size [%d]", action_item.partition.id, i, action_item.dest_node.id, sto_size)


    #compress the action list to remove redundance, set to None when del an action
    def compress_action_list(self):
        i = 0
        while i < len(self.action_list):
            cur = self.action_list[i]
            if (cur == None):
                i += 1
                continue
            j = i + 1
            while j < len(self.action_list):
                cmp = self.action_list[j]
                if (cmp == None):
                    j += 1
                    continue
                if (cur.action == 'ADD'):
                    if (cmp.action == 'DEL' and cmp.partition.id == cur.partition.id and cur.dest_node.id == cmp.src_node.id):
                        self.action_list[i] = None
                        self.action_list[j] = None
                        break
                    if (cmp.action == 'MOVE' and cmp.partition.id == cur.partition.id and cur.dest_node.id == cmp.src_node.id):
                        act = IndexPlacementAction('ADD', cur.partition, None, cmp.dest_node)
                        self.action_list[i] = None
                        self.action_list[j] = act
                        break
                elif (cur.action == 'DEL'):
                    if (cmp.action == 'ADD' and cmp.partition.id == cur.partition.id):
                        if (cur.src_node.id == cmp.dest_node.id):
                            self.action_list[i] = None
                            self.action_list[j] = None
                        else:
                            act = IndexPlacementAction('MOVE', cur.partition, cur.src_node, cmp.dest_node)
                            self.action_list[i] = None
                            self.action_list[j] = act
                        break
                elif (cur.action == 'MOVE'):
                    if (cmp.action == 'MOVE' and cmp.partition.id == cur.partition.id and cur.dest_node.id == cmp.src_node.id):
                        if cur.src_node.id == cmp.dest_node.id:
                            self.action_list[i] = None
                            self.action_list[j] = None
                        else:
                            act = IndexPlacementAction('MOVE', cur.partition, cur.src_node, cmp.dest_node)
                            self.action_list[i] = None
                            self.action_list[j] = act
                        break
                j += 1
                #end of while j >= 0
            i += 1
            #end of while i >= 0

    def output_index_distr(self):
        index_distr = {'index_group':{}, 'index_partition':{}}

        for group_id, ref_group in self.index_groups.iteritems():
            index_distr['index_group'][group_id] = ref_group.real_idc_count_map 

        for part_id, ref_part in self.index_partitions.iteritems():
            index_distr['index_partition'][part_id] = {}
            index_distr['index_partition'][part_id]['group'] = ref_part.group
            index_distr['index_partition'][part_id]['real_idc'] = {}
            for ref_node in ref_part.slavenodes():
                real_idc_id = ref_node.real_idc
                if real_idc_id not in index_distr['index_partition'][part_id]['real_idc']:
                    index_distr['index_partition'][part_id]['real_idc'][real_idc_id] = 0
                index_distr['index_partition'][part_id]['real_idc'][real_idc_id] += 1
        distr_str = json.dumps(index_distr, sort_keys=True, indent=4, separators=(',',':'))
        f = open(self.output_index_distr_file, 'w')
        f.write(distr_str)
        f.close()

    def output_action_list(self):
        if (self.output_index_distr_file != None):
            self.output_index_distr()

        for action_item in self.action_list:
            if action_item == None:
                continue
            if self.output_format == 'bash':
                sys.stdout.write(action_item.to_string())
            else:
                sys.stdout.write(action_item.to_json())
            sys.stdout.write('\n')

    #TODO:to delete this func
    def dump_to_csv(self):
        result=[]
        dbs={}
        sorted_groups = self.index_groups.keys()
        sorted_groups.sort()
        for group_id in sorted_groups:
            ref_group = self.index_groups[group_id]
            ref_group.determine_real_idc()
            #cannot read from ref_group directly, it is not correct after actions in simulate
            real_idc_id = ref_group.real_idc
            if real_idc_id != None:
                result.append('%s,%s(%d/%d)\n' % (group_id, real_idc_id, ref_group.real_idc_count_map[real_idc_id], ref_group.count_instance))
            else:
                result.append('%s,%s(0/%d)\n' % (group_id, real_idc_id, ref_group.count_instance))
        #sorted_nodes = self.slavenodes.keys()
        #sorted_nodes.sort()
        result.append("slavenode placement:\n")
        for node_id, ref_node in sorted(self.slavenodes.iteritems(), key=lambda(k, v):v.cpu_idl(False)):
            result.append('%s' % node_id)
            result.append('(%s)' % ref_node.real_idc)
            #ref_node = self.slavenodes[node_id]
            if ref_node.online != 'online':
                result.append('(%s)' % ref_node.online)
            result.append(',(%.2f,%.2f,%.2f,%.2f,%.2f,%.2f)' % (
                ref_node.cpu_idl(False),
                ref_node.resource_remain[mem_index],
                ref_node.resource_total[mem_index],
                ref_node.resource_remain[flash_index],
                ref_node.resource_total[flash_index],
                ref_node.actual_resource_remain(flash_index))
                )
            for part_id, meta in ref_node.partitions.iteritems():
                dbs.setdefault(part_id, [])
                dbs[part_id].append('%s(%s)(%s)' % (node_id, ref_node.real_idc, meta['state']))
                result.append(',%s(%s)(%s)' % (part_id,
                    self.index_partitions[part_id].group, meta['state']))
            result.append('\n')
        sorted_db=dbs.keys()
        sorted_db.sort()
        result.append("index partition distribution:\n")
        for block in sorted_db:
            result.append('%s(%s)' % (block, self.index_partitions[block].group))
            for instance in dbs[block]:
                result.append( ',%s' % instance )
            result.append( '\n' )
        return ''.join(result)

    def __mb_to_gb(self, meta, key):
        if key not in meta:
            return None
        ma = self.re_store.match(meta[key])
        cap = 0

        if ma == None:
            cap = -1
        elif ma.group("unit") == 'M' or ma.group('unit') == 'MB' or ma.group('unit') == 'm' or ma.group('unit') == 'mb':
            cap = float(ma.group('value')) / 1024
        else:
            cap = float(ma.group('value'))
        return cap

    def to_string(self):
        for id, node in self.slavenodes.iteritems():
            print '%s: [%s]' % (id, node.to_string())

        for id, partition in self.index_partitions.iteritems():
            print '%s: [%s]' % (id, partition.to_string())

def parse_options(argv):
    run_mode = 'rebalancement'
    output_format = 'bash'
    allow_move_group = False
    break_up_dist = 8
    debug = False
    group_real_idc_file = None
    stable_index_types = None
    output_index_distr_file = None
    input_index_distr_file = None
    cpu_rate = 100
    cpu_idle_cross_real_idc = 0.3
    ratio_cross_real_idc = 0.15
    slavenode_list = None
    partition_list = None

    try:
        opts, args = getopt.getopt(argv[1:], 'm:f:b:r:s:o:d:u:c:i:n:p:aght', ['run-mode=', 'output-format=', 'break-up-distance=', 'group-real-idc-file=', 'stable-index-type=', 'output-index-distribution=', 'input-index-distribution=', 'use-cpu-rate=', 'idle-cross-real-idc=', 'ratio-cross-real-idc=', 'slavenodes=', 'index-partitions=','allow-move-group', 'debug', 'help', 'test'])
    except Exception, e:
        print "getopt error"
        print e
        usage()
        sys.exit(1)
    for k,v in opts:
        if k == '-m' or k == '--run-mode':
            run_mode = v           
            if run_mode != 'rebalancement' and run_mode != 'allocation' and run_mode != 'full-replacement' and run_mode != 'silent':
                print  ("invalid run mode %s" % run_mode)
                usage()
                sys.exit(1)
        elif k == '-f' or k == '--output-format':
            output_format = v
            if output_format != 'json' and output_format != 'bash':
                print  ("invalid output format %s" % output_format)
                usage()
                sys.exit(1)
        elif k == '-b' or k == '--break-up-distance':
            try:
                break_up_dist = int(v)
            except:
                print  ("invalid break up distance %s" % v)
                usage()
                sys.exit(1)
        elif k == '-r' or k == '--group-real-idc-file':
            group_real_idc_file = v
        elif k == '-s' or k == '--stable-index-type':
            stable_index_types = v
        elif k == '-a' or k == '--allow-move-group':
            allow_move_group = True
        elif k == '-o' or k == '--output-index-distribution':
            output_index_distr_file = v
        elif k == '-d' or k == '--input-index-distribution':
            input_index_distr_file = v
        elif k == '-u' or k == '--use-cpu-rate':
            try:
                cpu_rate = float(v)
            except:
                print ("invalid use-cpu-rate %s" % v)
                usage()
                sys.exit(1)
        elif k == '-c' or k == '--idle-cross-real-idc':
            #TODO: try
            cpu_idle_cross_real_idc = float(v)
        elif k == '-i' or k == '--ratio-cross-real-idc':
            #TODO: try
            ratio_cross_real_idc = float(v)
        elif k == '-n' or k == '--slavenodes':
            slavenode_list = v.split(',')
        elif k == '-p' or k == '--index-partitions':
            partition_list = v.split(',')
        elif k == '-g' or k == '--debug':
            debug = True
        elif k == '-h' or k == '--help':
            usage()
            sys.exit(0)
        elif k == '-t' or k == '--test':
            import doctest
            doctest.testmod()
            sys.exit(0)
        else:
            print "invalid option %s" % k
            usage()
            sys.exit(1)

    return (run_mode, slavenode_list, partition_list, output_format, group_real_idc_file, stable_index_types, allow_move_group, output_index_distr_file, input_index_distr_file, break_up_dist, cpu_rate, cpu_idle_cross_real_idc, ratio_cross_real_idc, debug)

def usage():
    usage_message = \
"""Generate snapshot of online index placement plan.

Usage: placement_plan_generate.py [-m|--run-mode] [-f|--output-format] [-a|--allow-move-group] [-b|--break-up-distance] [-r|--group-real-idc-file] [-s|--stable-index-type]
       placement_plan_generate.py -h|--help

Options: 
    -m|--run-mode           run-mode: rebalancement(default), allocate, full-replacement, silent
                            rebalancement: allocate all index partitions and rebalance bewteen slavenodes inside real idc and o                                           between real idc
                            allocate: allocate all index partitions not meeting replica (including offline machine), constraint                                      -ing to the user-specified list if any
    -n|--slavenodes         indicate a list of slavenodes in the format of hostname1,hostname2,...
    -p|--index-partitions   indicate a list of index-partitions, e.g. vip_0,se_3,dnews-se_23,...
    -f|--output-format       output-format: bash(default), json
    -o|--output-index-distribution output the distribution of index-partitions among real_idcs
    -d|--input-index-distribution input the distribution of another idc(JX,TC) to avoid confliction that placing index to the ssame real idc e.g. m1 for both JX and TC
    -r|--group-real-idc-file indicate which real idc should a group be placed manually, dominate config of conflict_to_avoid
    -s|--stable-index-type   indicate which index type, if any, will be ingored during placement calculation
    -a|--allow-move-group    allow moving group if necessary: disallowed(default), conflict group to avoid
    -b|--break-up-distance   distance of successive partitions to be broke up: 4(default), 0(inactive) 
    -u|--use-cpu-rate        precentage of cpu capacity of each slavenode can be used in placement-plan, 100 by default which means taking full advantage of cpu
    -c|--idle-cross-real-idc when cpu idle of max load node exceeds this threshold, may balance index across real idc
    -i|--ratio-cross-real-idc proportion of instances of a signle group allowed placed across real idc
    -t|--test                run unit tests
    -h|--help                show this help 
"""
#TODO:cross real idc
    print usage_message

def is_valid_for_placement_plan(manager):
    return is_valid_placement(manager, lambda groups:len(groups)>0, lambda idcs:len(idcs)==1)
def is_valid_for_connection(manager):
    return is_valid_placement(manager, lambda groups:len(groups)==1, lambda idcs:len(idcs)>0)
def is_valid_placement(manager,group_check,idc_check):
    groups=[]
    idcs=[]
    for slavenode in manager.slavenodes.values():
        if not slavenode.idc in idcs:
            idcs.append(slavenode.idc)
    for index_partition in manager.index_partitions.values():
        if not index_partition.group in groups and index_partition.replication_factor > 0:
            groups.append(index_partition.group)
    if group_check(groups) and idc_check(idcs):
        return True
    else:
        sys.stderr.write('%s\n%s\n' % ('\n'.join(groups), '\n'.join(idcs)))
        return False

def build_index_placement_manager(placement_plan_snapshot, cpu_rate=100, cpu_idle_cross_real_idc=1.0, ratio_cross_real_idc=0.0, unbind_bad_group=False):
    global g_stable_index_types
    manager = IndexPlacementManager()

    #manager.ratio_can_index_group_cross_real_idc = ratio_cross_real_idc
    #manager.cpu_idle_cross_real_idc = cpu_idle_cross_real_idc

    #create index partitions
    for id, meta in placement_plan_snapshot['index_partition'].iteritems():
        partition = manager.create_index_partition(id, meta)
        #if it is set to be stable, will not participate in placement calculation
        if partition.type in g_stable_index_types:
            partition.to_ignore = True
    #create slavenodes 
    for id, meta in placement_plan_snapshot['slavenode'].iteritems():
        node = manager.create_slavenode(id, meta, cpu_rate)

    #manager.rm_offline_slavenodes()

    #placement
    #group_to_unbind = {}
    for id, meta in placement_plan_snapshot['slavenode'].iteritems():
        node = manager.get_slavenode(id)
        if None == node: continue
        if not id in placement_plan_snapshot['placement']:
            continue
        for instance in placement_plan_snapshot['placement'][id]:
            partition = manager.get_index_partition(instance["partition"])
            if None==partition:
                oops("partition %s appeared in placement on slavenode %s is not indicated in index_partition", instance["partition"], id)
            bad_group = manager.place_index_partition(partition, node, instance)
            #if bad_group != None and bad_group not in group_to_unbind:
            #    LOG.warning('all partitions in the same group should be place at the same real_idc, violated by %s, will re-allocate', bad_group)
            #    group_to_unbind[bad_group] = None

    #determine host real idc for group
    for ref_group in manager.index_groups.itervalues():
        ref_group.determine_real_idc()

    manager.action_list = []

    #zombies
    for id, meta in placement_plan_snapshot['zombie'].iteritems():
        manager.add_zombie(id, meta)

    #unbind bad groups
    #if unbind_bad_group:
    #    for bad_group_id in group_to_unbind.iterkeys():
    #        manager.unbind_bad_index_group_and_partitions(bad_group_id)

    

    for i in range(0, len(manager.resource_total)):
        if manager.resource_total[i] == 0:
            manager.resource_total[i] = -1
    return manager

def top_average(sorted_item_list, proportion):
    count = int(len(sorted_item_list) * proportion)
    if count == 0: count = count + 1
    i = 0
    sum = 0
    while i < count: 
        sum += sorted_item_list[i]
        i += 1
    return float(sum)/count

def count_max_min_mean_var_top(item_list, has_divisor):
    if len(item_list) == 0: return
    tmp_list = item_list
    if has_divisor:
        tmp_list = [ float(item[0])/item[1] for item in item_list ]
    #since cpu is measure by idl, sorted incrementally
    tmp_list.sort()
    mean = top_average(tmp_list, 1)
    variance = 0
    for cur in tmp_list:
        diff = cur - mean
        variance += diff*diff
    variance = float(variance) / len(tmp_list)
    std_variance = math.sqrt(variance)

    return (tmp_list[-1], tmp_list[0], mean, std_variance, top_average(tmp_list, 0.01), top_average(tmp_list, 0.05), top_average(tmp_list, 0.1))


def count_max_min_mean(item_list, has_divisor):
    if len(item_list) == 0: return
    if has_divisor:
        max = min = float(item_list[0][0])/item_list[0][1]
    else:
        max = min = item_list[0]
    mean = 0
    for item in item_list:
        if has_divisor:
            cur = float(item[0]) / item[1]
        else:
            cur = item
        if max < cur: max = cur
        if min > cur: min = cur
        mean += cur
    mean = float(mean) / len(item_list)

    variance = 0
    for item in item_list:
        if has_divisor:
            cur = float(item[0]) / item[1]
        else:
            cur = item
        diff = cur - mean
        variance += diff*diff
    variance = float(variance) / len(item_list)
    std_variance = math.sqrt(variance)

    return (max, min, mean, std_variance)
        
def plan_evaluation(manager):
    stat_str_csv =[]
    node_load = {}
    node_load_domain = {}
    node_load_real_idc = {}
    not_clear_offline = {}
    violation_domain = {}
    violation_part = {}
    violation_part_list = []
    domain_parts = {}
    parts_replica = {}
    node_flash = {}
    node_flash_actual = {}
    node_flash_real_idc = {}
    node_mem_real_idc = {}
    node_mem = {}
    node_actual_mem_real_idc = {}
    node_actual_mem = {}

    for ref_node in manager.slavenodes.itervalues():
        loads = ref_node.cpu_idl(False)
        node_id = ref_node.id
        parts = ref_node.index_partitions_of_new_and_running()

        if (ref_node.online == 'offline'):
            if len(parts) > 0:
                not_clear_offline[node_id] = None
            continue

        domain_id = ref_node.failure_domain
        real_idc_id = ref_node.real_idc
        node_load[node_id] = loads
        node_flash[node_id] = ref_node.resource_remain[flash_index]
        node_flash_actual[node_id] = ref_node.actual_resource_remain(flash_index)
        if real_idc_id not in node_flash_real_idc:
            node_flash_real_idc[real_idc_id] = []
        node_flash_real_idc[real_idc_id].append(ref_node.resource_remain[flash_index])
        node_mem[node_id] = ref_node.resource_remain[mem_index]
        node_actual_mem[node_id] = ref_node.actual_resource_remain(mem_index)
        if real_idc_id not in node_mem_real_idc:
            node_mem_real_idc[real_idc_id] = []
        node_mem_real_idc[real_idc_id].append(ref_node.resource_remain[mem_index])
        if real_idc_id not in node_actual_mem_real_idc:
            node_actual_mem_real_idc[real_idc_id] = []
        node_actual_mem_real_idc[real_idc_id].append(ref_node.actual_resource_remain(mem_index))

        if domain_id not in domain_parts:
            domain_parts[domain_id] = {}
        for part in parts:
            part_id = part.id
            if part_id not in parts_replica:
                parts_replica[part_id] = 0
            parts_replica[part_id] += 1
            if part_id in domain_parts[domain_id]:
                violation_domain[domain_id] = None
            domain_parts[domain_id][part_id] = None

       
        if domain_id not in node_load_domain:
            node_load_domain[domain_id] = [0, 0]
        node_load_domain[domain_id][0] += loads
        node_load_domain[domain_id][1] += 1
        
        if real_idc_id not in node_load_real_idc:
            node_load_real_idc[real_idc_id] = []
        node_load_real_idc[real_idc_id].append(loads)
    #end for
    for id, ref_part in manager.index_partitions.iteritems():
        rep_factor = ref_part.replication_factor
        if id not in parts_replica:
            if rep_factor != 0:
                violation_part[("%s,0:%d,"%(id, rep_factor))] = None
                violation_part_list.append([id, 0, rep_factor])
        elif parts_replica[id] != rep_factor:
            violation_part[("%s,%d:%d,"%(id, parts_replica[id], rep_factor))] = None
            violation_part_list.append([id, parts_replica[id], rep_factor])

    evaluation_json = {}
    stat_str_csv.append("number of violative failure_domain, %d, %s\n" % (len(violation_domain), ' '.join(violation_domain)))
    evaluation_json['violative_failure_domain'] = {}
    evaluation_json['violative_failure_domain']['num'] = len(violation_domain)
    evaluation_json['violative_failure_domain']['list'] = violation_domain.keys()
    
    stat_str_csv.append("number of partitions not matching replication, %d, %s\n" % (len(violation_part), ' '.join(violation_part)))
    evaluation_json['not_match_replication_partitions'] = {}
    evaluation_json['not_match_replication_partitions']['num'] = len(violation_part_list)
    evaluation_json['not_match_replication_partitions']['list'] = violation_part_list
    
    stat_str_csv.append("number of offline slavenode not cleared, %d, %s\n" % (len(not_clear_offline), ' '.join(not_clear_offline)))
    evaluation_json['offline_slavenodes'] = {}
    evaluation_json['offline_slavenodes']['num'] = len(not_clear_offline)
    evaluation_json['offline_slavenodes']['list'] = not_clear_offline.keys()
    
    stat_str_csv.append("statistical characteristics, max, min, mean, standard variance, mean of top1%, mean of top 5%, mean of top 10%\n")
    re = count_max_min_mean_var_top(node_load.values(), False)
    stat_str_csv.append("node cpu idel(%d), %.2f, %.2f, %.2f, %.4f, %.2f, %.2f, %.2f\n" % (len(node_load), re[0], re[1], re[2], re[3], re[4], re[5], re[6]))
    evaluation_json['cpu_idle'] = {}
    evaluation_json['cpu_idle']['num'] = len(node_load)
    evaluation_json['cpu_idle']['max'] = re[0]
    evaluation_json['cpu_idle']['min'] = re[1]
    evaluation_json['cpu_idle']['mean'] = re[2]
    evaluation_json['cpu_idle']['standard_deviation'] = re[3]
    evaluation_json['cpu_idle']['mean_of_top1_percent'] = re[4]
    evaluation_json['cpu_idle']['mean_of_top5_percent'] = re[5]
    evaluation_json['cpu_idle']['mean_of_top10_percent'] = re[6]

    re = count_max_min_mean_var_top(node_flash.values(), False)
    stat_str_csv.append("node flash remains(GB)(%d), %.2f, %.2f, %.2f, %.4f, %.2f, %.2f, %.2f\n" % (len(node_flash), re[0], re[1], re[2], re[3], re[4], re[5], re[6]))
    evaluation_json['flash_remain'] = {}
    evaluation_json['flash_remain']['num'] = len(node_flash)
    evaluation_json['flash_remain']['max'] = re[0]
    evaluation_json['flash_remain']['min'] = re[1]
    evaluation_json['flash_remain']['mean'] = re[2]
    evaluation_json['flash_remain']['standard_deviation'] = re[3]
    evaluation_json['flash_remain']['mean_of_top1_percent'] = re[4]
    evaluation_json['flash_remain']['mean_of_top5_percent'] = re[5]
    evaluation_json['flash_remain']['mean_of_top10_percent'] = re[6]
    
    re = count_max_min_mean_var_top(node_flash_actual.values(), False)
    stat_str_csv.append("node flash actual remains(GB)(%d), %.2f, %.2f, %.2f, %.4f, %.2f, %.2f, %.2f\n" % (len(node_flash), re[0], re[1], re[2], re[3], re[4], re[5], re[6]))
    evaluation_json['flash_actual_remain'] = {}
    evaluation_json['flash_actual_remain']['num'] = len(node_flash)
    evaluation_json['flash_actual_remain']['max'] = re[0]
    evaluation_json['flash_actual_remain']['min'] = re[1]
    evaluation_json['flash_actual_remain']['mean'] = re[2]
    evaluation_json['flash_actual_remain']['standard_deviation'] = re[3]
    evaluation_json['flash_actual_remain']['mean_of_top1_percent'] = re[4]
    evaluation_json['flash_actual_remain']['mean_of_top5_percent'] = re[5]
    evaluation_json['flash_actual_remain']['mean_of_top10_percent'] = re[6]
    
    stat_str_csv.append("average node flash remains(GB) grouped by physical idc:\n")
    evaluation_json['idc_flash_remain'] = {}
    for real_idc_id, ri_node_flashs in node_flash_real_idc.iteritems():
        re = count_max_min_mean_var_top(ri_node_flashs, False) 
        stat_str_csv.append("%s(%d), %.2f, %.2f, %.2f, %.4f, %.2f, %.2f, %.2f\n" % (real_idc_id, len(ri_node_flashs), re[0], re[1], re[2], re[3], re[4], re[5], re[6]))
        idc_flash_remain = {}
        idc_flash_remain['num'] = len(ri_node_flashs)
        idc_flash_remain['max'] = re[0]
        idc_flash_remain['min'] = re[1]
        idc_flash_remain['mean'] = re[2]
        idc_flash_remain['standard_deviation'] = re[3]
        idc_flash_remain['mean_of_top1_percent'] = re[4]
        idc_flash_remain['mean_of_top5_percent'] = re[5]
        idc_flash_remain['mean_of_top10_percent'] = re[6]
        evaluation_json['idc_flash_remain'][real_idc_id] = idc_flash_remain

    re = count_max_min_mean_var_top(node_mem.values(), False)
    stat_str_csv.append("node memory remains(GB)(%d), %.2f, %.2f, %.2f, %.4f, %.2f, %.2f, %.2f\n" % (len(node_mem), re[0], re[1], re[2], re[3], re[4], re[5], re[6]))
    evaluation_json['memory_remain'] = {}
    evaluation_json['memory_remain']['num'] = len(node_mem)
    evaluation_json['memory_remain']['max'] = re[0]
    evaluation_json['memory_remain']['min'] = re[1]
    evaluation_json['memory_remain']['mean'] = re[2]
    evaluation_json['memory_remain']['standard_deviation'] = re[3]
    evaluation_json['memory_remain']['mean_of_top1_percent'] = re[4]
    evaluation_json['memory_remain']['mean_of_top5_percent'] = re[5]
    evaluation_json['memory_remain']['mean_of_top10_percent'] = re[6]

    stat_str_csv.append("average node mem remains(GB) grouped by physical idc:\n")
    evaluation_json['idc_memory_remain'] = {}
    for real_idc_id, ri_node_mems in node_mem_real_idc.iteritems():
        re = count_max_min_mean_var_top(ri_node_mems, False) 
        stat_str_csv.append("%s(%d), %.2f, %.2f, %.2f, %.4f, %.2f, %.2f, %.2f\n" % (real_idc_id, len(ri_node_mems), re[0], re[1], re[2], re[3], re[4], re[5], re[6]))
        idc_memory_remain = {}
        idc_memory_remain['num'] = len(ri_node_mems)
        idc_memory_remain['max'] = re[0]
        idc_memory_remain['min'] = re[1]
        idc_memory_remain['mean'] = re[2]
        idc_memory_remain['standard_deviation'] = re[3]
        idc_memory_remain['mean_of_top1_percent'] = re[4]
        idc_memory_remain['mean_of_top5_percent'] = re[5]
        idc_memory_remain['mean_of_top10_percent'] = re[6]
        evaluation_json['idc_memory_remain'][real_idc_id] = idc_memory_remain

    re = count_max_min_mean_var_top(node_actual_mem.values(), False)
    stat_str_csv.append("node actual memory remains(GB)(%d), %.2f, %.2f, %.2f, "
        "%.4f, %.2f, %.2f, %.2f\n" % (len(node_actual_mem), re[0], re[1], re[2],
        re[3], re[4], re[5], re[6]))
    evaluation_json['memory_actual_remain'] = {}
    evaluation_json['memory_actual_remain']['num'] = len(node_actual_mem)
    evaluation_json['memory_actual_remain']['max'] = re[0]
    evaluation_json['memory_actual_remain']['min'] = re[1]
    evaluation_json['memory_actual_remain']['mean'] = re[2]
    evaluation_json['memory_actual_remain']['standard_deviation'] = re[3]
    evaluation_json['memory_actual_remain']['mean_of_top1_percent'] = re[4]
    evaluation_json['memory_actual_remain']['mean_of_top5_percent'] = re[5]
    evaluation_json['memory_actual_remain']['mean_of_top10_percent'] = re[6]
    
    stat_str_csv.append("average node actual mem remains(GB) grouped by physical idc:\n")
    evaluation_json['idc_memory_actual_remain'] = {}
    for real_idc_id, ri_node_mems in node_actual_mem_real_idc.iteritems():
        re = count_max_min_mean_var_top(ri_node_mems, False) 
        stat_str_csv.append("%s(%d), %.2f, %.2f, %.2f, %.4f, %.2f, %.2f, %.2f\n"
            % (real_idc_id, len(ri_node_mems), re[0], re[1], re[2], re[3], re[4], re[5], re[6]))
        idc_memory_actual_remain = {}
        idc_memory_actual_remain['num'] = len(ri_node_mems)
        idc_memory_actual_remain['max'] = re[0]
        idc_memory_actual_remain['min'] = re[1]
        idc_memory_actual_remain['mean'] = re[2]
        idc_memory_actual_remain['standard_deviation'] = re[3]
        idc_memory_actual_remain['mean_of_top1_percent'] = re[4]
        idc_memory_actual_remain['mean_of_top5_percent'] = re[5]
        idc_memory_actual_remain['mean_of_top10_percent'] = re[6]
        evaluation_json['idc_memory_actual_remain'][real_idc_id] = idc_memory_actual_remain

    re = count_max_min_mean_var_top(node_load_domain.values(), True)
    stat_str_csv.append("average node cpu idl grouped by failure domain(%d), %.2f, %.2f, %.2f, %.4f, %.2f, %.2f, %.2f\n" % (len(node_load_domain), re[0], re[1], re[2], re[3], re[4], re[5], re[6]))
    evaluation_json['domain_cpu_idle'] = {}
    evaluation_json['domain_cpu_idle']['num'] = len(node_load_domain)
    evaluation_json['domain_cpu_idle']['max'] = re[0]
    evaluation_json['domain_cpu_idle']['min'] = re[1]
    evaluation_json['domain_cpu_idle']['mean'] = re[2]
    evaluation_json['domain_cpu_idle']['standard_deviation'] = re[3]
    evaluation_json['domain_cpu_idle']['mean_of_top1_percent'] = re[4]
    evaluation_json['domain_cpu_idle']['mean_of_top5_percent'] = re[5]
    evaluation_json['domain_cpu_idle']['mean_of_top10_percent'] = re[6]
    
    stat_str_csv.append("average node cpu idl grouped by physical idc:\n")
    evaluation_json['idc_cpu_idle'] = {}
    for real_idc_id, ri_node_loads in node_load_real_idc.iteritems():
        re = count_max_min_mean_var_top(ri_node_loads, False) 
        stat_str_csv.append("%s(%d), %.2f, %.2f, %.2f, %.4f, %.2f, %.2f, %.2f\n" % (real_idc_id, len(ri_node_loads), re[0], re[1], re[2], re[3], re[4], re[5], re[6]))
        idc_cpu_idle = {}
        idc_cpu_idle['num'] = len(ri_node_loads)
        idc_cpu_idle['max'] = re[0]
        idc_cpu_idle['min'] = re[1]
        idc_cpu_idle['mean'] = re[2]
        idc_cpu_idle['standard_deviation'] = re[3]
        idc_cpu_idle['mean_of_top1_percent'] = re[4]
        idc_cpu_idle['mean_of_top5_percent'] = re[5]
        idc_cpu_idle['mean_of_top10_percent'] = re[6]
        evaluation_json['idc_cpu_idle'][real_idc_id] = idc_cpu_idle
    
    #stat_str_csv.append("top 0.1 slavenode tail groups:\n")
    #for real_idc_id in node_load_real_idc.iterkeys():
    #    real_idc = manager.real_idcs[real_idc_id]
    #    re = real_idc.get_tail_groups()
    #    if len(re) > 5:
    #        re = re[:5]
    #    result_str = ""
    #    for group_id, cpu in re[:len(re) - 1]:
    #        result_str += "%s(%.4f)," % (group_id, cpu)
    #    if len(re) > 0:
    #        result_str += "%s(%.4f)" % (re[len(re) - 1][0], re[len(re) - 1][1])
    #    stat_str_csv.append("%s, %s\n" % (real_idc_id, result_str))
    #count = int(0.01 * len(node_load))
    #stat_str_csv.append("top 1%% slavnodes(%d of %d):\n" % (count, len(node_load)))
    #for host, cpu_idl in  sorted(node_load.iteritems(), key=lambda(k,v):(v,k)):
    #    if count == 0:
    #        break
    #    stat_str_csv.append("%s, %.2f\n" % (host, cpu_idl))
    #    count -= 1

    return stat_str_csv, evaluation_json    
    
def simulate(in_name, act_name, io_out=sys.stdout):
    '''read input and actions, then output the dest outlook
    '''
    plan=None
    try:
        plan=json.loads(open(in_name).read())
    except Exception,e:
        sys.stderr.write('load json from in_name [%s] failed [%s]\n' % (in_name, e))
    manager = build_index_placement_manager(plan)
    orig_csv = manager.dump_to_csv()
    eval_orig, eval_json_orig = plan_evaluation(manager)

    actions=open(act_name).readlines()
    for action in actions:
        if action == None:
            continue

        args=action.split()
        if len(args) < 3:
            sys.stderr.write('error cmd %s\n' % action)
            return False
        index=args[1]
        node=args[2]
        ref_partition = manager.get_index_partition(index)
        ref_node = manager.get_slavenode(node)
        if action.startswith('ADD'):
           action = IndexPlacementAction('ADD', ref_partition, None, ref_node)
           if False == action.execute():
                oops('action %s execute failed', action.to_string())
        elif action.startswith('DEL'):
            action = IndexPlacementAction('DEL', ref_partition, ref_node, None)
            if False == action.execute():
                oops('action %s execute failed', action.to_string())
        elif action.startswith('MOVE'):
            node2=args[3]
            ref_node2=manager.get_slavenode(node2)
            action = IndexPlacementAction('MOVE', ref_partition, ref_node, ref_node2)
            if False == action.execute():
                oops('action %s execute failed', action.to_string())
 
    later_csv = manager.dump_to_csv()
    note='note' in plan and plan['note'] or ''

    eval_later, eval_json_later = plan_evaluation(manager)
    io_out.write('Orig:\n%s\n\nLater:\n%s\n\nActions:\n%s\n\nEvaluation:\nOrig:\n%s\nLater:\n%s\n\nNote:\n%s' %  
            (orig_csv, later_csv, ''.join(actions), ''.join(eval_orig), ''.join(eval_later), note))


def json_to_csv(obj):
    '''convert the json placement to csv format.
    '''
    place=obj['placement']
    nodes=obj['slavenode']
    dbs={}
    result=[]
    sorted_nodes=place.keys()
    sorted_nodes.sort()
    for node in sorted_nodes:
        result.append( '%s' % node )
        if not nodes[node]['online_state'] == 'online':
            result.append( '(%s)' % (nodes[node]['online_state']))
        sorted_place=place[node]
        sorted_place.sort()
        for block in sorted_place:
            dbs.setdefault(block["partition"],[])
            dbs[block["partition"]].append('%s(%s)' % ((node,block["state"]) ))
            result.append(',%s(%s)' % (block["partition"],block["state"]))
        result.append( '\n' )
    sorted_db=dbs.keys()
    sorted_db.sort()
    for block in sorted_db:
        result.append( '%s' % block )
        for instance in dbs[block]:
            result.append( ',%s' % instance )
        result.append( '\n' )
    return ''.join(result)



def main():
    global debug, g_group_guide, g_stable_index_types, g_index_conflict_to_avoid
    run_mode, slavenode_list, partition_list, output_format, group_conf_file, stable_index_types, allow_move_group, output_index_distr_file, input_index_distr_file, break_up_dist, cpu_rate, cpu_idle_cross_real_idc, ratio_cross_real_idc,  debug = parse_options(sys.argv)

    if (stable_index_types != None):
        types = stable_index_types.split(',')
        for type in types:
            type = type.strip()
            g_stable_index_types[type] = None

    if group_conf_file != None:
        try:
            g_group_guide = json.loads(file(group_conf_file, 'r').read())
        except Exception,e:
            sys.stderr.write("invalid group real idc conf file:%s\n" % e)
            sys.exit(1)
    if input_index_distr_file != None and input_index_distr_file != "None":
        try:
            g_index_conflict_to_avoid = json.loads(file(input_index_distr_file, 'r').read())
        except Exception,e:
            sys.stderr.write("invalid group real idc conf file:%s\n" % e)
            sys.exit(1)

    try:
        placement_plan_snapshot = json.loads(sys.stdin.read())
    except Exception,e:
        sys.stderr.write("invalid placement plan snapshot:%s\n" % e)
        sys.exit(1)

    manager = build_index_placement_manager(placement_plan_snapshot, cpu_rate, cpu_idle_cross_real_idc, ratio_cross_real_idc, True)
    manager.output_index_distr_file = output_index_distr_file

    #if not is_valid_for_placement_plan(manager):
    #   sys.stderr.write("invalid placement plan snapshot: use --idc-id=yyy to dump\n")
    #   sys.exit(1)

    if run_mode == 'full-replacement':
        for group_id in manager.index_groups.iterkeys():
            manager.unbind_index_group_and_partitions(group_id, with_zombie=False)

    manager.output_format = output_format
    if run_mode != 'silent':
        manager.generate(allow_move_group, break_up_dist, (run_mode=='allocation'), slavenode_list, partition_list)
    manager.compress_action_list()
    manager.allocate_slots()
    manager.output_action_list()


if __name__ == '__main__':
    main()



