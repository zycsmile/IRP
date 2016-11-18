#!/usr/bin/env python
########################################################################
# 
# Copyright (c) 2014 Baidu.com, Inc. All Rights Reserved
# 
########################################################################
 
'''
File: am_task_manager.py
Author: guoshaodan01(guoshaodan01@baidu.com)
Date: 2014/03/24 23:39:49
'''
import multiprocessing
import threading
import time
import os
import importlib
import json
import sys
import copy

import am_global
import logging
import opzk

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

class TaskSignal(opzk.ZKObject):
    def __init__(self, task_instance, zkcli, signal_name, meta=None):
        """singnal send to task-instance, meta is user-defined
        """
        super(TaskSignal, self).__init__(id=signal_name, meta=meta, zk_path=None, zkcli=zkcli, with_cache=False)
        self.task_instance = task_instance
        self._zk_path = "%s/%s" % (self.task_instance.zk_path(), self._id)

class TaskInstance(opzk.ZKObject):
    """struct to describe a a task instance
    """
    def __init__(self, task, zkcli, handler_options={}, id=None, meta=None):
        """task: reference of Task object
           handler_options: args to task_handler, should be in the type of dict. When executing task, the args to task_handler function will be a tuple in the form of (TaskInstance, TaskContext), handler_options can be get by get_options(). {} by default
           the meta will contains:
           {
               create_timestamp:
               start_timestamp:
               end_timestamp: integer as the name implies
               wait_for_signal: if blocked waiting for any signal
               step: current progresss of the task executing
               options: dict of user-specified args
               error: error msg
           }

        """
        super(TaskInstance, self).__init__(id=id, meta=meta, zk_path=None, zkcli=zkcli, with_cache=False)

        self.task = task

        if id is None:
            self._meta['create_timestamp'] = int(time.time())
            self._meta['start_timestamp'] = -1
            self._meta['end_timestamp'] = -1
            self._id = "%s.%s.%d" % (self.task.task_group.id(), self.task.id(), self._meta['create_timestamp'])
            self._meta['options'] = handler_options
            self._meta['wait_for_signal'] = ''
            self._meta['step'] = ''
            self._meta['error'] = ''

        if 'dump_file' not in self._meta:
            self._meta['dump_file'] = ''
        if 'plan_file' not in self._meta:
            self._meta['plan_file'] = ''
        if 'simulate_file' not in self._meta:
            self._meta['simulate_file'] = ''
        if 'preview_file' not in self._meta:
            self._meta['preview_file'] = ''
        if 'success_instance_file' not in self._meta:
            self._meta['success_instance_file'] = ''
        if 'fail_instance_file' not in self._meta:
            self._meta['fail_instance_file'] = ''
            
        if 'simulate_check_file' not in self._meta:
            self._meta['simulate_check_file'] = ''
        if 'preview_check_file' not in self._meta:
            self._meta['preview_check_file'] = ''
        if 'commit_check_file' not in self._meta:
            self._meta['commit_check_file'] = ''

        self._zk_path = "%s/%s" % (self.task.zk_path(), self._id)
        self._signals = {}

    def task_group_id(self):
        return self.task.task_group.id()

    def merge_options(self, default_options, up_options):
        for k,v in up_options.iteritems():
            default_options[k] = v
        return default_options

    def get_options(self):
        inst_opt = self.get_meta_attr('options')
        default_opt = self.task.get_meta_attr('default_options')
        return self.merge_options(default_opt, inst_opt)

    def wait(self, signal_name, check_interval_sec=10, max_wait=-1):
        """wait for signal. get and signal from zk, will not remove
        Args:
            signal_name, name of signal waiting for
            check_interval_sec, to check signal every check_interval_sec seconds, no blocking if check_interval_sec<=0
            max_wait, max time to check signal, return if exceed the time, keep trying untill receive if max_wait<0
        """
        #set_wait
        if self._meta['wait_for_signal'] != signal_name:
            self.set_meta_attr('wait_for_signal', signal_name)

        ref_signal = None
        while max_wait != 0:
            ret, signal_list = self._zkcli.List(self._zk_path, cache=False)
            if signal_name in signal_list:
                ret, signal_meta = self._zkcli.Get('%s/%s' % (self._zk_path, signal_name), cache=False)
                if ret != 0:
                    logging.warning("get signal meta failed, task_instance_id=[%s], signal_name=[%s]", self.id(), signal_name)
                else:
                    ref_signal = TaskSignal(self, self._zkcli, signal_name, signal_meta)
                    self._signals[signal_name] = ref_signal
                    logging.info("receive signal, task_instance_id=[%s], signal_name=[%s]", self.id(), signal_name)
            else:
                logging.info("check signal, still wait, task_instance_id=[%s], signal_name=[%s]", self.id(), signal_name)

            if ref_signal is not None or check_interval_sec <= 0:
                break
            if max_wait > 0:
                max_wait -= 1
            time.sleep(check_interval_sec)

        if ref_signal is not None:
            self.set_meta_attr('wait_for_signal', '')

        return ref_signal

    def send_signal(self, signal_name, meta=None):
        ref_signal = None
        if signal_name in self._signals:
            ref_signal = self._signals[signal_name]
        else:
            ref_signal = TaskSignal(self, self._zkcli, signal_name, meta)
            self._signals[signal_name] = ref_signal
        ref_signal.set_meta_all(meta)
        logging.info("send signal successfully, task_instance_id=[%s], signal_name=[%s]", self.id(), signal_name)
        return ref_signal

    def set_step(self, step):
        return self.set_meta_attr('step', step)
    def get_step(self):
        return self.get_meta_attr('step')
    def set_error(self, error_msg):
        return self.set_meta_attr('error', error_msg)

class TaskGroup(opzk.ZKObject):
    """Task Group, the task under the same group is mutually exclusive
    """
    def __init__(self, group_name, zkcli, meta=None):
        super(TaskGroup, self).__init__(id=group_name, meta=meta, zk_path="task_group/%s" % group_name, zkcli=zkcli)
        self._tasks = {}

    def add_task(self, ref_task):
        if ref_task is None:
            return -1

        if ref_task.id() in self._tasks:
            return -1

        self._tasks[ref_task.id()] = ref_task
        return 0

    def new_task(self, task_name, task_module_name, task_handler_name, default_options, timer=None):
        if task_name in self._tasks:
            return None
        ref_task = Task(task_name, self, task_module_name=task_module_name, task_handler_name=task_handler_name, zkcli=self._zkcli, default_options=default_options, timer=timer)
        self._tasks[task_name] = ref_task
        ref_task.add_object()

        logging.info("add task successfully, task-group_name=[%s], task_name=[%s]", self._id, task_name)
        return ref_task

    def get_task(self, task_name):
        if task_name not in self._tasks:
            return None
        return self._tasks[task_name]
    def list_task(self):
        return self._tasks.keys()
    def count_task(self):
        return len(self._tasks)

    def remove_task(self, task_name):
        ref_task = self.get_task(task_name)
        if ref_task is None:
            return 0

        if ref_task.count_task_instance() > 0:
            return 1

        if ref_task.remove_object() != 0:
            return -1

        del self._tasks[task_name]
        return 0


class Task(opzk.ZKObject):
    """Task
    """
    def __init__(self, task_name, task_group, zkcli, task_module_name=None, task_handler_name=None, default_options=None, timer=None, meta=None):
        """task_group: reference of TaskGroup object
           task_module_name: moudule name of task handler
           task_handler_name: function name of task handler
           the meta will contains:
           {
               module_name: string, module name of task handler implementation
               handler_name: string, handler name of task, should be a function in module of module_name
               default_options: dict, options passed to task handler by default
               timer: int, optional, task will run periodically if set
           }

        """
        super(Task, self).__init__(id=task_name, meta=meta, zk_path=("%s/%s" % (task_group.zk_path(), task_name)), zkcli=zkcli)

        self.task_group = task_group
        self._task_instances = {}

        if task_module_name is not None:
            self._meta['module_name'] = task_module_name
        if task_handler_name is not None:
            self._meta['handler_name'] = task_handler_name
        if default_options is not None:
            self._meta['default_options'] = default_options
        if timer is not None:
            self._meta['timer'] = timer


    def add_task_instance(self, ref_task_instance):
        if ref_task_instance is None:
            return -1

        if ref_task_instance.id() in self._task_instances:
            return -1

        self._task_instances[ref_task_instance.id()] = ref_task_instance
        return 0

    def set_timer(self, interval_sec):
        self._meta['timer'] = interval_sec
        return self._set_node()

    def new_task_instance(self, handler_options={}):
        merged_options = copy.deepcopy(self._meta['default_options'])
        self.merge_dict(merged_options, handler_options)

        ref_task_instance = TaskInstance(task=self, zkcli=self._zkcli, handler_options=merged_options)
        self._task_instances[ref_task_instance.id()] = ref_task_instance
        ref_task_instance.add_object()
        return ref_task_instance

    def get_task_instance(self, task_instance_id):
        if task_instance_id not in self._task_instances:
            return None
        return self._task_instances[task_instance_id]
    def list_task_instance(self):
        return self._task_instances.keys()
    def count_task_instance(self):
        return len(self._task_instances)

    def remove_task_instance(self, task_instance_id):
        ref_task_instance = self.get_task_instance(task_instance_id)
        if ref_task_instance is None:
            return 0

        if ref_task_instance.remove_object_r() != 0:
            return -1

        del self._task_instances[task_instance_id]
        return 0



class TaskManager(object):
    """Task Manager
    """
    def __init__(self, timeout_conf, zk_server, zk_root, zk_user, zk_pass, task_context, 
            check_task_interval=30):
        """timeout_conf: timeout config for task process grouped by tast_group
        """
        #task_queue_ definition: {task_group:[task_instance, Process Object]}
        self.TASK_ITEM_IDX = 0
        self.TASK_EXECUTOR_IDX = 1
        self._check_task_interval = check_task_interval

        self._task_groups = {}
        self._task_queue = {}
        self._queue_lock = threading.Lock()
        self._timeout_conf = timeout_conf
        self._is_running = False

        self._zkcli = opzk.ZkcliHelper(am_global.zkcli_path, zk_server, zk_root, zk_user, zk_pass)
        #don't Init zkcli, do not need to fill cach in zkcli
        self._task_context = task_context

    def Init(self):
        """init all Task Config from ZK
        """
        ret, tmp_meta = self._meta = self._zkcli.Get('task_group', cache=False)
        if ret != 0:
            ret = self._meta = self._zkcli.Set('task_group', {})
            if ret != 0:
                logging.error('create zk_node failed, node=[task_group]')
                return ret

        ret, task_group_list = self._zkcli.List('task_group')
        if ret != 0:
            logging.error("list task group failed, node=[task_group]")
            return ret
        logging.info("get task group list, group_list=[%s], ret=[%d]", ','.join(task_group_list), ret)
        for task_group_id in task_group_list:
            ret, group_meta = self._zkcli.Get('task_group/%s' % task_group_id)
            if ret != 0:
                logging.error('get meta of task_group failed, task_group_name=[%s]', task_group_id)
                return ret
            ref_task_group = TaskGroup(task_group_id, self._zkcli, group_meta)
            self._task_groups[task_group_id] = ref_task_group

            ret, task_list = self._zkcli.List(ref_task_group.zk_path())
            if ret != 0:
                logging.error("list task group failed, task_group_name=[%s]", task_group_id)
                return ret
            for task_id in task_list:
                ret, task_meta = self._zkcli.Get('%s/%s' % (ref_task_group.zk_path(), task_id))
                if ret != 0:
                    logging.error('get meta of task failed, task_group_name=[%s], task_name=[%s]', task_group_id, task_id)
                    return ret
                ref_task =  Task(task_id, ref_task_group, zkcli=self._zkcli, meta=task_meta)
                ref_task_group.add_task(ref_task)

                ret, task_instance_list = self._zkcli.List(ref_task.zk_path())
                if ret != 0:
                    logging.error("list task instance failed, task_group_name=[%s], task_name=[%s]", task_group_id, task_id)
                    return ret
                logging.info("restore task, task=[%s], task_group=[%s]", task_id, task_group_id)
                for task_instance_id in task_instance_list:
                    ret, task_instance_meta = self._zkcli.Get('%s/%s' % (ref_task.zk_path(), task_instance_id))
                    if ret != 0:
                        logging.warn("get task instance meta failed, path=[%s]", ref_task.zk_path())
                        continue
                    #TODO
                    ref_task_instance =  TaskInstance(task=ref_task, zkcli=self._zkcli, id=task_instance_id, meta=task_instance_meta)
                    ref_task.add_task_instance(ref_task_instance)
                    if 'end_timestamp' not in task_instance_meta:
                        logging.warn('meta is invalid, no end_timestamp, task_instance_id=[%s]', task_instance_id)
                        continue
                    #re run task is dangerous
                    """
                    if task_instance_meta['end_timestamp'] == -1:
                        ret = self.run_task(ref_task_instance)
                        logging.warn('restore task instance failed, task_instance_id=[%s], ret=[%d]', ref_task_instance.id(), ret)
                    """
        return 0



    def new_task_group(self, task_group_name, meta=None):
        if task_group_name in self._task_groups:
            return None
        ref_task_group = TaskGroup(task_group_name, self._zkcli, meta)
        self._task_groups[task_group_name] = ref_task_group
        ref_task_group.add_object()
        return ref_task_group

    def get_task_group(self, task_group_name):
        if task_group_name not in self._task_groups:
            return None
        return self._task_groups[task_group_name]
    def list_task_group(self):
        return self._task_groups.keys()
    def count_task_group(self):
        return len(self._task_groups)


    def remove_task_group(self, task_group_name):
        ref_task_group = self.get_task_group(task_group_name)
        if ref_task_group is None:
            return 0

        if ref_task_group.count_task() > 0:
            return 1

        if ref_task_group.remove_object() != 0:
            return -1

        del self._task_groups[task_group_name]
        return 0

    #TODO: timer task
    def run_task(self, task_instance):
        """add a task into task-queue, starts the task with multiprocess if there is no conflict
        Args:
            task_instance: TaskInstance object
        Returns:
            return_code: 0, OK
                         1, confict with other task
                         2, get handler error
                         -1, other errors
            task_instance_id: task_group.task_name.create_timestamp, if conflict
                     None, otherwise
        """
        #get handler
        task_meta = task_instance.task.get_meta_all()
        if 'module_name' not in task_meta or 'handler_name' not in task_meta:
            logging.warn("handler info not found in task meta, run failed, task_name=[%s]", task_instance.task.id())
            return 2
        task_handler = self._get_handler_function(task_meta['module_name'], task_meta['handler_name'])
        if task_handler is None:
            logging.warn("get handler function failed, module_name=[%s], handler_name=[%s]", task_meta['module_name'], task_meta['handler_name'])
            return 2

        #TODO: confilt between groups
        self._queue_lock.acquire()
        logging.info("get queue_lock")
        task_group_id = task_instance.task_group_id()
        if task_group_id in self._task_queue:
                task_instance_id = self._task_queue[task_group_id][self.TASK_ITEM_IDX].id()
                self._queue_lock.release()
                logging.info("release queue_lock")
                logging.info("task in the same group is already inqueue, task_group=[%s], task_name=[%s], task_instance_id_inqueue=[%s]", task_group_id, task_instance.task.id(), task_instance_id)
                return 1 
        else:
            self._task_queue[task_group_id] = [task_instance, None]
        self._queue_lock.release()
        logging.info("release queue_lock")

        task_args = (task_instance, self._task_context)

        #thread-safe
        self._task_queue[task_group_id][self.TASK_EXECUTOR_IDX] = \
            multiprocessing.Process(target=task_handler, args=task_args)
        logging.info("task_instance_id=[%s], description=[start to run]", task_instance.id())
        self._task_queue[task_group_id][self.TASK_EXECUTOR_IDX].start()
        task_instance.set_meta_attr('start_timestamp', int(time.time()))
        return 0

    def check_task(self, sec_wait_for_term=5):
        """check running state of each task process,
        remove one which is finished or timeout
        """
        logging.info("check_task begin")
        curr_time = int(time.time())
        process_to_kill = []
        #lock free, will not conflict with add_task
        self._queue_lock.acquire()
        logging.info("get queue_lock")
        for key in self._task_queue.keys():
            p = self._task_queue[key][self.TASK_EXECUTOR_IDX]
            instance = self._task_queue[key][self.TASK_ITEM_IDX]
            instance_id = instance.id()
            start_timestamp = instance.get_meta_attr('start_timestamp')
            if start_timestamp is None:
                logging.error("task_instance_id=[%s], description=[no start_timestamp, marked as timeout]", instance_id)
                start_timestamp = 1
            #TODO: save exit code and end time on ZK
            if p.is_alive():
                #check timeout
                timeout_sec = 0
                if key in self._timeout_conf:
                    timeout_sec = self._timeout_conf[key]
                else:
                    timeout_sec = self._timeout_conf['default']

                #terminate and remove task which is timeout
                if start_timestamp > 0 and curr_time - start_timestamp >= timeout_sec:
                    p.terminate()
                    process_to_kill.append(p)
                    logging.warning("task_instance_id=[%s], description=[terminated by timeout]", instance_id)
                    instance.set_meta_attr('end_timestamp', int(time.time()))

                    del self._task_queue[key]
                    self.am_del_task_instance(instance.id())
                logging.info("task_instance_id=[%s], description=[still running normally]", instance_id)
            else:
                #remove task which is already done
                logging.info("task_instance_id=[%s], description=[terminated normally, remove from queue]", instance_id)
                instance.set_meta_attr('end_timestamp', int(time.time()))
                del self._task_queue[key]
                self.am_del_task_instance(instance.id())
        #end of foreach key in _task_queue
        self._queue_lock.release()
        logging.info("release queue_lock")
        #kill process if failing to terminate
        time.sleep(sec_wait_for_term)
        for p in process_to_kill:
            if p.is_alive():
                pid = p.pid
                if p is not None and p != -1:
                    logging.warning("pid=[%d], description=[timeout in termination, killed in force]", pid)
                    os.kill(pid, 9)

        return 0

    def stop_task(self, ref_task_instance, sec_wait_for_term=5):
        task_group_name, task_name, ctm = ref_task_instance.id().split('.')
        self._queue_lock.acquire()
        logging.info("get queue_lock")

        if task_group_name not in self._task_queue: 
            logging.info('task-instance is not running, task_group is not in task_queue, task_group_name=[%s], task_instance_id=[%s]', task_group_name, ref_task_instance.id())
            self._queue_lock.release()
            logging.info("release queue_lock")
            return 0

        p = self._task_queue[task_group_name][self.TASK_EXECUTOR_IDX]
        instance = self._task_queue[task_group_name][self.TASK_ITEM_IDX]

        if instance.id() != ref_task_instance.id():
            logging.info('task-instance is not running , running_task_instance_id=[%s], task_instance_id=[%s]', instance.id(), ref_task_instance.id())
            self._queue_lock.release()
            logging.info("release queue_lock")
            return 0

        instance.set_meta_attr('end_timestamp', int(time.time()))
        p.terminate()
        del self._task_queue[task_group_name]
        self.am_del_task_instance(instance.id())
        logging.warning("begin to terminate, task_instance_id=[%s]", ref_task_instance.id())

        time.sleep(1)
        if not p.is_alive():
            self._queue_lock.release()
            logging.info("release queue_lock")
            return 0

        time.sleep(sec_wait_for_term)
        if p.is_alive():
            pid = p.pid
            if p is not None and p != -1:
                logging.info("timeout in termination, killed in force, pid=[%d], task_instance_id=[%s]", pid, ref_task_instance.id())
                os.kill(pid, 9)

        self._queue_lock.release()
        logging.info("release queue_lock")
        return 0

    def run(self):
        """will block the control flow
           Returns:
               ret_code: 0, OK
                         1, Error, maybe already running in another thread
        """
        if self._is_running:
            return 1
        else: 
            self._is_running = True

        while(self._is_running):
            self.check_task()
            time.sleep(self._check_task_interval)


    def stop(self):
        """terminate all task and deallocate resource
        """
        self._is_running = False
        for key, tuple in self._task_queue.iteritems():
            p = tuple[self.TASK_EXECUTOR_IDX]
            instance = tuple[self.TASK_ITEM_IDX]
            if p.is_alive():
                p.terminate()
                logging.warning("task_instance_id=[%s], description=[terminated by stop]", instance.id())

    #AppMaster Interface
    def am_add_task_group(self, req):
        meta = {}
        if len(req.data) > 0:
            try:
                meta = json.loads(req.data)
            except:
                return 1, 'data is not in valid format of json, data=[%s]' % req.data
        if '.' in req.task_group_name:
            return 1, '"." is not allowed in task_group_name'
        ref_task_group = self.get_task_group(req.task_group_name)

        #already existed
        if ref_task_group is not None:
            logging.info("add task_group successfully, task_group_name=[%s]", req.task_group_name)
            return 0, ''

        #new
        ref_task_group = self.new_task_group(req.task_group_name, meta)
        if ref_task_group is None:
            return 1, 'fail to add task-group, it is already existed, task_group_name=[%s]' % req.task_group_name
        logging.info("add task_group successfully, task_group_name=[%s]", req.task_group_name)

        return 0, ''

    def am_del_task_group(self, req):
        ret = self.remove_task_group(req.task_group_name)
        if ret == 1:
            return 1, 'fail to delete task-group, not empty, task_group_name=[%s]' % \
                    req.task_group_name
        elif ret != 0:
            return 1, 'zookeeper fail'
        return 0, ''

    def am_set_task_group_meta(self, req):
        ref_task_group = self.get_task_group(req.task_group_name)
        if ref_task_group is None:
            return 1, 'fail to set meta, task-group is not existed, task_group_name=[%s]' % req.task_group_name

        value_meta = None
        if len(req.value) > 0:
            try:
                value_meta = json.loads(req.value)
            except:
                return 1, 'data is not in valid format of json, data=[%s]' % req.value
        else:
            return 1, "no meta value specified"

        if len(req.key) > 0:
            #add-meta-attr
            if req.add == True:
                ret = ref_task_group.add_meta_attr(req.key, value_meta)
                if ret != 0:
                    return 1, 'key is already existed, key=[%s], task_group_name=[%s]' % (req.key, req.task_group_name)
            #set-meta-attr
            else:
                ret = ref_task_group.set_meta_attr(req.key, value_meta)
                if ret != 0:
                    return 1, 'key is not existed, key=[%s], task_grouop_name=[%s]' % (req.key, req.task_group_name)
        else:
            #set-meta-all
            ret = ref_task_group.set_meta_all(value_meta)
            if ret != 0:
                return 1, 'fail to set-meta-all, task_group_name=[%s]' % (req.task_group_name)

        return 0, ''

    def am_get_task_group_meta(self, req):
        ref_task_group = self.get_task_group(req.task_group_name)
        if ref_task_group is None:
            return 1, 'fail to set meta, task-group is not existed, task_group_name=[%s]' % req.task_group_name

        value = None
        if len(req.key) > 0:
            value = ref_task_group.get_meta_attr(req.key)
            if value == None:
                return 1, 'key not found, key=[%s], task_group_name=[%s]' % (req.key, req.task_group_name)
        else:
            value = ref_task_group.get_meta_all()

        value_str = self._dump_meta(value, req.pretty)
        return 0, value_str


    def am_list_task(self, req):
        """
        Returns: (ret, err_msg, [task_list])
        """
        ref_task_group = self.get_task_group(req.task_group_name)
        if ref_task_group is None:
            return 1, 'fail to list task, task-group is not existed, task_group_name=[%s]' % req.task_group_name, []
        task_list = ref_task_group.list_task()
        return 0, '', task_list
       

    def am_add_task(self, req):
        if '.' in req.task_name:
            return 1, '"." is not allowed task_group_name'

        ref_task_group = self.get_task_group(req.task_group_name)
        if ref_task_group is None:
            return 1, 'fail to add task, task-group is not existed, task_group_name=[%s]' % req.task_group_name
        dop = None
        try:
            dop = json.loads(req.default_options)
        except Exception as e:
            return 1, 'faile to deserialize default_options, err=[%s]' % e
        if req.timer_sec == 0:
            ref_task = ref_task_group.new_task(req.task_name, req.module_name, req.handler_name, default_options=dop)
        else:
            ref_task = ref_task_group.new_task(req.task_name, req.module_name, req.handler_name, default_options=dop, timer=req.timer_sec)
        if ref_task is None:
            return 1, 'fail to add task, it is already existed, task_group_name=[%s], task_name=[%s]' % (req.task_group_name, req.task_name)

        return 0, ''

    def am_del_task(self, req):
        ref_task_group = self.get_task_group(req.task_group_name)
        if ref_task_group is None:
            return 1, 'fail to delete task, task-group is not existed, task_group_name=[%s]' % req.task_group_name
        ret = ref_task_group.remove_task(req.task_name)
        if ret == 1:
            return 1, 'fail to delete task, not empty, task_name=[%s]' % req.task_name
        elif ret != 0:
            return 1, 'zookeeper fail'
        return 0, ''

    def am_set_task_meta(self, req):
        ref_task_group = self.get_task_group(req.task_group_name)
        if ref_task_group is None:
            return 1, 'fail to set meta, task-group is not existed, task_group_name=[%s]' % req.task_group_name
        ref_task = ref_task_group.get_task(req.task_name)
        if ref_task is None:
            return 1, 'fail to set meta, task is not existed, task_name=[%s], task_group_name=[%s]' % (req.task_name, req.task_group_name)

        value_meta = None
        if len(req.value) > 0:
            try:
                value_meta = json.loads(req.value)
            except:
                return 1, 'data is not in valid format of json, data=[%s]' % req.value
        else:
            return 1, "no meta value specified"

        if len(req.key) > 0:
            #add-meta-attr
            if req.add == True:
                ret = ref_task.add_meta_attr(req.key, value_meta)
                if ret != 0:
                    return 1, 'key is already existed, key=[%s], task_name=[%s], task_group_name=[%s]' % (req.key, req.task_name, req.task_group_name)
            #set-meta-attr
            else:
                ret = ref_task.set_meta_attr(req.key, value_meta)
                if ret != 0:
                    return 1, 'key is not existed, key=[%s], task_name=[%s], task_group_name=[%s]' % (req.key, req.task_name, req.task_group_name)
        else:
            #set-meta-all
            ret, err_msg = self._check_task_meta(value_meta)
            if ret != 0:
                return ret, err_msg
            ret = ref_task.set_meta_all(value_meta)
            if ret != 0:
                return 1, 'fail to set-meta-all, task_name=[%s]' % (req.task_name)

        return 0, ''

    def _check_task_meta(self, meta):
        if 'module_name' not in meta:
            return 1, '"module_name" is required'
        if 'handler_name' not in meta:
            return 1, '"handler_name" is requried'
        if 'default_options' not in meta:
            return 1, '"default_options" is required'
        if type(meta['default_options']) is not dict:
            return 1, 'value of "default_options" should be dict'
        if 'timer' in meta: 
            try:
                t = int(meta['timer'])
            except Exception as e:
                return 1, 'value of "timer" should be int'
            meta['timer'] = t
        return 0, ''

    def am_get_task_meta(self, req):
        ref_task_group = self.get_task_group(req.task_group_name)
        if ref_task_group is None:
            return 1, 'fail to get meta, task-group is not existed, task_group_name=[%s]' % req.task_group_name
        ref_task = ref_task_group.get_task(req.task_name)
        if ref_task is None:
            return 1, 'fail to get meta, task is not existed, task_name=[%s], task_group_name=[%s]' % (req.task_name, req.task_group_name)

        value = None
        if len(req.key) > 0:
            value = ref_task.get_meta_attr(req.key)
            if value == None:
                return 1, 'key not found, key=[%s], task_name=[%s], task_group_name=[%s]' % (req.key, req.task_name, req.task_group_name)
        else:
            value = ref_task.get_meta_all()

        value_str = self._dump_meta(value, req.pretty)
        logging.info("get task meta successfully, task_group_name=[%s], task_name=[%s]", req.task_group_name, req.task_name)
        return 0, value_str

    def am_list_task_instance(self, req):
        ref_task_group = self.get_task_group(req.task_group_name)
        if ref_task_group is None:
            return 1, 'fail to list task-instance, task-group is not existed, task_group_name=[%s]' % req.task_group_name, []
        ref_task = ref_task_group.get_task(req.task_name)
        if ref_task is None:
            return 1, 'fail to list task-instance, task is not existed, task_name=[%s], task_group_name=[%s]' % (req.task_name, req.task_group_name), []
        logging.info("list task-instance successfully, task_name=[%s], task_group_name=[%s]", req.task_name, req.task_group_name)
        return 0, '', ref_task.list_task_instance()

      
    def am_get_task_instance_meta(self, req):
        task_group_name, task_name, create_time = req.task_instance_id.strip().split('.')
        ref_task_group = self.get_task_group(task_group_name)
        if ref_task_group is None:
            return 1, 'fail to get meta, task-group is not existed, task_group_name=[%s]' % task_group_name
        ref_task = ref_task_group.get_task(task_name)
        if ref_task is None:
            return 1, 'fail to get meta, task is not existed, task_name=[%s], task_group_name=[%s]' % (task_name, task_group_name)
        ref_inst = ref_task.get_task_instance(req.task_instance_id)
        if ref_inst is None:
            return 1, 'fail to get meta, task-instance is not existed, task_instance_id=[%s]' % (req.task_instance_id)

        value = None
        if len(req.key) > 0:
            value = ref_inst.get_meta_attr(req.key)
            if value == None:
                return 1, 'key not found, key=[%s], task_name=[%s], task_group_name=[%s]' % (req.key, req.task_name, req.task_group_name)
        else:
            value = ref_inst.get_meta_all()

        value_str = self._dump_meta(value, req.pretty)
        return 0, value_str

    def am_del_task_instance(self, task_instance_id):
        task_group_name, task_name, create_time = task_instance_id.strip().split('.')
        ref_task_group = self.get_task_group(task_group_name)
        if ref_task_group is None:
            return 0, ''
        ref_task = ref_task_group.get_task(task_name)
        if ref_task is None:
            return 0, ''

        if task_group_name in self._task_queue:
            instance = self._task_queue[task_group_name][self.TASK_ITEM_IDX]
            if instance.id() == task_instance_id:
                return 1, 'fail to delete task-instance, task-instance is still running, task_instance_id=[%s]' % task_instance_id
        ret = ref_task.remove_task_instance(task_instance_id)
        if ret == 0:
            return 0, ''
        else:
            return 1, 'fail to delete task-instance, internal error'


    def am_execute_task(self, req):
        """
        Returns: (0, task_instance_id) or (error_code, error_msg)
        """
        options = None
        if len(req.options) > 0:
            try:
                options = json.loads(req.options)
            except:
                return 1, 'options is not in valid format of json, data=[%s]' % req.options
            if type(options) is not dict:
                return 1, 'value of "options" should be dict'
        else:
            options = {}


        ref_task_group = self.get_task_group(req.task_group_name)
        if ref_task_group is None:
            return 1, 'fail to execute task, task-group is not existed, task_group_name=[%s]' % req.task_group_name
        ref_task = ref_task_group.get_task(req.task_name)
        if ref_task is None:
            return 1, 'fail to execute task, task is not existed, task_name=[%s], task_group_name=[%s]' % (req.task_name, req.task_group_name)

        ref_task_inst = ref_task.new_task_instance(options)
        ret = self.run_task(ref_task_inst)
        if ret == 1:
            return 1, 'fail to execute task, conflict with another running task in the same task-group. Try later'
        elif ret == 2:
            return 1, 'fail to execute task, load handler failed, please check config'
        elif ret < 0:
            return 1, 'fail to execute task, check log for more info'
        return 0,ref_task_inst.id()
            

    def am_stop_task(self, req):
        """
        Args:
            req (task_group_name, task_name)
        """
        if req.task_group_name in self._task_queue:
            instance = self._task_queue[req.task_group_name][self.TASK_ITEM_IDX]
            inst_id = instance.id()
            task_group_name, task_name, create_timestamp = inst_id.split('.')
            if task_name  == req.task_name:
                ret = self.stop_task(instance)
                return ret, ''
        return 0, ''
 
    def am_query_task(self, req):
        """
        Returns: (0, "{task_instance_id, meta_str}") or (error_code, error_msg)
        """
        if req.task_group_name in self._task_queue:
            instance = self._task_queue[req.task_group_name][self.TASK_ITEM_IDX]
            inst_id = instance.id()
            task_group_name, task_name, create_timestamp = inst_id.split('.')
            if task_name  == req.task_name:
                meta = instance.get_meta_all()
                if meta is None:
                    return 1, 'query task failed when getting meta, task_instance_id=[%s]' % inst_id
                inst_info = meta.copy()
                inst_info['task_instance_id'] = inst_id
                info_str = self._dump_meta(inst_info, req.pretty)
                logging.info('quey task successfully, task_instance_id=[%s]', inst_id)
                return 0, info_str
        return 1, 'task is not running'

    def am_task_send_signal(self, req):
        """
        Args:
            req (task_group_name, task_name, signal_name, data)
        Returns: 
            0, err_msg
        """
        if req.task_group_name in self._task_queue:
            instance = self._task_queue[req.task_group_name][self.TASK_ITEM_IDX]
            inst_id = instance.id()
            task_group_name, task_name, create_timestamp = inst_id.split('.')
            if task_name  == req.task_name:
                meta = None
                if len(req.data) > 0:
                    try:
                        meta = json.loads(req.data)
                    except Exception as e:
                        logging.warn('send signal failed, deserialize data failed, err=[%s], task_instance_id=[%s], signal_name=[%s]', e, inst_id, req.signal_name)
                        return 1, 'data is not in valid format of json, data=[%s]' % req.data
                instance.send_signal(req.signal_name, meta)
                return 0, ''
        logging.warn('send signal failed, task is not running, task_group_name=[%s], task_name=[%s], signal_name=[%s]', req.task_group_name, req.task_name, req.signal_name)
        return 1, 'task is not running'
 

    def _get_handler_function(self, module_name, func_name):
        fun = None
        try:
            m = importlib.import_module(module_name)
            fun = getattr(m, func_name)
        except Exception as e:
            logging.warn("get handler fucntion failed, module_name=[%s], function_name=[%s], err=[%s]", module_name, func_name, e)

        return fun

    def _dump_meta(self, meta, pretty):
        if pretty == True:
            return json.dumps(meta, sort_keys=True, indent=4, separators=(',',':'))
        return json.dumps(meta)


class TaskManagerThread(threading.Thread):
    """thread to run TaskManager
    """
    def __init__(self, thread_name, task_manager):
        super(TaskManagerThread, self).__init__(name=thread_name)
        self._task_manager = task_manager

    def run(self):
        logging.info("task manager thread running")
        ret = self._task_manager.Init()
        if ret != 0:
            logging.error("Init task manager failed")
            raise Exception("Init task manager failed")
        logging.info("task manager restored from ZK")
        self._task_manager.run()
        return 0
