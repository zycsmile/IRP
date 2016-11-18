#!/usr/bin/env python
#
import sys
import os
import logging
import traceback
import threading
import json

import sofa

import service_manager
import am_global
import am_conf
import am_task
import bhcli_helper
import bc_deploy_task
import check_state_task
import debug_task
import am_task_manager
import level_manager
import am_log as LOG

class Am:
    MODULE='amimp.ver_1_0_0'
    IMPLEMENTS=['appmaster.ver_1_0_0.Am']

    #def __init__(self):
    def __init__(self, conf):
        print "begin to init Am Service"
        sofa.use('appmaster.ver_1_0_0')

        ret = LOG.init_log(am_global.log_prefix)
        if ret != 0:
            print "InitLogger failed"
            raise Exception("InitLogger failed")

        print "init log successfully"

        try: 
            conf_files=[]
            for cf in am_global.conf_file:
                conf_files.append(os.path.join(am_global.conf_dir, cf))
            ret = am_conf.LoadConf(conf_files)
            if ret != 0:
                logging.error("LoadConf failed")
                raise Exception("LoadConf failed")
        except Exception,e:
            print e
            traceback.print_exc()
            raise Exception("LoadConf failed")
        print "load conf successfully"

        try:
            self.__service_mgr = service_manager.ServiceManager(am_conf.g_conf.zk_server, 
                    am_conf.g_conf.zk_root, am_conf.g_conf.zk_user, am_conf.g_conf.zk_pass)
        except Exception, e:
            print "init service_mananger failed"
            print e
            traceback.print_exc()
            raise Exception("init service_mananger failed")
        ret = self.__service_mgr.Init()
        if ret != 0:
            print "Init service manager from zookeeper failed"
            raise Exception("init service_mananger failed")
        print "init service_manager sucessfully"

        #TODO: move to upper level
        try:
            bh_mgr = bhcli_helper.BHManager(am_conf.g_conf.bh_loc, am_conf.g_conf.bh_cluster)
            logging.info("init bhcli manager successfully")
            self._task_context = am_task.TaskContext(self.__service_mgr, bh_mgr, 
                    am_conf.g_conf.matrix_cluster)
            logging.info("init task context successfully")
            self._task_manager = am_task_manager.TaskManager(am_conf.g_conf.task_timeout, 
                    am_conf.g_conf.zk_server, am_conf.g_conf.zk_root, 
                    am_conf.g_conf.zk_user, am_conf.g_conf.zk_pass, 
                    self._task_context, am_conf.g_conf.check_task_interval)
            logging.info("init task manager successfully")
            self._task_manager_thread = am_task_manager.TaskManagerThread("task_manager_thread", self._task_manager)
            self._task_manager_thread.start()
            logging.info("run task manager successfully")
            self._level_manager = level_manager.LevelManager(am_conf.g_conf.zk_server, 
                am_conf.g_conf.zk_root, am_conf.g_conf.zk_user, am_conf.g_conf.zk_pass, bh_mgr)
            ret = self._level_manager.Init()
            if ret != 0:
                logging.error("init level manager failed")
                raise Exception("init level manager failed")
            logging.info("run level manager successfully")

        except Exception, e:
            print "init global context failed"
            print e
            print traceback.print_exc()
            raise Exception("init global context failed")

    def query(self, key):
        logging.info("test query.")
        return (0,'')

    def AddService(self, service_id):
        logging.info("request received: AddApp, service_id=[%s]", service_id)
        ret = self.__service_mgr.AddService(service_id)
        err_msg = ''
        if ret != 0:
            err_msg = "add service failed, service_id=[%s]" % service_id
        return ret, err_msg

    def AddApp(self, req):
        logging.info("request received: AddApp, service_id=[%s], app_id=[%s], meta=[%s]", req.service_id, req.app_id, req.meta)
        ret=0
        err_msg=''

        if req.meta != None and len(req.meta) > 0:
            try:
                meta_dict = json.loads(req.meta)
            except:
                return 1, "meta is not in valid format of json"
            ret = self.__service_mgr.AddApp(req.app_id, req.service_id, meta_dict)
        else:
            ret = self.__service_mgr.AddApp(req.app_id, req.service_id)
        if ret != 0:
            err_msg = "add app to ZK failed, service_id=[%s], app_id=[%s]" % (req.service_id, req.app_id)
        return (ret,err_msg)

    def SetAppMeta(self, req):
        logging.info("request received: SepAppMeta, service_id=[%s], app_id=[%s]", req.service_id, req.app_id)
        ret = 0
        err_msg = ''
        try:
            value_dict = json.loads(req.value)
        except:
            return 1, "value is not in valid format of json"

        if req.key != None and len(req.key) > 0:
            ret = self.__service_mgr.AppSetMetaAttr(req.app_id, req.service_id, req.key, value_dict)
        else:
            ret = self.__service_mgr.AppSetMetaAll(req.app_id, req.service_id, value_dict)
 
        if ret != 0:
            err_msg = "set meta failed, service_id=[%s], app_id=[%s]" % (req.service_id, req.app_id)
        return ret, err_msg

    def _dump_meta(self, meta, pretty):
        if pretty == True:
            return json.dumps(meta, sort_keys=True, indent=4, separators=(',',':'))
        return json.dumps(meta)

    def GetAppMeta(self, req):
        logging.info("request received: GepAppMeta, service_id=[%s], app_id=[%s]", req.service_id, req.app_id)
        ret = 0
        err_msg = ''

        if req.key != None and len(req.key) > 0:
            ret, value = self.__service_mgr.AppGetMetaAttr(req.app_id, req.service_id, req.key)
        else:
            ret, value = self.__service_mgr.AppGetMetaAll(req.app_id, req.service_id)
        if ret != 0:
            return ret, ''
        value_str = self._dump_meta(value, req.pretty)
        return 0, value_str


    def DelApp(self, req):
        logging.info("request received: DelApp, service_id=[%s], app_id=[%s]", req.service_id, req.app_id)
        ret=0
        err_msg=''
        logging.info("start del, service_id=[%s], app_id=[%s]", req.service_id, req.app_id)
        ret = self.__service_mgr.DelApp(req.app_id, req.service_id)
        logging.info("end del, service_id=[%s], app_id=[%s], ret=[%d]", req.service_id, req.app_id, ret)
        if ret != 0:
            err_msg="delete app failed, service_id=[%s], app_id=[%s]" % (req.service_id, req.app_id)
        return (ret,err_msg)

    def ListApp(self, req):
        logging.info("request received: ListApp")
        res=appmaster.ver_1_0_0.AppListResponse()

        if req.service_id == None or len(req.service_id) == 0:
            res.err_msg = "service id is null"
            return 1, res
        if req.app_group_id != None and len(req.app_group_id) > 0:
            ret, applist = self.__service_mgr.ListAppByAppGroup(service_id=req.service_id, app_group_id=req.app_group_id)
        else:
            ret, applist = self.__service_mgr.ListAppByService(req.service_id)
        if ret != 0:
            res.err_msg = "list app failed"
            applist = []
        res.app_list = applist
        return ret, res
            
    def GetSpec(self, req):
        logging.info("request received: GetSpec, service_id=[%s], app_id=[%s], spec_type=[%s]", req.service_id, req.app_id, req.spec_type)
        ret=0
        res=appmaster.ver_1_0_0.AppGetSpecResponse()
        res.err_msg = ''
        if req.spec_type == None or req.spec_type == "" or req.spec_type == "all":
            ret, spec = self.__service_mgr.AppGetSpecAll(req.app_id, req.service_id)
        else:
            ret, spec = self.__service_mgr.AppGetSpec(req.spec_type, req.app_id, req.service_id)
        if ret != 0:
            res.err_msg = "get spec failed"
        else:
            res.spec = self._dump_meta(spec, req.pretty)

        return ret, res

    def AddAppGroup(self, req):
        print "here 1"
        try:
            logging.info("request received: AddAppGroup, service_id=[%s], app_group_id=[%s]", req.service_id, req.app_group_id)
            print "here 2"
            ret = self.__service_mgr.AddAppGroup(req.app_group_id, req.service_id)
            print "here 3"
            err_msg = ''
            if ret != 0:
                err_msg = "add app group failed, service_id=[%s], app_group_id=[%s]" % (req.service_id, req.app_group_id)
        except Exception, e:
            logging.warning("add group exception, err=[%s]", e)
            traceback.print_exc()
        except:
            logging.warning("add group exception")
            traceback.print_exc()
        return ret, err_msg

    def AppGroupAddApp(self, req):
        logging.info("request received: AppGroupAddApp, service_id=[%s], app_group_id=[%s], app_id=[%s]", req.service_id, req.app_group_id, req.app_id)
        ret=0
        err_msg=''

        ret = self.__service_mgr.AppGroupAddApp(req.app_group_id, req.service_id, req.app_id)
        if ret != 0:
            err_msg = "add app to app_group failed, service_id=[%s], app_group_id=[%s], app_id=[%s]" % (req.service_id, req.app_group_id, req.app_id)
        return (ret,err_msg)
    #wangxuan
    def AddAppType(self, req):
        """
        add apptype node
        """
        logging.info("request received: AddApp, service_id=[%s], type_id=[%s], meta=[%s]", 
                req.service_id, req.type_id, req.meta)
        ret = 0
        err_msg = ''

        if req.meta is not None and len(req.meta) > 0:
            try:
                meta_dict = json.loads(req.meta)
            except:
                return 1, "meta is not in valid format of json"
            ret = self.__service_mgr.AddAppType(req.type_id, req.service_id, meta_dict)
        else:
            ret = self.__service_mgr.AddAppType(req.type_id, req.service_id)
        if ret != 0:
            err_msg = "add app type to ZK failed, service_id=[%s], type_id=[%s]" % (
                    req.service_id, req.type_id)
        return (ret, err_msg)

    def DelAppType(self, req):
        """
        delete apptype node
        """
        logging.info("request received: DelApp, service_id=[%s], type_id=[%s]", req.service_id, 
                req.type_id)
        ret = 0
        err_msg = ''
        logging.info("start del, service_id=[%s], type_id=[%s]", req.service_id, req.type_id)
        ret = self.__service_mgr.DelAppType(req.type_id, req.service_id)
        logging.info("end del, service_id=[%s], type_id=[%s], ret=[%d]", req.service_id, 
                req.type_id, ret)
        if ret != 0:
            err_msg="delete app failed, service_id=[%s], type_id=[%s]" % (req.service_id, 
                    req.type_id)
        return (ret, err_msg)
        
    def ListAppType(self, req):
        """
        list apptype node
        """
        logging.info("request received: ListAppType")
        res=appmaster.ver_1_0_0.AppListTypeResponse()

        if req.service_id is None or len(req.service_id) == 0:
            res.err_msg = "service id is null"
            return 1, res
        ret, applist = self.__service_mgr.ListAppTypeByService(req.service_id)
        if ret != 0:
            res.err_msg = "list app failed"
            applist = []
        res.app_type_list = applist
        return ret, res

    def SetAppTypeMeta(self, req):
        """
        set apptype meta
        """
        logging.info("request received: SepAppTypeMeta, service_id=[%s], type_id=[%s]", 
                req.service_id, req.type_id)
        ret = 0
        err_msg = ''
        try:
            value_dict = json.loads(req.value)
        except:
            return 1, "value is not in valid format of json"

        if req.key is not None and len(req.key) > 0:
            ret = self.__service_mgr.AppTypeSetMetaAttr(req.type_id, req.service_id, req.key, 
                    value_dict)
        else:
            ret = self.__service_mgr.AppTypeSetMetaAll(req.type_id, req.service_id, value_dict)
 
        if ret != 0:
            err_msg = "set meta failed, service_id=[%s], type_id=[%s]" % (req.service_id, 
                    req.type_id)
        return ret, err_msg

    def GetAppTypeMeta(self, req):
        """
        get apptype meta
        """
        logging.info("request received: GepAppMeta, service_id=[%s], type_id=[%s]", 
                req.service_id, req.type_id)
        ret = 0
        err_msg = ''

        if req.key is not None and len(req.key) > 0:
            ret, value = self.__service_mgr.AppTypeGetMetaAttr(req.type_id, req.service_id, req.key)
        else:
            ret, value = self.__service_mgr.AppTypeGetMetaAll(req.type_id, req.service_id)
        if ret != 0:
            return ret, ''
        value_str = self._dump_meta(value, req.pretty)
        return 0, value_str

    #Task Interface
    #task-group
    def AddTaskGroup(self, req):
        logging.info("request received: AddTaskGroup, task_group_name=[%s]", req.task_group_name)
        return self._task_manager.am_add_task_group(req)
    def DelTaskGroup(self, req):
        logging.info("request received: DelTaskGroup, task_group_name=[%s]", req.task_group_name)
        return self._task_manager.am_del_task_group(req)
    def SetTaskGroupMeta(self, req):
        logging.info("request received: SetTaskGroupMeta, task_group_name=[%s]", req.task_group_name)
        return self._task_manager.am_set_task_group_meta(req)
    def GetTaskGroupMeta(self, req):
        logging.info("request received: GetTaskGroupMeta, task_group_name=[%s]", req.task_group_name)
        return self._task_manager.am_get_task_group_meta(req)
    def ListTaskGroup(self, req):
        logging.info("request received: ListTaskGroup")
        res=appmaster.ver_1_0_0.ListTaskGroupResponse()
        res.task_group_list = self._task_manager.list_task_group()
        res.err_msg = ''
        return 0, res

    #task
    def ListTask(self, req):
        logging.info("request received: ListTask, task_group_name=[%s]", req.task_group_name)
        res=appmaster.ver_1_0_0.ListTaskResponse()
        ret, res.err_msg, res.task_list = self._task_manager.am_list_task(req)
        return ret, res

    def AddTask(self, req):
        logging.info("request received: AddTask, task_name=[%s], task_group_name=[%s]", req.task_name, req.task_group_name)
        return self._task_manager.am_add_task(req)

    def DelTask(self, req):
        logging.info("request received: DelTask, task_name=[%s], task_group_name=[%s]", req.task_name, req.task_group_name)
        return self._task_manager.am_del_task(req)

    def SetTaskMeta(self, req):
        logging.info("request received: SetTaskMeta, task_name=[%s], task_group_name=[%s]", req.task_name, req.task_group_name)
        return self._task_manager.am_set_task_meta(req)

    def GetTaskMeta(self, req):
        logging.info("request received: GetTaskMeta, task_name=[%s], task_group_name=[%s]", req.task_name, req.task_group_name)
        return self._task_manager.am_get_task_meta(req)

    def ExecuteTask(self, req):
        logging.info("request received: ExecuteTask, task_name=[%s], task_group_name=[%s]", req.task_name, req.task_group_name)
        res=appmaster.ver_1_0_0.ExecuteTaskResponse()
        ret,out = self._task_manager.am_execute_task(req)
        if ret == 0:
            res.task_instance_id = out
            res.err_msg = ''
        else:
            res.task_instance_id = ''
            res.err_msg = out
        return ret, res

    def QueryTask(self, req):
        logging.info("request received: QueryTask, task_name=[%s], task_group_name=[%s]", req.task_name, req.task_group_name)
        res=appmaster.ver_1_0_0.QueryTaskResponse()
        ret, out1 = self._task_manager.am_query_task(req)
        if ret != 0:
            res.err_msg = out1
            res.task_info = ''
            return 1, res
        res.task_info = out1
        res.err_msg = ''
        return 0, res

    def StopTask(self, req):
        logging.info("request received: StopTask, task_name=[%s], task_group_name=[%s]", req.task_name, req.task_group_name)
        ret, err_msg = self._task_manager.am_stop_task(req)
        return ret, err_msg

    def TaskSendSignal(self, req):
        logging.info("request received: TaskSendSignal, task_name=[%s], task_group_name=[%s], signal_name=[%s]", req.task_name, req.task_group_name, req.signal_name)
        return self._task_manager.am_task_send_signal(req)


    #TaskInstance
    def ListTaskInstance(self, req):
        logging.info("request received: ListTaskInstance, task_name=[%s], task_group_name=[%s]", req.task_name, req.task_group_name)
        res=appmaster.ver_1_0_0.ListTaskInstanceResponse()
        ret, res.err_msg, res.task_instance_list = self._task_manager.am_list_task_instance(req)
 
        return ret, res
    def GetTaskInstanceMeta(self, req):
        logging.info("request received: GetTaskInstanceMeta, task_instance_id=[%s]", req.task_instance_id)
        return self._task_manager.am_get_task_instance_meta(req)
    def DelTaskInstance(self, req):
        logging.info("request received: DelTaskInstance, task_instance_id=[%s]", req.task_instance_id)
        return self._task_manager.am_del_task_instance(req.task_instance_id)


    #Level Interface
    def CreateLevel(self, req):
        logging.info("request received: CreateLevel, level_id=[%s]", req.level_id)
        return self._level_manager.am_create_level(req)

    def SetLevelMeta(self, req):
        logging.info("request received: SetlevelMeta, level_id=[%s]", req.level_id)
        return self._level_manager.am_set_level_meta(req)

    def GetLevelMeta(self, req):
        logging.info("request received: GetLevelMeta, level_id=[%s]", req.level_id)
        return self._level_manager.am_get_level_meta(req)

    def ReconfigLevelNaming(self, req):
        logging.info("request received: ReconfigLevelNaming, level_id=[%s]", req.level_id)
        return self._level_manager.am_reconfig_level_naming(req)

    def ListLevelAppInstance(self, req):
        logging.info("request received: ListLevelAppInstance, level_id=[%s]", req.level_id)
        res = appmaster.ver_1_0_0.ListLevelAppInstanceResponse()
        ret, err_msg, ailist = self._level_manager.am_list_level_app_instance(req)
        if ret == 0:
            res.app_instance_list = ailist
            res.err_msg = err_msg
        else:
            res.app_instance_list = []
            res.err_msg = err_msg
        return ret, res
        
    def CloseLevel(self, req):
        logging.info("request received: CloseLevel, level_id=[%s]", req.level_id)
        return self._level_manager.am_close_level(req)

    def DumpAppNaming(self, req):
        logging.info("request received: DumpAppNaming, app_id=[%s]", req.app_id)
        res = appmaster.ver_1_0_0.DumpAppNamingResponse()
        ret, res.err_msg, res.naming_spec, res.overlays = self._level_manager.am_dump_app_naming(req)
        return ret, res

