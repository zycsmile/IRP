#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" a manager to mantain state and store them into zk
"""

__author__ = 'guoshaodan01@baidu.com (Guo Shaodan)'

import os
import json
import subprocess
import copy
import logging
import am_global
import traceback
import opzk


class SpecContent:
    """
    """
    def __init__(self):
        """
        """
        self.version = None
        self.spec = {}


class ServiceManager:
    """
    """
    def __init__(self, zk_server, zk_root, zk_user="beehive", zk_pass="beehive_token"):
        """
        """
        self.zkcli = opzk.ZkcliHelper(am_global.zkcli_path, zk_server, zk_root, zk_user, zk_pass)
        #cache package_spec and data_spec, each service has one cache
        #{"service_id":{"package":{}, ""}}
        self.__spec_content_cache = {}

    def Init(self):
        ret = self.zkcli.Init()
        if ret != 0: return ret
        ret = self.zkcli.Set("service", "{}")
        return ret

    def AddService(self, id, meta={}):
        ret = self.zkcli.Set("service/%s" % id, meta)
        logging.info("add service, service_id=[%s], ret=[%d]", id, ret)
        #TODO: app_layer is to be compatible with zkmirror, remove later
        ret = self.zkcli.Set("service/%s/app" % id, {"app_layer":1})
        logging.info("add service app node, service_id=[%s], ret=[%d]", id, ret)
        ret = self.zkcli.Set("service/%s/app_group" % id, {})
        logging.info("add service app_group node, service_id=[%s], ret=[%d]", id, ret)
        ret = self.zkcli.Set("service/%s/config" % id, {})
        logging.info("add service config node, service_id=[%s], ret=[%d]", id, ret)
        ret = self.zkcli.Set("service/%s/config/task" % id, {})
        logging.info("add service config node, service_id=[%s], ret=[%d]", id, ret)
        ret = self.zkcli.Set("service/%s/config/event" % id, {})
        logging.info("add service config node, service_id=[%s], ret=[%d]", id, ret)

        #TODO: add config node for task and event
        return ret

    def DelService(self, id, force=False):
        if force:
            ret = self.zkcli.DelR("service/%s" % id)
            logging.info("del service recursively, service_id=[%s], ret=[%d]", id, ret);
        else:
            ret, app_list = self.zkcli.List("service/%s/app" % id)
            if ret == 0 and len(app_list) > 0:
                logging.warning("del service failed, not null, service_id=[%s]", id)
                return 1
        return 0

    def AddApp(self, id, service_id, meta={}):
        ret, out = self.zkcli.Get("service/%s" % service_id)       
        if ret != 0:
            self.AddService(service_id)
        ret = self.zkcli.Set("service/%s/app/%s" % (service_id,id), meta)
        logging.info("add app, app_id=[%s], ret=[%d]", id, ret)
        return ret

    def ListAppByService(self, service_id):
        ret, applist = self.zkcli.List("service/%s/app" % service_id)
        return ret, applist

    def ListAppByAppGroup(self, service_id, app_group_id):
        ret, meta = self.zkcli.Get("service/%s/app_group/%s" % (service_id, app_group_id))
        logging.info("get meta all for app_group, service_id=[%s], app_group_id=[%s], ret=[%d]", service_id, app_group_id, ret)
        if ret != 0:
            return ret, None
        return ret, meta["apps"]



    def AppSetMetaAll(self, id, service_id, meta):
        ret, out = self.zkcli.Get("service/%s/app/%s" % (service_id, id))
        if ret != 0:
            logging.warning("set meta all for app failed, not existed, serivce_id=[%s], app_id[%s], ret=[%d]", service_id, id, ret)
            return ret
        ret = self.zkcli.Set("service/%s/app/%s" % (service_id,id), meta)
        logging.info("set meta all for app, service_id=[%s], app_id=[%s], ret=[%d]", service_id, id, ret)
        return 0

    def AppGetMetaAll(self, id, service_id):
        ret, meta = self.zkcli.Get("service/%s/app/%s" % (service_id, id))
        logging.info("get meta all for app, service_id=[%s], app_id=[%s], ret=[%d]", service_id, id, ret)

        return ret, meta

    #key can be hierachy with '.' and value can be a dict or list
    def AppSetMetaAttr(self, id, service_id, key, value):
        """key is splited by '/'
        """
        ret, meta_all = self.AppGetMetaAll(id, service_id)
        if ret != 0:
            logging.warning("set meta attr failed, not existed, service_id=[%s], app_id=[%s], ret=[%d]", service_id, id, ret)
            return ret
        keys = key.split('/')
        i = 0

        tmpd = meta_all
        while i < len(keys)-1:
            k = keys[i]
            if k not in tmpd:
                logging.warning("set meta attr failed, key not found, service_id=[%s], app_id=[%s], key=[%s]", service_id, id, k)
                return 2
            else:
                tmpd = tmpd[k]
                i += 1
        tmpd[keys[-1]] = value

        ret = self.AppSetMetaAll(id, service_id, meta_all)
        return ret

    #key can be hierachy with '.' and value can be a dict or list
    def AppGetMetaAttr(self, id, service_id, key):
        ret, meta_all = self.AppGetMetaAll(id, service_id)
        if ret != 0:
            logging.warning("get meta attr failed, not existed, service_id=[%s], app_id=[%s], ret=[%d]", service_id, id, ret)
            return ret, None
        keys = key.split('/')
        i = 0

        tmpd = meta_all
        while i < len(keys):
            k = keys[i]
            if k not in tmpd:
                logging.warning("get meta attr failed, key not found, service_id=[%s], app_id=[%s], key=[%s]", service_id, id, k)
                return 2, None
            else:
                tmpd = tmpd[k]
                i += 1

        return 0, tmpd


        ret = self.AppSetMetaAll(id, service_id, meta_all)


    def DelApp(self, id, service_id):
        ret = self.zkcli.DelR("service/%s/app/%s" %(service_id, id))
        return ret

    def AppGetSpecAll(self, id, service_id):
        app_spec = {}
        ret, spec = self.AppGetSpec("resource", id, service_id)
        if ret != 0:
            logging.warning("get all spec failed when getting single spec, service_id=[%s], \
                    app_id=[%s], spec_type=[%s]", service_id, id, spec_type)
            return ret, None
        app_spec["resource"] = spec
        return 0, app_spec

    def AppGetSpec(self, spec_type, id, service_id):
        ret, spec = self.AppGetMetaAttr(id, service_id, spec_type)
        if ret != 0:
            logging.warning("get spec failed, no spec in meta, service_id=[%s], app_id=[%s], \
                    spec_type=[%s]", service_id, id, spec_type)
            return 1, None
        #if spec_type == "resource" and ("container_group_id" not in spec or spec["container_group_id"] == ""):
        #    spec["container_group_id"] = id

        return 0, spec

    def AddAppGroup(self, id, service_id, meta={}):
        ret, out = self.zkcli.Get("service/%s" % service_id)       
        if ret != 0:
            logging.warning("add app group failed, service not existed, serivce_id=[%s], app_group_id=[%s]", service_id, id)
            return ret
        if "apps" not in meta:
            meta["apps"] = []
        ret = self.zkcli.Set("service/%s/app_group/%s" % (service_id,id), meta)
        logging.info("add app, app_id=[%s], ret=[%d]", id, ret)
        return ret

    def DelAppGroup(self, id, service_id):
        ret = self.zkcli.DelR("service/%s/app_group/%s" %(service_id, id))
        return ret


    def AppGroupAddApp(self, id, service_id, app_id):
        ret, out = self.zkcli.Get("service/%s/app/%s" % (service_id, app_id))
        if ret != 0:
            logging.warning("add app to app group failed, app not existed, serivce_id=[%s], app_id[%s], ret=[%d]", service_id, app_id, ret)
            return ret

        ret, meta = self.zkcli.Get("service/%s/app_group/%s" % (service_id, id))
        logging.info("get meta all for app_group, service_id=[%s], app_group_id=[%s], ret=[%d]", service_id, id, ret)
        if ret != 0:
            return ret

        meta["apps"].append(app_id)

        ret = self.zkcli.Set("service/%s/app_group/%s" % (service_id,id), meta)
        logging.info("set meta all for app_group, service_id=[%s], app_group_id=[%s], ret=[%d]", service_id, id, ret)

        return ret


    def __get_remote_spec(self, spec_type, id, service_id):
        ret, spec_tmp = self.AppGetMetaAttr(id, service_id, "package")
        if ret != 0:
            logging.warning("no package attr in meta, service_id=[%s], app_id=[%s]", service_id, id)
            return ret, None

        if "data_source" not in spec_tmp or len(spec_tmp["data_source"]) == 0:
            logging.warning("no data_source attr in meta, service_id=[%s], app_id=[%s]", service_id, id)
            return 1, None

        key_module_name = "module_name" 
        if key_module_name not in spec_tmp or len(spec_tmp[key_module_name]) == 0:
            logging.warning("no module_name attr in meta, service_id=[%s], app_id=[%s]", service_id, id)
            return 1, None

     
        spec_path="%s/spec/%s" % (am_global.data_dir, spec_tmp[key_module_name])
        if spec_type == "dynamic_data":
            filestr = "data_spec.json"
        else:
            filestr = "%s_spec.json" % spec_type

        data_source=spec_tmp["data_source"]
        dirs = data_source.split('/')
        i = -1
        version_dir = dirs[i]
        while -i <= len(dirs) and version_dir == '':
            i -= 1
            version_dir = dirs[i]
        cut_dirs = data_source[data_source.find("//"):data_source.rfind(version_dir)].count('/') - 3
        local_spec_dir = spec_path + '/'+ version_dir +'/output/spec'
        local_spec_file = local_spec_dir+"/"+filestr
        #check existence
        #memory cache
        if service_id in self.__spec_content_cache and self.__spec_content_cache[service_id].version == version_dir and spec_type in self.__spec_content_cache[service_id].spec:
            logging.info("hit in spec cache, service_id=[%s], version=[%s], spec_type=[%s]", service_id, version_dir, spec_type)
            return 0, copy.deepcopy(self.__spec_content_cache[service_id].spec[spec_type])
        """
        if not service_id in self.__spec_content_cache:
            logging.info("service_id not hit")
        elif not self.__spec_content_cache[service_id].version == version_dir:
            logging.info("version not hit, [%s], [%s]", self.__spec_content_cache[service_id].version, version_dir)
        elif not spec_type in self.__spec_content_cache[service_id].spec:
            logging.info("spec_type not hit, [%s], [%s]", spec_type, ','.join(self.__spec_content_cache[service_id].spec))
        """

        #file cache
        exfile = os.path.exists(local_spec_file)
        logging.info("local_spec_file: [%s]", local_spec_file)
        if exfile:
            logging.info("hit in local file cache, service_id=[%s], version=[%s], spec_type=[%s], file=[%s]", service_id, version_dir, spec_type, local_spec_file)
        #mkdir
        if not exfile:
            cmd = "mkdir -p %s" % spec_path
            logging.info("mkdir -p %s", spec_path)
            res = os.system(cmd)
            #wget
            scm_addr=data_source + "/output/spec/" + filestr
            cmd = ["wget", "-q","-r","-nH", "--preserve-permissions", "--level=0", "--limit-rate=10000k", scm_addr, "-P", spec_path, "--cut-dirs=%d" % (cut_dirs)]
            logging.info("Popen: %s", ' '.join(cmd))
            ret, out = opzk.ExecuteCmd(cmd)
            #res = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            #ret = res.wait()
            if ret != 0:
                logging.warning("wget spec file failed, service_id=[%s], app_id=[%s]", service_id, id)
                return 1, None
        try:
            file_object = open(local_spec_file, 'r')
            try:
                all_the_text = file_object.read()
            finally:
                file_object.close()
        except:
            traceback.print_exc()
            logging.warning("open spec file failed, path=[%s], file=[%s], curr_dir=[%s]", local_spec_dir, filestr, os.getcwd())
            return 1, None
        spec=json.loads(all_the_text)

        #memory cache
        if service_id not in self.__spec_content_cache:
            self.__spec_content_cache[service_id] = SpecContent()
        cval = self.__spec_content_cache[service_id]
        cval.version = version_dir
        cval.spec[spec_type] = copy.deepcopy(spec)
        logging.info("put into spec cache, service_id=[%s], version=[%s], spec_type=[%s]", service_id, version_dir, spec_type)

        return 0, spec



    def __merge_dict(self, template, value_dict):
        for k,v in value_dict.iteritems():
#            logging.warning("__merge_dict: curr_k=%s, curr_v=%s", k, str(v))
            if k not in template:
#                logging.warning("__merge_dict: k not in template curr_k=%s", k)
                template[k] = v
            elif type(v) == dict:
#                logging.warning("__merge_dict: merge recursively, curr_k=%s, template=%s", k, str(template))
                self.__merge_dict(template[k], v)
            else:
                template[k] = v
#wangxuan03 add app type
    def AddAppType(self, type_id, service_id, meta=None):
        """add apptype node
        """
        if meta is None:
            meta = {}
        ret, out = self.zkcli.Get("service/%s" % service_id)       
        if ret != 0:
            self.AddService(service_id)
        ret, out = self.zkcli.Get("service/%s/app_type" % service_id)
        if ret != 0:
            ret = self.zkcli.Set("service/%s/app_type" % service_id, {})
        ret = self.zkcli.Set("service/%s/app_type/%s" % (service_id, type_id), meta)
        logging.info("add app_type, type_id=[%s], ret=[%d]", type_id, ret)
        return ret 

    def AppTypeSetMetaAll(self, type_id, service_id, meta):
        """set apptype meta
        """
        ret, out = self.zkcli.Get("service/%s/app_type/%s" % (service_id, type_id))
        if ret != 0:
            logging.warning("set meta all for app_type failed, not existed, serivce_id=[%s], \
                    type_id[%s], ret=[%d]", service_id, type_id, ret)
            return ret
        ret = self.zkcli.Set("service/%s/app_type/%s" % (service_id, type_id), meta)
        logging.info("set meta all for app_type, service_id=[%s], app_type=[%s], ret=[%d]", 
                service_id, type_id, ret)
        return 0

    def DelAppType(self, type_id, service_id):
        """del apptype node
        """
        ret = self.zkcli.DelR("service/%s/app_type/%s" % (service_id, type_id))
        return ret

    def AppTypeGetMetaAll(self, type_id, service_id):
        """get apptype meta
        """
        ret, meta = self.zkcli.Get("service/%s/app_type/%s" % (service_id, type_id))
        logging.info("get meta all for app_type, service_id=[%s], type_id=[%s], ret=[%d]", 
                service_id, type_id, ret)
        return ret, meta

    def AppTypeGetMetaAttr(self, id, service_id, key):
        """set apptype meta attr
        """
        ret, meta_all = self.AppTypeGetMetaAll(id, service_id)
        if ret != 0:
            logging.warning("get meta attr failed, not existed, service_id=[%s], \
                    app_id=[%s], ret=[%d]", service_id, id, ret)
            return ret, None
        keys = key.split('/')
        i = 0

        tmpd = meta_all
        while i < len(keys):
            k = keys[i]
            if k not in tmpd:
                logging.warning("get meta attr failed, key not found, service_id=[%s], \
                        type_id=[%s], key=[%s]", service_id, id, k)
                return 2, None
            else:
                tmpd = tmpd[k]
                i += 1

        return 0, tmpd
        ret = self.AppSetMetaAll(id, service_id, meta_all)

    def AppTypeSetMetaAttr(self, id, service_id, key, value):
        """key is splited by '/'
        """
        ret, meta_all = self.AppTypeGetMetaAll(id, service_id)
        if ret != 0:
            logging.warning("set meta attr failed, not existed, service_id=[%s], \
                    app_id=[%s], ret=[%d]", service_id, id, ret)
            return ret
        keys = key.split('/')
        i = 0

        tmpd = meta_all
        while i < len(keys)-1:
            k = keys[i]
            if k not in tmpd:
                logging.warning("set meta attr failed, key not found, service_id=[%s], \
                        app_id=[%s], key=[%s]", service_id, id, k)
                return 2
            else:
                tmpd = tmpd[k]
                i += 1
        tmpd[keys[-1]] = value

        ret = self.AppTypeSetMetaAll(id, service_id, meta_all)
        return ret

    def ListAppTypeByService(self, service_id):
        """
        list apptype in service
        """
        ret, apptypelist = self.zkcli.List("service/%s/app_type" % service_id)
        return ret, apptypelist

    
