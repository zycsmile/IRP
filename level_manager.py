#!/usr/bin/env python
########################################################################
# 
# Copyright (c) 2014 Baidu.com, Inc. All Rights Reserved
# 
########################################################################
 
'''
File: level_manager.py
Author: guoshaodan01(guoshaodan01@baidu.com)
Date: 2014/04/18 10:14:10
'''
import logging
import sys
import json

import opzk
import bhcli_helper
import am_global



class Level(opzk.ZKObject):
    """Level
    """
    def __init__(self, level_id, zkcli, bhcli, level_spec=None, type=None, level_define=None, meta=None):
        """
        meta of level:
        "type", indicates for what purpose the level is created, such as "naming", ""
        "level_spec", the spec of leve, specify how to create it 
        "level_define", defination of level,app_id:ns_lock_num for naming type
                                            app_id:tag for other type
        """
        super(Level, self).__init__(id=level_id, meta=meta, zk_path="level/%s" % level_id, zkcli=zkcli)

        if type is not None:
            self._meta['type'] = type
        if level_spec is not None:
            self._meta['level_spec'] = level_spec

        self._bhcli = bhcli

    def create(self):
        """select app instance and fill the level_define
        """
        if "level_define" in self._meta and len(self._meta["level_define"]) > 0:
            logging.warn("level is already created, level_id=[%s]", self._id)
            return 1

        if self._meta['type'] == "naming":
            level_spec = self._meta["level_spec"]
            if len(level_spec["level_items"]) != 1:
                logging.warn("for naming type of level, number of level items should be 1, but [%d] found", len(level_spec["level_items"]))
                return 1

            app_id = level_spec["level_items"][0]["app_id"]
            amount = level_spec["level_items"][0]["amount"]

            desc = bhcli_helper.describe_app(self._bhcli, app_id)
            if desc is None:
                logging.warn("describe app failed, cannot create level, app_id=[%s], level_id=[%s]", app_id, self._id)
                return 1
            num_amount = self._parse_amount(len(desc["app_instance"]), amount)


            level_def = []
            def_item = []
            def_item.append(app_id)
            def_item.append(num_amount)
            level_def.append(def_item)

            if "level_define" not in self._meta:
                self.add_meta_attr("level_define", level_def)
            else:
                self.set_meta_attr("level_define", level_def)
        else:
            logging.warn("[%s] type of level is not implemented", self._meta["type"])
            return -1
        return 0

    def _parse_amount(self, sum, amount):
        num = 0
        if amount[-1] == '%':
            quant = float(amount[:-1])
            if quant >= 100:
                num = sum
            else:
                num = int(sum * quant / 100)
        else:
            quant = int(amount)
            if quant > sum:
                num = sum
            else:
                num = quant
        return num

    def get_level_define(self):
        return self.get_meta_attr("level_define")

    def reconfig_naming(self, version, naming_spec=None, overlays=None):
        """Reconfig naming
        Returns: 
            ret, err_msg
        """
        app_id, ns_lock = self._meta['level_define'][0]

        bh_app = self._bhcli.get_app(app_id)

        #set ns_lock to 0
        ret, out = bh_app.set_ns_lock(0)
        if ret != 0:
            logging.warn("set ns lock to zero failed, app_id=[%s], level_id=[%s]", app_id, self._id)
            return 1, 'set ns lock to zero failed'

        logging.info("set ns lock to zero successfully, app_id=[%s], level_id=[%s]", app_id, self._id)
        #reconfig naming
        if naming_spec is not None:
            ret, out = bh_app.reconfig_naming(naming_spec)
            if ret != 0:
                logging.warn("reconfig naming of app failed, app_id=[%s], level_id=[%s]", app_id, self._id)
                return 1, 'reconfig naming of app failed'
            logging.info("naming_spec is user-specified, reconfig naming of app successfully, app_id=[%s], level_id=[%s]", app_id, self._id)

        #set overlays
        app_id_list = []
        desc = bhcli_helper.describe_app(self._bhcli, app_id)
        naming_spec = desc['app_spec']['naming']
        for item in naming_spec['dependency']:
            app_id_list.append(item['app_id'])
        ret, err_msg = self._bhcli.set_ns_overlay(app_id_list, version, overlays)
        if ret != 0:
            logging.warn("set ns overlays of dependecy failed, app_id=[%s], level_id=[%s]", app_id, self._id)
            return 1, err_msg
        logging.info("set ns overlays of dependecy successfully, app_id=[%s], level_id=[%s]", app_id, self._id)


        #set ns_lock as to level_define
        ret, out = bh_app.set_ns_lock(ns_lock)
        if ret != 0:
            logging.warn("set ns lock failed, ns_lock=[%d], app_id=[%s], level_id=[%s]", ns_lock, app_id, self._id)
            return 1, 'set ns lock to [%d] failed' % ns_lock
        logging.info("set ns lock successfully, ns_lock=[%d], app_id=[%s], level_id=[%s]", ns_lock, app_id, self._id)

        if "naming_version" not in self._meta:
            self.add_meta_attr("naming_version", version)
        else:
            self.set_meta_attr("naming_version", version)
        return 0, ''

    def list_app_instance(self, naming_version=None):
        """
        Returns:
            ret, err_msg, app_instance_list
        """
        level_type = self._meta['type']
        if level_type == 'naming':
            if naming_version is None or len(naming_version) == 0:
                if 'naming_version' not in self._meta:
                    err_msg = 'cannot list level of naming type before reconfig_naming'
                    logging.warn('%s, level_id=[%s]', err_msg, self._id)
                    return 1, err_msg, None
                naming_version = self._meta['naming_version']

            app_id, ns_lock = self._meta['level_define'][0]
            desc = bhcli_helper.describe_app(self._bhcli, app_id)
            app_instance_list = []
            for app_instance in desc['app_instance']:
                app_inst_id = app_instance['app_instance_id']
                hostname = app_instance['container_instance']['hostname']
                for item in app_instance['naming']['dependency']:
                    if "version" in item and item['version'] == naming_version:
                        app_instance_list.append(app_inst_id+' '+hostname)
                        break
            return 0, '', app_instance_list
        else:
            return 1, 'type [%s] is not surpported' % level_type, None


class LevelManager(object):
    """operator of level
    """
    def __init__(self, zk_server, zk_root, zk_user, zk_pass, bhcli):
        """
        """
        self._bhcli = bhcli
        self._zkcli = opzk.ZkcliHelper(am_global.zkcli_path, zk_server, zk_root, zk_user, zk_pass)
        #don't Init zkcli, do not need to fill cach in zkcli

        self._levels = {}

    def Init(self):
        """restore level from zk
        """
        ret, tmp_meta = self._meta = self._zkcli.Get('level', cache=False)
        if ret != 0:
            ret = self._meta = self._zkcli.Set('level', {})
            if ret != 0:
                logging.error('create zk_node failed, node=[level]')
                return ret


        ret, level_list = self._zkcli.List('level')
        if ret != 0:
            logging.error('get level list failed, node=[level]')
            return ret
        logging.info("get level list, level_list=[%s], ret=[%d]", ','.join(level_list), ret)

        for level_id in level_list:
            ret, level_meta = self._zkcli.Get('level/%s' % level_id)
            if ret != 0:
                logging.error("get meta of level failed, level_id=[%s]", level_id)
                return ret

            ref_level = Level(level_id, self._zkcli, self._bhcli, level_spec=None, type=None, meta=level_meta)
            self._levels[level_id] = ref_level
 
        return 0

    def new_level(self, level_id, level_spec, type, meta=None):
        if level_id in self._levels:
            return None

        ref_level = Level(level_id, self._zkcli, self._bhcli, level_spec, type, meta=meta)
        self._levels[level_id] = ref_level
        ref_level.add_object()
        return ref_level

    def get_level(self, level_id):
        if level_id not in self._levels:
            return None
        return self._levels[level_id]

    def list_level(self):
        return self._levels.keys()

    def count_level(self):
        return len(self._levels)

    #TODO: handle the tag if not in type of naming
    def remove_level(self, level_id):
        ref_level = self.get_level(level_id)
        if ref_level is None:
            return 0

        if ref_level.count_task() > 0:
            return 1

        if ref_level.remove_object() != 0:
            return -1

        del self._levels[level_id]
        return 0

    def _check_level_spec(self, level_spec):
        if "service_id" not in level_spec:
            return 1, "invalid level_spec, service_id is not found"
        if type(level_spec["service_id"]) is not str and type(level_spec["service_id"]) is not unicode:
            return 1, "invalid level_spec, service_id should be in type of str, but [%s]" % type(level_spec["service_id"])
        if "level_items" not in level_spec:
            return 1, "invalid level_spec, level_items in not found"
        items = level_spec["level_items"]
        if type(items) is not list:
            return 1, "invalid level_spec, level_items should be in type of list, but [%s]" % type(items)
        for item in items:
            if type(item) is not dict:
                return 1, "invalid level_spec, item of level_items should be in type of dict, but [%s]" % type(item)
            if "app_id" not in item:
                return 1, "invalid level_spec, app_id not found in LevelItem"
            if type(item["app_id"]) is not str and type(item["app_id"]) is not unicode:
                return 1, "invalid level_spec, app_id should be in type of str in LevelItem, but [%s]" % type(item["app_id"])
            if "amount" not in item:
                return 1, "invalid level_spec, amount not found in LevelItem"
            if type(item["amount"]) is not str and type(item["amount"]) is not unicode:
                return 1, "invalid level_spec, amount should be in type of str in LevelItem, but [%s]" % type(item["amount"])
            amount = item["amount"]
            if amount[-1] == '%':
                try:
                    quant = float(amount[:-1])
                except Exception as e:
                    return 1, "invalid level_spec, amount format error"
            else:
                try:
                    quant = int(amount)
                except Exception as e:
                    return 1, "invalid level_spec, amount format error"

        return 0,''


    #app master Interface
    def am_create_level(self, req):
        """create level
        Args:
            req: level_id, spec_json, type
        Returns:
            ret, err_msg
        """
        level_spec = None
        try:
            level_spec = json.loads(req.spec_json)
        except Exception as e:
            return 1, "level json deserializing fail"

        ret, err = self._check_level_spec(level_spec)
        if ret != 0:
            return ret, err

        if req.type == "naming":
            if len(level_spec["level_items"]) != 1:
                return 1, "for naming, number of level_items should be 1"
            ref_level = self.new_level(req.level_id, level_spec, req.type)
            if ref_level is None:
                return 1, "level is existed, level_id=[%s]" % req.level_id
            ret = ref_level.create()
            if ret != 0:
                return 1, "app in level_items maybe error, check more in log"
        else:
            return 1, "type [%s] is not surpported" % req.type
        return 0, ''

    def am_set_level_meta(self, req):
        """set level meta
        Args:
            req: level_id, key, value, add
        Returns:
            ret, err_msg
        """
        ref_level = self.get_level(req.level_id)
        if ref_level is None:
            logging.warn("set level meta failed, level not existed, level_id=[%s]", req.level_id)
            return 1, 'level not existed, level_id=[%s]' % req.level_id

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
                ret = ref_level.add_meta_attr(req.key, value_meta)
                if ret != 0:
                    return 1, 'key is existed, key=[%s], level_id=[%s]' % (req.key, req.level_id)
            #set-meta-attr
            else:
                ret = ref_level.set_meta_attr(req.key, value_meta)
                if ret != 0:
                    return 1, 'key not found, key=[%s], level_id=[%s]' % (req.key, req.level_id)
        else:
            #set-meta-all
            ret = ref_level.set_meta_all(value_meta)
            if ret != 0:
                return 1, 'fail to set-meta-all, level_id=[%s]' % (req.level_id)

        return 0, ''

    def am_get_level_meta(self, req):
        """get level meta
        Args:
            req: level_id, key, pretty
        Returns:
            ret, err_msg
        """
        ref_level = self.get_level(req.level_id)
        if ref_level is None:
            logging.warn("get level meta failed, level not existed, level_id=[%s]", req.level_id)
            return 1, 'level not existed, level_id=[%s]' % req.level_id

        value = None
        if len(req.key) > 0:
            value = ref_level.get_meta_attr(req.key)
            if value == None:
                return 1, 'key not found, key=[%s], level_id=[%s]' % (req.key, req.level_id)
        else:
            value = ref_level.get_meta_all()

        value_str = self._dump_meta(value, req.pretty)
        logging.info("get level meta successfully, level_id=[%s]", req.level_id)
        return 0, value_str

    def am_dump_app_naming(self, req):
        """dump naming spec and overlays of dependencies which can be used to rollback connection
        Args:
            req: app_id
        Returns:
            ret, err_msg, naming_spec_str, overlays_str
        """
        desc = bhcli_helper.describe_app(self._bhcli, req.app_id)
        if desc is None:
            logging.warn("describe app failed, cannot dump naming, app_id=[%s]", req.app_id)
            return 1, 'dump failed, app is not existed', '', ''
        naming_spec = desc['app_spec']['naming']
        app_id_list = []
        for item in naming_spec['dependency']:
            app_id_list.append(item['app_id'])
        overlays = self._bhcli.dump_ns_overlay(app_id_list)
        if overlays is None:
            logging.warn("dump_ns_overlay of dependency failed, app_id=[%s]", req.app_id)
            return 1, 'dump failed, dump ns overlay of dependency failed', '', ''
        naming_spec_str = self._dump_json(naming_spec, req.pretty)
        overlay_str = self._dump_json(overlays, req.pretty)
        return 0, '', naming_spec_str, overlay_str

    def am_reconfig_level_naming(self, req):
        """reconfig naming
        Args:
            req: level_id, version, naming_spec(optional), overlays(optional)
        Returns
            ret, err_msg
        """
        ref_level = self.get_level(req.level_id)
        if ref_level is None:
            logging.warn("reconfig level failed, level is not existed, type=[naming], level_id=[%s]", req.level_id)
            return 1, 'level is not existed, level_id=[%s]' % req.level_id
        ret, naming_spec = self._load_json(req.naming_spec)
        if ret != 0:
            return ret, "load naming_spec failed, check its format"
        ret, overlays = self._load_json(req.overlays)
        if ret != 0:
            return ret, "load overlays failed, check its format"

        ret, err_msg = ref_level.reconfig_naming(req.overlay_version, naming_spec, overlays)
        return ret, err_msg

     
    def _dump_json(self, meta, pretty):
        if pretty == True:
            return json.dumps(meta, sort_keys=True, indent=4, separators=(',',':'))
        return json.dumps(meta)

    def _load_json(self, json_str):
        if json_str is None or len(json_str) == 0:
            return 0, None
        try:
            json_dict = json.loads(json_str)
        except Exception as e:
            logging.warn('load json failed, err=[%s]', e)
            return 1, None
        return 0, json_dict

    def am_list_level_app_instance(self, req):
        """ list app instance of level
        Args:
            req: level_id
        Return:
            ret, err_msg, id_list
        """
        ref_level = self.get_level(req.level_id)
        if ref_level is None:
            logging.warn("list app instance of level failed, level is not existed, type=[naming], level_id=[%s]", req.level_id)
            return 1, 'level is not existed, level_id=[%s]' % req.level_id, None

        return ref_level.list_app_instance(req.naming_version)

    def am_close_level(self, req):
        """ close level, just remove, TODO: how to handle tag for other type
        Args:
            req: level_id
        Return:
            ret, err_msg
        """
        ref_level = self.get_level(req.level_id)
        if ref_level is None:
            logging.info("delete return ok, level is not existed, level_id=[%s]", req.level_id)
            return 1, 'level is not existed, level_id=[%s]' % req.level_id, None

        ret = ref_level.remove_object_r()
        if ret == 0:
            del self._levels[req.level_id]
            return 0, ''
        else:
            return 1, 'delete from zookeeper failed'

    def _dump_meta(self, meta, pretty):
        if pretty == True:
            return json.dumps(meta, sort_keys=True, indent=4, separators=(',',':'))
        return json.dumps(meta)

