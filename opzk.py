#! /usr/bin/env python
import shlex,subprocess,json,os,time
import Queue
import logging
import zk_cache
import threading

def ExecuteGetCommand(command_line):
    result=""
    args = shlex.split(command_line)
    print "exec:",command_line
    p=subprocess.Popen(args,stdout=subprocess.PIPE)
    if (p.wait()):
        return result
    else:
        result=p.stdout.read().decode('utf8')
        return result
		
def ExecuteSetCommand(command_line):
    res=os.system(command_line)
    return res

def ExecuteCmd(cmd_list, retry_times=1, retry_interval_sec=0):
    ret = 0
    output = None
    cmd = cmd_list

    i = 0
    while retry_times > 0:
        try:
            ret = 0
            output = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
            output = output.rstrip()
            break
        except subprocess.CalledProcessError, er:
            ret = er.returncode
            output = er.output
            i += 1
            retry_times-=1
            if retry_interval_sec > 0: time.sleep(retry_interval_sec*i)
        except Exception as ex:
            ret = 1
            output = ex.message
            i += 1
            retry_times -= 1
            if retry_interval_sec > 0: time.sleep(retry_interval_sec * i)

    return (ret, output)

class ZKCacheFiller(threading.Thread):
    def __init__(self, threadName, zkcli):
        super(ZKCacheFiller, self).__init__(name = threadName)
        self.zkcli = zkcli

    def run(self):
        logging.info("start to fill zk cache")
        node_que = Queue.Queue()
        node_que.put("")
        i = 0
        while not node_que.empty():
            cur_node = node_que.get()
            i+=1
            #traverse
            ret, node_list = self.zkcli.List(cur_node)
            if ret != 0:
                logging.error("%s: get children for node failed, node=[%s]", __name__, cur_node)
                return ret
            logging.info("%s: get children for node, node=[%s], num_children=[%s]", __name__, cur_node, len(node_list))

            for node in node_list:
                if cur_node == '':
                    node_que.put(node)
                else:
                    node_que.put(cur_node + '/' + node)
            #handle
            ret, meta = self.zkcli.Get(cur_node)
            if ret != 0:
                logging.warning("get meta all for node failed, set to {}, node=[%s]", cur_node)
                
            logging.info("get meta all for node, node=[%s]", cur_node)
        logging.info("finish to fill zk cache")


#any code should not use ZkcliHelper directly execpt service_manager, task_manager
class ZkcliHelper:
    def __init__(self, zkcli, zk_server, zk_root, zk_user, zk_pass):
        self.__zkcli = zkcli

        self.__zk_root = zk_root
        if not self.__zk_root.startswith("/"):
            self.__zk_root = "/%s" % self.__zk_root
        if self.__zk_root.endswith("/"):
            self.__zk_root = self.__zk_root[:-1]

        self.__zk_server = zk_server
        self.__zk_user = zk_user
        self.__zk_pass = zk_pass
        self.__server_argument = "--server=%s" % self.__zk_server
        self.__user_argument = "--user=%s" % self.__zk_user
        self.__pass_argument = "--pass=%s" % self.__zk_pass
        
        self.__zk_cache = zk_cache.ZKCache()


    def Init(self):
        #init cache
        t = ZKCacheFiller("zk_cache_filler", self)
        t.start()
        return 0

        """
        node_que = Queue.Queue()
        node_que.put("")
        i = 0
        while not node_que.empty():
            cur_node = node_que.get()
            i+=1
            #traverse
            ret, node_list = self.List(cur_node, False)
            if ret != 0:
                logging.error("%s: get children for node failed, node=[%s]", __name__, cur_node)
                return ret
            logging.info("%s: get children for node, node=[%s], num_children=[%s]", __name__, cur_node, len(node_list))

            for node in node_list:
                if cur_node == '':
                    node_que.put(node)
                else:
                    node_que.put(cur_node + '/' + node)
            #handle
            ret, meta = self.Get(cur_node, False)
            if ret != 0:
                logging.warning("get meta all for node failed, set to {}, node=[%s]", cur_node)
                
            logging.info("get meta all for node, node=[%s]", cur_node)

            ret = self.__zk_cache.Set(cur_node, meta)
            if ret != 0:
                logging.error("add node to cache failed, node=[%s]", cur_node)
                return ret
        logging.info("init cache successfully, num_node=[%d]", i)
        """
 
    def List(self, path, cache=True):
        if cache:
            ret, node_list = self.__zk_cache.List(path)
            if ret == zk_cache.ZKCache.OK:
                return ret, node_list

        cmd = ["node", "list", "--path=%s/%s" % (self.__zk_root, path)]
        ret, node_list_str = self.__execute_cmd(cmd)
        if ret != 0:
            logging.error("get children for node failed, node=[%s]", path)
            return ret, None
        logging.info("get children for node, node=[%s]", path)

        node_list_ori = node_list_str.strip().split('\n')
        node_list = []
        for line in node_list_ori:
            if "ZOO_" in line:
                #logging.warning("get error output, skip. line=[%s]", line)
                continue
            if len(line) == 0:
                continue
            node_list.append(line)
        ret = self.__zk_cache.AddChildren(path, node_list)
        return ret, node_list

    def Set(self, path, data):
        try:
            meta_str = json.dumps(data)
        except Exception, e:
            logging.warning("encode json failed for node, node=[%s], err=[%s]", path, e)
            return 1


        cmd = ["node", "set", "--path=%s/%s" % (self.__zk_root, path), "--value='%s'" % meta_str]
        ret, out = self.__execute_cmd(cmd)
        if ret != 0:
            logging.error("set node failed, node=[%s]", path)
            return ret
        ret = self.__zk_cache.Set(path, data)
        return ret

    def Get(self, path, cache=True):
        if cache:
            ret, meta = self.__zk_cache.Get(path)
            if ret == zk_cache.ZKCache.OK:
                return ret, meta

        cmd = ["node", "get", "--path=%s/%s" % (self.__zk_root, path)]
        ret, meta_str = self.__execute_cmd(cmd)
        if ret != 0:
            logging.error("get meta all for node failed, node=[%s]", path)
            return ret, None

        #filter out noise in output, kinda bug of zkcli
        str_line_ori = meta_str.strip().split('\n')
        str_line = []
        for line in str_line_ori:
            if "ZOO_" in line:
                #logging.warning("get error output, skip. line=[%s]", line)
                continue
            str_line.append(line)
        meta_str = '\n'.join(str_line)

        if meta_str.startswith("'"):
            meta_str = meta_str[1:]
        if meta_str.endswith("'"):
            meta_str = meta_str[:-1]
        logging.info("get meta all for node, node=[%s]", path)

        meta = None
        try:
            meta = json.loads(meta_str)
        except Exception, e:
            logging.warning("decode json failed for node, node=[%s], err=[%s]", path, e)
            return 1, None

        self.__zk_cache.Set(path, meta)
        return 0, meta

    def DelR(self, path):
        cmd = ["node", "del", "--path=%s/%s" % (self.__zk_root, path), "--recursive"]
        ret,out = self.__execute_cmd(cmd)
        if ret != 0:
            logging.error("del node recursively failed, node=[%s]", path)
            return ret

        ret = self.__zk_cache.Del(path)

        return ret

    def Del(self, path):
        cmd = ["node", "del", "--path=%s/%s" % (self.__zk_root, path)]
        ret,out = self.__execute_cmd(cmd)
        if ret != 0:
            logging.error("del node failed, node=[%s], ret=[%d], out=[%s]", path, ret, out)
            return ret

        ret = self.__zk_cache.Del(path)

        return ret

    def __execute_cmd(self, cmd_list, retry_times=2, retry_interval_sec=1):
        ret = 0
        output = None
        
        cmd=[self.__zkcli]
        cmd.extend(cmd_list)
        cmd.append(self.__server_argument)
        cmd.append(self.__user_argument)
        cmd.append(self.__pass_argument)

        while retry_times > 0:
            try:
                ret = 0
                output = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
                output = output.rstrip()
                break
            except subprocess.CalledProcessError, er:
                ret = er.returncode
                output = er.output
                retry_times-=1
                if retry_interval_sec > 0: time.sleep(retry_interval_sec)
            except Exception as ex:
                ret = 1
                output = ex.message
                retry_times -= 1
                if retry_interval_sec > 0: time.sleep(retry_interval_sec)

        logging.info("execute cmd finished, cmd=[%s], ret=[%d]", ' '.join(cmd), ret)
        if ret != 0:
            logging.warning("execute cmd failed, cmd=[%s], ret=[%d], output=[%s]", ' '.join(cmd), ret, output)
        return (ret, output)

class ZKObject(object):
    """common definition of attribute and functions of ZKNode
    """
    def __init__(self, id, meta, zk_path, zkcli, with_cache=True):
        self._id = id
        if meta is None:
            self._meta= dict()
        else:
            self._meta = meta
        self._zkcli = zkcli
        self._zk_path = zk_path
        self._with_cache = with_cache

    def id(self):
        return self._id
    def zk_path(self):
        return self._zk_path

    def get_meta_all(self):
        """should read only
        """
        if not self._with_cache:
            ret, meta = self._meta = self._zkcli.Get(self._zk_path, cache=False)
            if ret != 0:
                return None
            self._meta = meta
        return self._meta

    def get_meta_attr(self, key):
        """return value by key, subkey is specified by '/'
        """
        if key is None:
            return None

        if not self._with_cache:
            ret, meta = self._meta = self._zkcli.Get(self._zk_path, cache=False)
            if ret != 0:
                return None
            self._meta = meta

        keys = key.split('/')
        return self._get_meta_attr_list(keys)

    def add_object(self):
        return self._set_node()
    def remove_object(self):
        return self._zkcli.Del(self._zk_path)
    def remove_object_r(self):
        """remove recursively
        """
        return self._zkcli.DelR(self._zk_path)


    def set_meta_all(self, ex_meta):
        """will set to zk too
        """
        if ex_meta is None:
            ex_meta = {}
        if self._zk_path is None:
            return -1
        ret = self._zkcli.Set(self._zk_path, ex_meta)
        if ret != 0:
            return -1
        self._meta = ex_meta
        return 0

    def set_meta_attr(self, key, value):
        """will set to zk too
        """
        if self._zk_path is None:
            return -1
        keys = key.split('/')
        pre_keys = keys[:-1]
        last_key = keys[-1]

        pre_value = self._get_meta_attr_list(pre_keys)
        if type(pre_value) != dict or last_key not in pre_value:
            return -1
        pre_value[last_key] = value
        return self._zkcli.Set(self._zk_path, self._meta)

    def add_meta_attr(self, key, value):
        """will set to zk too
        """
        if self._zk_path is None:
            return -1
        keys = key.split('/')
        pre_keys = keys[:-1]
        last_key = keys[-1]

        pre_value = self._get_meta_attr_list(pre_keys)
        if type(pre_value) != dict or last_key in pre_value:
            return -1
        pre_value[last_key] = value
        return self._zkcli.Set(self._zk_path, self._meta)

    def del_meta_attr(self, key):
        if self._zk_path is None:
            return -1
        keys = key.split('/')
        pre_keys = keys[:-1]
        last_key = keys[-1]

        pre_value = self._get_meta_attr_list(pre_keys)
        if type(pre_value) != dict or last_key not in pre_value:
            return 0
        del pre_value[last_key]
        return self._zkcli.Set(self._zk_path, self._meta)

    def _get_meta_attr_list(self, key_list):
        value = self._meta
        for key in key_list:
            if type(value) != dict or key not in value:
                logging.info('key not found, key=[%s], value_type=[%s]', key, str(type(value)))
                return None
            else:
                value = value[key]
        return value

    def _set_node(self):
        return self._zkcli.Set(self._zk_path, self._meta)   

    def merge_dict(self, template, value_dict):
        for k,v in value_dict.iteritems():
#            logging.warning("merge_dict: curr_k=%s, curr_v=%s", k, str(v))
            if k not in template:
#                logging.warning("merge_dict: k not in template curr_k=%s", k)
                template[k] = v
            elif type(v) == dict:
#                logging.warning("merge_dict: merge recursively, curr_k=%s, template=%s", k, str(template))
                self.merge_dict(template[k], v)
            else:
                template[k] = v

