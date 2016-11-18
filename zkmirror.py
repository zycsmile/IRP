#! /usr/bin/env python
#coding=gbk
#
import shlex,subprocess,json,traceback,os
import opzk
import sys
import logging

reload(sys)
sys.setdefaultencoding('utf-8')

class ZkMirror:
    __root_tree={}
    __root_path=''
    __server=''
   
    def test_1(self):
        print "123456"

    def __init__(self,root_path,server):
        print "start zkmirror."
        self.__root_path=root_path
        self.__server=server
    
    def __CreateRoot(self):
        try:
            command_line="./zkcli node list --path='"+self.__root_path+"/service' --server='"+self.__server+"'"
            #TODO: add service if not existed
            result=opzk.ExecuteGetCommand(command_line)
            list=result.split('\n')
            self.__root_tree["service"]={}
            i=0
            while(i<len(list)):
                if (0==len(list[i])):
                    i+=1
                    continue
                self.__root_tree["service"][list[i]]={}
                i+=1
        except:
            logging.warning("zkcli list serivce failed")
            traceback.print_exc()
            return -1
        return 0

    def __GetErrDict(self):
        err={}
        err["flag"]="err"
        return err

    def __IsErrDict(self,dict):
        if 'flag' not in dict.keys():
            return 0
        if (0!=cmp(dict["flag"],"err")):
            return 0
        return 1

    def __CreateNextLayer(self,node_path,layer):
        next_layer={}
        try:
            #get node meta info
            command_line="./zkcli node get --path='"+node_path+"' --server='"+self.__server+"'"
            result=opzk.ExecuteGetCommand(command_line)
            if (1==len(result)):
                next_layer["Meta"]={}
            else:
                next_layer["Meta"]=json.loads(result)
            command_line="./zkcli node list --path='"+node_path+"' --server='"+self.__server+"'"
            result=opzk.ExecuteGetCommand(command_line)
            list=result.split('\n')
            if (layer<=1):
                next_layer["children_count"]=0
                next_layer["children"]={}
                return next_layer
            next_layer["children_count"]=0
            i=0
            next_layer["children"]={}
            #层数不能小于实际节点深度,否则返回错误dict
            if (0==len(list)):
                print "children count can not be 0. layer:", layer
                next_layer=self._GetErrDict()
                return next_layer
            while(i<len(list)):
                if (0==len(list[i])):
                    i+=1
                    continue
                next_layer["children_count"]+=1
                next_node_path=node_path+"/"+list[i].encode()
                temp_layer=self.__CreateNextLayer(next_node_path,layer-1)
                if (self.__IsErrDict(temp_layer)):
                    return temp_layer
                next_layer["children"][list[i]]=temp_layer
                temp_layer["parent"]=next_layer
                i+=1
        except:
            traceback.print_exc()
            next_layer=self.__GetErrDict()
            return next_layer
        return next_layer

    #创建一个服务的镜像树
    def __CreateOneServiceApp(self,svc):
        app_root=self.__root_tree["service"][svc]["app"]
        try:
            #获取app的meta信息
            command_line="./zkcli node get --path='"+self.__root_path+"/service/"+svc.encode()+"/app' --server='"+self.__server+"'"
            result=opzk.ExecuteGetCommand(command_line)
            logging.info("get service meta, path=[/service/%s/app], meta=[%s]", svc.encode(), result)
            if (1==len(result)):
                meta={}
            else:
                meta=json.loads(result)
            app_root["Meta"]=meta
            #如果app meta中不存在层数,说明meta信息错误返回失败
            if "app_layer" not in meta.keys():
                logging.error("no app_layer in meta of approot, service=[%s]", svc)
                return -1
            layer=meta["app_layer"]
            #获取app的子节点信息
            command_line="./zkcli node list --path='"+self.__root_path+"/service/"+svc.encode()+"/app' --server='"+self.__server+"'"
            result=opzk.ExecuteGetCommand(command_line)
            list=result.split('\n')
            logging.info("get app list, father_path=[/service/%s/app], num_children=[%d]", svc.encode(), len(list))
            app_root["children"]={}
            app_root["children_count"]=0
            i=0
            while(i<len(list)):
                if (0==len(list[i])):
                    i+=1
                    continue;
                app_root["children_count"]+=1
                next_node_path=self.__root_path+"/service/"+svc.encode()+"/app/"+list[i].encode()
                next_layer=self.__CreateNextLayer(next_node_path,layer)
                if (self.__IsErrDict(next_layer)):
                    print "get next layer fail,order:", i
                    return -1
                app_root["children"][list[i]]=next_layer
                next_layer['parent']=app_root
                i+=1
        except:
            traceback.print_exc()
            return -1
        return 0

    def __CreateOneServiceConfig(self,svc):
        config_root=self.__root_tree["service"][svc]["config"]
        try:
            #获取event信息
            config_root["event"]={}
            config_root["task"]={}
            command_line="./zkcli node list --path='"+self.__root_path+"/service/"+svc.encode()+"/config/event' --server='"+self.__server+"'"
            result=opzk.ExecuteGetCommand(command_line)
            list=result.split('\n')
            i=0
            while(i<len(list)):
                config_root["event"][list[i]]={}
                command_line="./zkcli node get --path='"+self.__root_path+"/service/"+svc.encode()+"/config/event/"+list[i].encode()+"' --server='"+self.__server+"'"
                result=opzk.ExecuteGetCommand(command_line)
                if (1==len(result)):
                    config_root["event"][list[i]]["Meta"]={}
                else:
                    config_root["event"][list[i]]["Meta"]=json.loads(result)
                i+=1
            #获取task信息
            command_line="./zkcli node list --path='"+self.__root_path+"/service/"+svc.encode()+"/config/task' --server='"+self.__server+"'"
            result=opzk.ExecuteGetCommand(command_line)
            list=result.split('\n')
            j=0
            while(j<len(list)):
                config_root["task"][list[j]]={}
                command_line="./zkcli node get --path='"+self.__root_path+"/service/"+svc.encode()+"/config/task/"+list[j].encode()+"' --server='"+self.__server+"'"
                result=opzk.ExecuteGetCommand(command_line)
                if (1==len(result)):
                    config_root["task"][list[j]]["Meta"]={}
                else:
                    config_root["task"][list[j]]["Meta"]=json.loads(result)
                j+=1
        except:
            traceback.print_exc()
            return -1
        return 0
    
    def __CreateOneService(self,svc):
        service_root=self.__root_tree["service"][svc]
        service_root["app"]={}
        service_root["config"]={}
        ret=self.__CreateOneServiceApp(svc)
        if(0!=ret):
            return ret
        self.__CreateOneServiceConfig(svc)
        if(0!=ret):
            return ret
        return 0
        
    def CreateTree(self):
        try:
            logging.info("start create tree")
            ret=self.__CreateRoot()
            if(0!=ret):
                logging.error("create root failed")
                return ret
            service=self.__root_tree["service"]
            for eachkey in service.iterkeys():
                ret=self.__CreateOneService(eachkey)
                if (0!=ret):
                    logging.error("create service failed, service=[%s]", eachkey)
                    return ret
        except:
            traceback.print_exc()
            return -1
        return 0

    def __GetErrList(self):
        list=["ErrList"]
        return list

    def __IsErrList(self,list):
        if (0!=cmp(list[0], "ErrList")):
            return 0
        return 1

    def __GetNextLayer(self,root,layer,app_prefix):
        list=[]
        count=0
        try:
            if (layer<=1):
                list.insert(count, app_prefix)
                count+=1
                return list
            if (0==root["children_count"]):
                print "layer is not match, layer:",layer, " ", app_prefix
                list=self.__GetErrList()
                return list
            for eachkey in root["children"].keys():
                next_root=root["children"][eachkey]
                next_app_prefix=app_prefix+"_"+eachkey
                temp_list=self.__GetNextLayer(next_root,layer-1,next_app_prefix)
                if (self.__IsErrList(temp_list)):
                    print "get next layer fail. next_app_prefix:",next_app_prefix
                i=0
                while (i<len(temp_list)):
                    list.insert(count,temp_list[i])
                    count+=1
                    i+=1
        except:
            traceback.print_exc()
            list=self.__GetErrList()
            return list
        return list

    def __GetAppByService(self,svc):
        list=[]
        count=0
        try:
            service_root=self.__root_tree["service"][svc]["app"]
            layer=service_root["Meta"]["app_layer"]
            if (0==service_root["children_count"]):
                list=self.__GetErrList()
                print "children count can not be 0."
                return list
            for eachkey in service_root["children"].keys():
                next_app_prefix=svc+"_"+eachkey
                next_root=service_root["children"][eachkey]
                temp_list=self.__GetNextLayer(next_root,layer,next_app_prefix)
                if (self.__IsErrList(temp_list)):
                    print "get app by service fail.svc:",svc
                    return temp_list
                i=0
                while(i<len(temp_list)):
                    list.insert(count,temp_list[i])
                    count+=1
                    i+=1
        except:
            traceback.print_exc()
        return list
    
    def GetAppList(self):
        list=[]
        count=0
        service=self.__root_tree["service"]
        try:
            for eachkey in service.keys():
                print "key:", eachkey
                temp_list=self.__GetAppByService(eachkey)
                if (self.__IsErrList(temp_list)):
                    return 0,"get app service fail.",temp_list
                i=0
                print "list_len:", len(temp_list)
                while(i<len(temp_list)):
                    list.insert(count,temp_list[i])
                    count+=1
                    i+=1
        except:
            traceback.print_exc()
            list=self.__GetErrList()
            return 1,"access zk except",list
        print "list:", list
        return 0,"",list
    
    def GetAppListByPath(self, app_path):
        list=[]
        path=app_path.split("/")
        if (0==len(path)):
            return 1,"",list
        app_root=self.__root_tree["service"]
        if path[0] not in app_root.keys():
            return 1,"err1",list
        if (1==len(path)):
            list=self.__GetAppByService(path[0])
            if (self.__IsErrList(list)):
                return 1, "err3",list
            return 0, "", list
        app_root=app_root[path[0]]["app"]
        i=1
        app_prefix=path[0]
        layer=app_root["Meta"]["app_layer"]
        while(i<len(path)):
            if path[i] not in app_root["children"].keys():
                print "node:",path[i], "is not exists.i:",i
                return 1, "err2", list
            app_prefix+="_"+path[i]
            layer-=1
            app_root=app_root["children"][path[i]]
            i+=1
        
        list=self.__GetNextLayer(app_root,layer+1,app_prefix)
        if (self.__IsErrList(list)):
            return 1, "err4", list
        return 0, "", list

    def __FindNextLayer(self,root,layer,app_prefix,app_id,list):
        lenth=len(app_prefix)
        if(0==cmp(app_prefix[0:lenth],app_id[0:lenth])):
            if (layer<=0):
                #list.insert(len(list),root["Meta"])
                return 1
            if (0==root["children_count"]):
                print "find next layer fail. layer:", layer
                return 0
            for eachkey in root["children"].keys():
                next_app_prefix=app_prefix+"_"+eachkey
                next_root=root["children"][eachkey]
                find=self.__FindNextLayer(next_root,layer-1,next_app_prefix,app_id,list)
                if (find):
                    list.insert(len(list),eachkey)
                    return 1
        return 0
       
    def __GetSubSpec(self,spec,meta):
        for eachkey in meta.keys():
            if (dict==type(meta[eachkey])):
                if (eachkey not in spec.keys()):
                    spec[eachkey]=meta[eachkey]
                else:
                    self.__GetSubSpec(spec[eachkey],meta[eachkey])
            else:
                spec[eachkey]=meta[eachkey]

    def __GetSpec(self,list,layer,spectype):
        spec={}
        print list
        print layer, spectype
        try:
            if (layer<>(len(list)-1)):
                print "get spec fail. layer:", layer, " listlen:", len(list)
                spec=slef.__GetErrDict()
                return spec
            node=self.__root_tree["service"][list[layer]]["app"]
            meta=node["Meta"]
            if(spectype in meta.keys()):
                if (dict <> type(meta[spectype])):
                    print "err meta type: ", type(meta[spectype])
                    spec=self.__GetErrDict()
                    return spec
                self.__GetSubSpec(spec, meta[spectype])
            i=1
            while(i<=layer):
                if (list[layer-i] not in node["children"].keys()):
                    break
                node=node["children"][list[layer-i]]
                meta=node["Meta"]
                if (spectype not in meta.keys()):
                    i+=1
                    continue
                if (dict <> type(meta[spectype])):
                    print "err meta type: ", type(meta[spectype])
                    spec=self.__GetErrDict()
                    return spec
                self.__GetSubSpec(spec, meta[spectype])
                i+=1
        except:
            traceback.print_exc()
        return spec

    def __GetPackageOrDataSpec(self,metalist,layer,spectype): 
        spec={}
        spec_tmp={}
        filestr=""
        spec_tmp=self.__GetSpec(metalist,layer,"package")
        if("data_source" not in spec_tmp.keys()):
            spec=self.__GetErrDict()
            return spec
        spec_path="spec"
        cmd = "mkdir -p %s" % spec_path
        res = os.system(cmd)
        scm_addr=""
        if ("package"==spectype):
            scm_addr=spec_tmp["data_source"]+"/output/spec/package_spec.json"
            filestr="package_spec.json"
        else:
            if("data"==spectype):
                scm_addr=spec_tmp["data_source"]+"/output/spec/data_spec.json"
                spec_tmp=self.__GetSpec(metalist,layer,"data")
                filestr="data_spec.json"
            else:
                spec=self.__GetErrDict()
                return spec
        cmd = "wget -q -r -nH --preserve-permissions --level=0 --limit-rate=10000k %s -P %s --cut-dirs=4" % (scm_addr, spec_path)
        res = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        local_spec_dir=''
        if res.wait():
            local_spec_dir = ''
        else:
            local_spec_dir = spec_path + '/'+ 'output/spec'
        try:
            file_object = open(local_spec_dir+"/"+filestr)
            all_the_text = ""
            try:
                all_the_text = file_object.read()
            finally:
                file_object.close()
        except:
            traceback.print_exc()
            spec=self.__GetErrDict()
            return spec
        spec=json.loads(all_the_text)
        self.__GetSubSpec(spec, spec_tmp)
        return spec

    #returns (ret_code, error_msg, app_spec), app_spec is in type of dict
    def GetAppSpec(self,app_id):
        app_spec={}
        list=[]
        find=0
        layer=0
        service_root=self.__root_tree["service"]
        for eachkey in service_root.keys():
            list=[]
            next_root=service_root[eachkey]["app"]
            layer=next_root["Meta"]["app_layer"]
            next_app_prefix=eachkey
            find=self.__FindNextLayer(next_root,layer,next_app_prefix,app_id,list)
            if (find):
                list.insert(len(list),eachkey)
                break
        if (find):
            print "start get resource spec."
            temp_spec=self.__GetSpec(list,layer,"resource")
            if (self.__IsErrDict(temp_spec)):
                print "get resource spec fail."
                value=json.dumps(temp_spec)
                return 1,"get resource spec fail.", value
            app_spec["resource"]=temp_spec
            print "start get naming spec."
            temp_spec=self.__GetSpec(list,layer,"naming")
            if (self.__IsErrDict(temp_spec)):
                print "get naming spec fail."
                value=json.dumps(temp_spec)
                return 1, "get naming spec fail.", value
            app_spec["naming"]=temp_spec
            print "start get package spec."
            temp_spec=self.__GetPackageOrDataSpec(list,layer,"package")
            if (self.__IsErrDict(temp_spec)):
                value=json.dumps(temp_spec)
                print "get package spec fail."
                return 1,"get package spec fail.",value
            app_spec["package"]=temp_spec
            print "start get data spec."
            temp_spec=self.__GetPackageOrDataSpec(list,layer,"data")
            if (self.__IsErrDict(temp_spec)):
                value=json.dumps(temp_spec)
                print "get data spec fail."
                return 1,"get data spec fail.",value
            app_spec["data"]=temp_spec
            value=json.dumps(app_spec)
        else:
            print "app is not exists."
            return 1, 'app is not exists.', ''
        return 0,'',value

    def __AddServiceToZk(self,svc,layer):
        cmd_line="./zkcli node set --path='"+self.__root_path+"/service/"+svc+"' --server='"+self.__server+"' --value=''"
        res=os.system(cmd_line)
        meta={}
        meta["app_layer"]=layer
        str=json.dumps(meta)
        cmd_line="./zkcli node set --path='"+self.__root_path+"/service/"+svc+"/app' --server='"+self.__server+"' --value='"+str+"'"
        res = os.system(cmd_line)
        cmd_line="./zkcli node set --path='"+self.__root_path+"/service/"+svc+"/config/event' --server='"+self.__server+"' --value=''"
        res = os.system(cmd_line)
        cmd_line="./zkcli node set --path='"+self.__root_path+"/service/"+svc+"/config/task' --server='"+self.__server+"' --value=''"
        res = os.system(cmd_line)

    def __AddAppToZk(self,app_path):
        list=app_path.split("/")
        app=""
        i=1
        while(i<len(list)):
            app=app+list[i]+"/"
            i+=1
        cmd_line="./zkcli node set --path='"+self.__root_path+"/service/"+list[0]+"/app/"+app+"' --value='' --server='"+self.__server+"'"
        res = os.system(cmd_line)

    #添加一个服务,app_path的格式样例:bs/vip/vip0/jx
    def AddService(self,app_path):
        #添加一个服务时,先设置zk,然后修改内存镜像树
        #设置zk
        list=app_path.split("/")
        if list[0] not in self.__root_tree["service"].keys():
            print "add service into zk:", list[0]
            self.__AddServiceToZk(list[0],len(list)-1)
            meta={}
            meta["app_layer"]=len(list)-1
            self.__root_tree["service"][list[0]]={}
            self.__root_tree["service"][list[0]]["app"]={}
            self.__root_tree["service"][list[0]]["app"]["children_count"]=0
            self.__root_tree["service"][list[0]]["app"]["children"]={}
            self.__root_tree["service"][list[0]]["config"]={}
            self.__root_tree["service"][list[0]]["config"]["event"]={}
            self.__root_tree["service"][list[0]]["config"]["task"]={}
            self.__root_tree["service"][list[0]]["app"]["Meta"]=meta
        #检查设置的层数是否相同,同一服务的所有app层数必须相同
        if ((len(list)-1)!=self.__root_tree["service"][list[0]]["app"]["Meta"]["app_layer"]):
            err_msg="app_path is no invalid. app_path:"+app_path+" actual layer:"+len(list)-1+" expect layer:"+self.__root_tree["service"][list[0]]["app"]["Meta"]["    app_layer"] 
            print err_msg
            return 1,err_msg
        i=1
        #将app添加到zk中
        self.__AddAppToZk(app_path)
        #将app添加到镜像树中
        app_root=self.__root_tree["service"][list[0]]["app"]
        i=1
        while(i<len(list)):
            if (list[i] not in app_root["children"].keys()):
                app_root["children_count"]+=1
                app_root["children"][list[i]]={}
                app_root["children"][list[i]]["children_count"]=0
                app_root["children"][list[i]]["children"]={}
                app_root["children"][list[i]]["Meta"]={}
                app_root["children"][list[i]]["parent"]=app_root
            app_root=app_root["children"][list[i]]
            i+=1
        return 0,""
    
    def SetPathMeta(self,app_path,value):
        list=app_path.split("/")
        meta={}
        try:
            meta=json.loads(value)
        except:
            traceback.print_exc()
            err_msg="value is not valid.value:",value
            print err_msg
            return 1,err_msg
        if(0==len(list)):
            return 1,"list length is zero."
        if list[0] not in self.__root_tree["service"].keys():
            str="service is not exists.svc:"+list[0]
            return 1,str
        node=self.__root_tree["service"][list[0]]["app"]
        i=1
        while(i<(len(list))):
            node=node["children"][list[i]]
            i+=1
        self.__GetSubSpec(node["Meta"],meta)
        str=""
        command_line=""
        new_value=json.dumps(node["Meta"])
        #设置整层服务的meta
        if (1==len(list)):
            command_line="./zkcli node set --path='"+self.__root_path+"/service/"+list[0].encode()+"' --value='"+new_value+"' --server='"+self.__server+"'"
            os.system(command_line)
            return  0,""
        i=1
        while(i<len(list)):
            str=str+"/"+list[i].encode()
            i+=1
        command_line="./zkcli node set --path='"+self.__root_path+"/service/"+list[0].encode()+"/app"+str+"' --value='"+new_value+"' --server='"+self.__server+"'"
        os.system(command_line)
        return 0,""
    
    def SetAppMeta(self,app_id,value):         
        find=0
        layer=0
        list=[]
        service_root=self.__root_tree["service"]
        for eachkey in service_root.keys():                        
            list=[]
            next_root=service_root[eachkey]["app"]                                
            layer=next_root["Meta"]["app_layer"]                                            
            next_app_prefix=eachkey                                                        
            find=self.__FindNextLayer(next_root,layer,next_app_prefix,app_id,list)
            if (find):
                list.insert(len(list),eachkey)                                                            
                break                                                                                                           
        if (find): 
            if (layer<>(len(list)-1)):
                err_msg= "layer is not match list.layer:"+layer+" list_len:"+len(list)
                print err_msg
                return 1,err_msg
            app_path=list[layer]
            i=1
            while(i<=layer):
                app_path+="/"+list[layer-i]
                i+=1
            return self.SetPathMeta(app_path,value)
        err_msg="app-id("+app-id+") is not found."
        print err_msg
        return 1,err_msg

    #删除一个节点时,要考虑其父结点的计数,如果父节点已经没有子节点要连父节点一起删除,并且zk上的信息要一起删除
    #但是服务节点不能以上述规则删除
    def __DelNode(self,approot,node,list):
        temp_node=node
        if (1>=len(list)):
            print "list length is not match."
            return 1
        #表示需要一起删除的父节点层数
        delete_parent_layer=0
        while(temp_node<>approot):
            temp_node["parent"]["children_count"]-=1
            if (0<>temp_node["parent"]["children_count"]):
                node_parent=temp_node["parent"]
                for eachkey in node_parent["children"].keys():
                    if (temp_node==node_parent["children"][eachkey]):
                        del(node_parent["children"][eachkey])
                    break;
                break
            delete_parent_layer+=1
            temp_node=temp_node["parent"]
        i=1
        str=''
        while(i<(len(list)-delete_parent_layer)):
            str+='/'+list[i].encode()
            i+=1
        command_line="./zkcli node del --path='"+self.__root_path+"/service/"+list[0].encode()+"/app"+str+"' --recursive --server='"+self.__server+"'"
        os.system(command_line)
        return 0

    def DelAppPath(self,app_path):
        list=app_path.split("/")
        print "list:", list, "path",app_path
        if(0==len(list)):
            err_msg='del node from mirtree:list length is zero.'
            print err_msg
            return 1,err_msg
        service_root=self.__root_tree["service"]
        print "start del"
        if (1==len(list)):
            print "   ",service_root.keys()
            if (list[0] in service_root.keys()):
                del(service_root[list[0]])
                command_line="./zkcli node del --path='"+self.__root_path+"/service/"+list[0].encode()+"' --recursive --server='"+self.__server+"'"
                os.system(command_line)
            else:
                print "service:",list[0]," not exists in the mirtree."
            return 0,''
        node=service_root[list[0]]["app"]
        i=1
        while(i<len(list)):
            if list[i] not in node["children"].keys():
                err_msg="node is not exists in the mirtree.i:"+i+" app_path:"+app_path
                print err_msg
                return 1,err_msg
            if (i+1>=len(list)):
                self.__DelNode(service_root[list[0]]["app"],node["children"][list[i]],list)
                break
            node=node["children"][list[i]]
            i+=1
        return 0,''

    def DelApp(self,app_id):
        find=0
        layer=0
        list=[]
        print "DelApp: ", app_id
        service_root=self.__root_tree["service"]
        for eachkey in service_root.keys():    
            list=[]
            next_root=service_root[eachkey]["app"]
            layer=next_root["Meta"]["app_layer"]    
            next_app_prefix=eachkey
            find=self.__FindNextLayer(next_root,layer,next_app_prefix,app_id,list)
            if (find):    
                list.insert(len(list),eachkey)    
                break 
        if (find):
            if (layer<>(len(list)-1)):
                err_msg="layer is not match list.layer:"+layer+" list_len:"+len(list)
                print err_msg
                return 1,err_msg
            app_path=list[layer]
            i=1
            while(i<=layer):
                app_path+="/"+list[layer-i]
                i+=1
            return self.DelAppPath(app_path)
        else:
            err_msg="app_path("+app_id+") is not found."
            print err_msg
            return 1,err_msg

    def PrintTree(self):
        print "path:",self.__root_path
        print "server:",self.__server
        print "tree:",self.__root_tree
