#! /usr/bin/env python
#

import json
import zkmirror

mir=zkmirror.ZkMirror("/beehive/appmaster_version_1","10.38.189.15:1181")

ret=mir.CreateTree()

mir.AddService("bs/vip/vip4/tj")
#mir.AddService("bs/vip/vip0/tc")
#mir.AddService("bs/vip/vip0/tc")

#mir.AddService("bc/group/jx")

metar={'resource':{'liuzhixiang':'ok'},'naming':{'liuzhixiang':'ok'}}
str=json.dumps(metar)
print "str",str
mir.SetPathMeta("bs/jx/vip/vip1",str)
metar={'naming':{'liuzhiwei':'ok'}}
str=json.dumps(metar)
mir.SetPathMeta("bs/jx/vip",str)
metar={'resource':{'liuzhiwei':'no ok'},'naming':{'liuzhiwei':'ok'}}
str=json.dumps(metar)
mir.SetPathMeta('bs/jx',str)
metar={'package':{'data_source':'ftp://cq01-ps-pa-rdtest3.cq01//home/work/bs_output2/'},'data':{'data_source':'ftp://cq01-ps-pa-rdtest3.cq01//home/work/bs_output2/'}}
str=json.dumps(metar)
mir.SetPathMeta("bs/jx",str)
mir.DelApp("bs_vip_vip4_tj")
ret,err,list=mir.GetAppListByPath("bs/jx/vip")
print "list:",list

ret, err, value=mir.GetAppSpec("bs_jx_vip_vip1")
if (0<>ret):
    print "fail:",err
else:
    print "sucess:",value
#mir.DelApp("bs_vip_vip4_tj")
#ret,err,list=mir.GetAppList()
#print "list2:", list

mir.PrintTree()

