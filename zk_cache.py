#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" Utility that makes placements for index partitions into a cluster. 
"""

__author__ = 'guoshaodan01@baidu.com (Guo Shaodan)'

import threading
import copy

class ZNode:
    def __init__(self):
        self.meta = None
        self.children = {}
        self.children_ok = False

class ZKCache:
    MISS = -1
    OK = 0
    ERROR = 1
    def __init__(self):
        self.root = ZNode()

        self.lock_ = threading.Lock()


    def Get(self, path):
        self.lock_.acquire()
        ret, ref_node = self.__find_node(path)
        if ret != 0:
            self.lock_.release()
            return ret, None
        if ref_node.meta == None:
            self.lock_.release()
            return ZKCache.MISS, None
        meta = copy.deepcopy(ref_node.meta)
        self.lock_.release()
        return 0, meta


    def Set(self, path, meta):
        self.lock_.acquire()
        if path == "/":
            self.root.meta = meta
            self.lock_.release()
            return 0

        if path.startswith('/'):
            path = path[1:]

        nodes = path.split('/')
        ref_node =  self.root
        for node in nodes:
            if node == '':
                break
            if node not in ref_node.children:
                ref_node.children[node] = ZNode()
            ref_node = ref_node.children[node]
        ref_node.meta = copy.deepcopy(meta)
        self.lock_.release()
        return 0 

    def AddChildren(self, path, children):
        self.lock_.acquire()

        if path.startswith('/'):
            path = path[1:]
        nodes = path.split('/')
        ref_node =  self.root
        for node in nodes:
            if node == '':
                break
            if node not in ref_node.children:
                ref_node.children[node] = ZNode()
            ref_node = ref_node.children[node]
        for node in children:
            if node not in ref_node.children:
                ref_node.children[node] = ZNode()
        ref_node.children_ok = True
        self.lock_.release()
        return 0 


    def List(self, path):
        self.lock_.acquire()
        ret, ref_node = self.__find_node(path)
        if ret != 0:
            self.lock_.release()
            return ret, None
        if not ref_node.children_ok:
            self.lock_.release()
            return ZKCache.MISS, None
        children = ref_node.children.keys()
        self.lock_.release()
        return 0, children

    def Del(self, path):
        if path == "" or path == "/":
            return ZKCache.ERROR
        if path[-1] == "/":
            path = path[:-1]
        pare_slash = path.rfind('/')
        if pare_slash == -1:
            parent_path = ""
        else:
            parent_path = path[:pare_slash]
        node_name = path[len(parent_path)+1:]

        self.lock_.acquire()
        ret, ref_node = self.__find_node(parent_path)
        if ret != 0:
            self.lock_.release()
            #return success if not existed
            return 0
        if node_name in ref_node.children:
            del ref_node.children[node_name]
        self.lock_.release()
        return 0

    def __find_node(self, path):
        if path == "/":
            return 0, self.root

        #cache miss if wrong formmat
        if path.startswith('/'):
            path = path[1:]

        nodes = path.split('/')
        ref_node =  self.root
        for node in nodes:
            if node == '':
                break
            if node not in ref_node.children:
                return ZKCache.MISS,None
            ref_node = ref_node.children[node]
        return 0, ref_node

