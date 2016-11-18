#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""base class of Task
"""

__author__ = 'guoshaodan01@baidu.com (Guo Shaodan)'

import threading
import json

class TaskContext:

    def __init__(self, service_manager, bhcli_helper, matrix_cluster):
        self.service_manager_ = service_manager
        self.bhcli_ = bhcli_helper
        self.matrix_cluster_ = matrix_cluster

#class AMTask(threading.Thread):
class AMTask(object):
    def __init__(self, task_name, context, options_str, service_id, app_id=None, app_group_id=None):
        #super(AMTask, self).__init__(name = task_name)

        self.context_ = context
        self.task_name_ = task_name
        self.service_id_ = service_id
        self.app_id_ = app_id
        self.app_group_id_ = app_group_id
        if options_str != None and len(options_str) != 0:
            self.options_ = json.loads(options_str)
        else:
            self.options_ = {}

    def run(self):
        pass

