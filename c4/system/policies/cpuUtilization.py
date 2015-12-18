#!/usr/bin/env python

import json

from c4.system.db import DBManager
from c4.system.policyEngine import Event, PolicyWrapper
from c4.system.configuration import Configuration

class CpuUtilization(Event):

    id = "cpu.utilization"

    def evaluate(self):
        """
        For all nodes, get the CPU usage.  Return the max value.
        """
        cpu_usage = 0.0
        nodes_and_cpudevmgrs = self.getAllCpuDMNames()
        # for all nodes, get the name of the cpu devmgr
        for node, cpu_name in nodes_and_cpudevmgrs.iteritems():
            # get the cpu usage for this cpu devmgr
            cpu_usage_temp = self.getCpuUsage(node, cpu_name)
            # save max cpu usage
            if cpu_usage_temp > cpu_usage:
                cpu_usage = cpu_usage_temp
        return cpu_usage

    def getAllCpuDMNames(self):
        """
        Get a dictionary of node_name and cpu device manager name from the
        database configuration table.
        """
        nodes_and_cpudevmgrs = {}

        configuration = Configuration()
        for nodeName in configuration.getNodeNames():
            for device in configuration.getDevices(nodeName, flatDeviceHierarchy=True):
                if type == "c4.system.devices.cpu.Cpu":
                    nodes_and_cpudevmgrs[nodeName] = device.name

        return nodes_and_cpudevmgrs

    def getCpuUsage(self, node, name):
        cpu_usage = 0.0
        dbm = DBManager()
        rows = dbm.query("select details from t_sm_latest where node = ? and name = ?", (node, name,))
        if len(rows) > 0 and rows[0][0] is not None:
            details = json.loads(rows[0][0])
            cpu_usage = details.get("usage", 0.0)
        dbm.close()
        return cpu_usage

class CpuPolicy(PolicyWrapper):

    id = "cpu.policy"
    policy = "(cpu.utilization >= 90.0) -> System.log('CPU utilization high')"
