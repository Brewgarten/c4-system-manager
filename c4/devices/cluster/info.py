"""
Copyright (c) IBM 2015-2017. All Rights Reserved.
Project name: c4-system-manager
This project is licensed under the MIT License, see LICENSE
"""
from c4.system.backend import Backend
from c4.system.deviceManager import (DeviceManagerImplementation, DeviceManagerStatus,
                                     operation)
from c4.utils.logutil import ClassLogger


@ClassLogger
class Info(DeviceManagerImplementation):
    """
    Information device manager which is used to abstract configuration and
    state information of the cluster such that it can be queried from all
    nodes in the cluster.
    """
    def __init__(self, clusterInfo, name, properties=None):
        super(Info, self).__init__(clusterInfo, name, properties=properties)

    @operation
    def getNode(self, node, includeDevices=True, flatDeviceHierarchy=False):
        """
        Get node information for the specified system manager

        :param node: node
        :type node: str
        :param includeDevices: include devices for the node
        :type includeDevices: bool
        :param flatDeviceHierarchy: flatten device hierarchy
        :type flatDeviceHierarchy: bool
        :returns: node
        :rtype: :class:`~c4.system.configuration.NodeInfo`
        """
        return Backend().configuration.getNode(node,
                                               includeDevices=includeDevices,
                                               flatDeviceHierarchy=flatDeviceHierarchy)

    @operation
    def getNodeNames(self):
        """
        Return a list of node names.
        """
        return Backend().configuration.getNodeNames()

    @operation
    def getSystemManagerNodeName(self):
        """
        Get node name of the active system manager

        :returns: node name
        :rtype: str
        """
        return Backend().configuration.getSystemManagerNodeName()

    def handleStatus(self, message):
        """
        The handler for an incoming Status message.
        """
        return InfoStatus(self.state)

class InfoStatus(DeviceManagerStatus):
    """
    Information device manager

    :param state: state
    :type state: :class:`~c4.system.configuration.States`
    """
    def __init__(self, state):
        super(InfoStatus, self).__init__()
        self.state = state
