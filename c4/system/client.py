import platform as platformSpec

from c4.messaging import RouterClient
from c4.system.messages import Operation
from c4.utils.logutil import ClassLogger


@ClassLogger
class Client(RouterClient):
    """
    A client for the system that allows sending requests
    to the system through to the local system manager.

    :param node: optional node name (uses platform node name by default)
    :type node: str
    """
    def __init__(self, node=None):
        if node:
            self.node = node
        else:
            self.node = platformSpec.node()
        super(Client, self).__init__(self.node)
        self.infoDeviceAddress = "system-manager/info"

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
        return self.sendRequest(Operation(self.infoDeviceAddress,
                                          "getNode",
                                          node,
                                          includeDevices=includeDevices,
                                          flatDeviceHierarchy=flatDeviceHierarchy))

    def getNodeNames(self):
        """
        Return a list of node names.
        """
        return self.sendRequest(Operation(self.infoDeviceAddress,
                                          "getNodeNames"))

    def getSystemManagerNodeName(self):
        """
        Get node name of the active system manager

        :returns: node name
        :rtype: str
        """
        return self.sendRequest(Operation(self.infoDeviceAddress,
                                          "getSystemManagerNodeName"))
