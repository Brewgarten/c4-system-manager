from c4.messaging import Envelope

class DeployDeviceManager(Envelope):
    """
    A message sent from the REST server to the active system manager to distribute and install the custom
    device manager egg.
    Or a message sent from the active system manager to the other system managers to install the custom
    device manager egg.

    :param From: typically localhost/rest or system-manager
    :type From: str
    :param To: typically system-manager or node (i.e. other system managers)
    :type To: str
    """
    def __init__(self, From, To):
        super(DeployDeviceManager, self).__init__(From, To, "deploydevicemanager")

class DeployPolicy(Envelope):
    """
    A message sent from the REST server to the active system manager to distribute and install the custom
    policy egg.
    Or a message sent from the active system manager to the other system managers to install the custom
    policy egg.

    :param From: typically localhost/rest or system-manager
    :type From: str
    :param To: typically system-manager or node (i.e. other system managers)
    :type To: str
    """
    def __init__(self, From, To):
        super(DeployPolicy, self).__init__(From, To, "deploypolicy")

class DisableDeviceManager(Envelope):
    """
    A message sent from the active system manager to a thin or passive system manager to
    disable one or more device managers.

    :param To: a system manager
    :type To: str
    """
    def __init__(self, To):
        super(DisableDeviceManager, self).__init__("system-manager", To)

class DisableDeviceManagerRequest(Envelope):
    """
    A message sent from the REST server to the active system manager to disable one or more device managers.
    Change device manager state from RUNNING to MAINTENANCE or REGISTERED to MAINTENANCE.

    :param From: localhost/rest
    :type From: str
    :param deviceList: List of devices to disable.  * is a valid device name.
    :type deviceList: list
    :param nodeList: List of nodes where the given device managers will be disabled.  * is a valid node name.
    :type nodeList: list
    """
    def __init__(self, From, deviceList, nodeList):
        super(DisableDeviceManagerRequest, self).__init__(From, "system-manager")
        self.Message["names"] = deviceList
        self.Message["nodes"] = nodeList

class DisableNode(Envelope):
    """
    A message sent from the active system manager to a thin or passive system manager to
    disable a node.

    :param To: a system manager
    :type To: str
    """
    def __init__(self, To):
        super(DisableNode, self).__init__("system-manager", To)

class DisableNodeRequest(Envelope):
    """
    A message sent from the REST server to a system manager to disable one or more nodes
    Changes node state from REGISTERED to MAINTENANCE

    :param From: localhost/rest
    :type From: str
    :param nodeList: List of nodes. * is a valid node name.
    :type nodeList: list
    """
    def __init__(self, From, nodeList):
        super(DisableNodeRequest, self).__init__(From, "system-manager")
        self.Message["nodes"] = nodeList

class DisablePolicyRequest(Envelope):
    """
    A message sent from the REST server to a system manager to disable one or more policies.

    :param From: localhost/rest
    :type From: str
    :param policyList: List of policy names to disable.  * is a valid policy name.
    :type policyList: list
    """
    def __init__(self, From, policyList):
        super(DisablePolicyRequest, self).__init__(From, "system-manager")
        self.Message["policies"] = policyList

class EnableDeviceManager(Envelope):
    """
    A message sent from the active system manager to a thin or passive system manager to
    enable a device managers.

    :param To: a system manager
    :type To: str
    """
    def __init__(self, To):
        super(EnableDeviceManager, self).__init__("system-manager", To)

class EnableDeviceManagerRequest(Envelope):
    """
    A message sent from the REST server to a system manager to enable one or more device managers.
    Changes device manager state from MAINTENANCE to REGISTERED.

    :param From: localhost/rest
    :type From: str
    :param deviceList: List of devices to enable.  * is a valid device name.
    :type deviceList: list
    :param nodeList: List of nodes where the given device managers will be enabled.  * is a valid node name.
    :type nodeList: list
    """
    def __init__(self, From, deviceList, nodeList):
        super(EnableDeviceManagerRequest, self).__init__(From, "system-manager")
        self.Message["names"] = deviceList
        self.Message["nodes"] = nodeList

class EnableNode(Envelope):
    """
    A message sent from the active system manager to a thin or passive system manager to
    enable itself.

    :param To: a system manager
    :type To: str
    """
    def __init__(self, To):
        super(EnableNode, self).__init__("system-manager", To)

class EnableNodeRequest(Envelope):
    """
    A message sent from the REST server to a system manager to enable one or more nodes
    Changes node state from MAINTENANCE to REGISTERED.

    :param From: localhost/rest
    :type From: str
    :param nodeList: List of nodes. * is a valid node name.
    :type nodeList: list
    """
    def __init__(self, From, nodeList):
        super(EnableNodeRequest, self).__init__(From, "system-manager")
        self.Message["nodes"] = nodeList

class EnablePolicyRequest(Envelope):
    """
    A message sent from the REST server to a system manager to enable one or more policies.

    :param From: localhost/rest
    :type From: str
    :param policyList: List of policy names to enable.  * is a valid policy name.
    :type policyList: list
    """
    def __init__(self, From, policyList):
        super(EnablePolicyRequest, self).__init__(From, "system-manager")
        self.Message["policies"] = policyList

class GPFSClusterManagerTakeOverNotification(Envelope):
    """
    A message sent from a GPFS notification agent to the system manager after a clusterManagerTakeOver event

    :param From: location of the notification agent
    :type From: str
    :param To: the address of the system manager
    :type To: str
    """
    def __init__(self, From, To):
        super(GPFSClusterManagerTakeOverNotification, self).__init__(From, To)

class GPFSDiskFailureNotification(Envelope):
    """
    A message sent from a GPFS notification agent to the system manager after a diskFailure event

    :param From: location of the notification agent
    :type From: str
    :param To: the address of the system manager
    :type To: str
    """
    def __init__(self, From, To):
        super(GPFSDiskFailureNotification, self).__init__(From, To)

class GPFSFilesetLimitExceededNotification(Envelope):
    """
    A message sent from a GPFS notification agent to the system manager after a fileSetLimitExceeded event

    :param From: location of the notification agent
    :type From: str
    :param To: the address of the system manager
    :type To: str
    """
    def __init__(self, From, To):
        super(GPFSFilesetLimitExceededNotification, self).__init__(From, To)

class GPFSFLowDiskSpaceNotification(Envelope):
    """
    A message sent from a GPFS notification agent to the system manager after a lowDiskSpace event

    :param From: location of the notification agent
    :type From: str
    :param To: the address of the system manager
    :type To: str
    """
    def __init__(self, From, To):
        super(GPFSFLowDiskSpaceNotification, self).__init__(From, To)

class GPFSMountNotification(Envelope):
    """
    A message sent from a GPFS notification agent to the system manager after a mount event

    :param From: location of the notification agent
    :type From: str
    :param To: the address of the system manager
    :type To: str
    """
    def __init__(self, From, To):
        super(GPFSMountNotification, self).__init__(From, To)

class GPFSNodeJoinNotification(Envelope):
    """
    A message sent from a GPFS notification agent to the system manager after a nodeJoin event

    :param From: location of the notification agent
    :type From: str
    :param To: the address of the system manager
    :type To: str
    """
    def __init__(self, From, To):
        super(GPFSNodeJoinNotification, self).__init__(From, To)

class GPFSNodeLeaveNotification(Envelope):
    """
    A message sent from a GPFS notification agent to the system manager after a nodeLeave event

    :param From: location of the notification agent
    :type From: str
    :param To: the address of the system manager
    :type To: str
    """
    def __init__(self, From, To):
        super(GPFSNodeLeaveNotification, self).__init__(From, To)

class GPFSNoDiskSpaceNotification(Envelope):
    """
    A message sent from a GPFS notification agent to the system manager after a noDiskSpace event

    :param From: location of the notification agent
    :type From: str
    :param To: the address of the system manager
    :type To: str
    """
    def __init__(self, From, To):
        super(GPFSNoDiskSpaceNotification, self).__init__(From, To)

class GPFSNotification(Envelope):
    """
    A generic message sent from a GPFS notification agent to the system manager

    :param From: location of the notification agent
    :type From: str
    :param To: the address of the system manager
    :type To: str
    """
    def __init__(self, From, To):
        super(GPFSNotification, self).__init__(From, To)

class GPFSPreMountNotification(Envelope):
    """
    A message sent from a GPFS notification agent to the system manager after a preMount event

    :param From: location of the notification agent
    :type From: str
    :param To: the address of the system manager
    :type To: str
    """
    def __init__(self, From, To):
        super(GPFSPreMountNotification, self).__init__(From, To)

class GPFSPreShutdownNotification(Envelope):
    """
    A message sent from a GPFS notification agent to the system manager after a preShutdown event

    :param From: location of the notification agent
    :type From: str
    :param To: the address of the system manager
    :type To: str
    """
    def __init__(self, From, To):
        super(GPFSPreShutdownNotification, self).__init__(From, To)

class GPFSPreStartupNotification(Envelope):
    """
    A message sent from a GPFS notification agent to the system manager after a preStartup event

    :param From: location of the notification agent
    :type From: str
    :param To: the address of the system manager
    :type To: str
    """
    def __init__(self, From, To):
        super(GPFSPreStartupNotification, self).__init__(From, To)

class GPFSPreUnmountNotification(Envelope):
    """
    A message sent from a GPFS notification agent to the system manager after a preUnmount event

    :param From: location of the notification agent
    :type From: str
    :param To: the address of the system manager
    :type To: str
    """
    def __init__(self, From, To):
        super(GPFSPreUnmountNotification, self).__init__(From, To)

class GPFSQuorumLossNotification(Envelope):
    """
    A message sent from a GPFS notification agent to the system manager after a quorumLoss event

    :param From: location of the notification agent
    :type From: str
    :param To: the address of the system manager
    :type To: str
    """
    def __init__(self, From, To):
        super(GPFSQuorumLossNotification, self).__init__(From, To)

class GPFSQuorumNodeJoinNotification(Envelope):
    """
    A message sent from a GPFS notification agent to the system manager after a quorumNodeJoin event

    :param From: location of the notification agent
    :type From: str
    :param To: the address of the system manager
    :type To: str
    """
    def __init__(self, From, To):
        super(GPFSQuorumNodeJoinNotification, self).__init__(From, To)

class GPFSQuorumNodeLeaveNotification(Envelope):
    """
    A message sent from a GPFS notification agent to the system manager after a quorumNodeLeave event

    :param From: location of the notification agent
    :type From: str
    :param To: the address of the system manager
    :type To: str
    """
    def __init__(self, From, To):
        super(GPFSQuorumNodeLeaveNotification, self).__init__(From, To)

class GPFSQuorumReachedNotification(Envelope):
    """
    A message sent from a GPFS notification agent to the system manager after a quorumReached event

    :param From: location of the notification agent
    :type From: str
    :param To: the address of the system manager
    :type To: str
    """
    def __init__(self, From, To):
        super(GPFSQuorumReachedNotification, self).__init__(From, To)

class GPFSShutdownNotification(Envelope):
    """
    A message sent from a GPFS notification agent to the system manager after a shutdown event

    :param From: location of the notification agent
    :type From: str
    :param To: the address of the system manager
    :type To: str
    """
    def __init__(self, From, To):
        super(GPFSShutdownNotification, self).__init__(From, To)

class GPFSStartupNotification(Envelope):
    """
    A message sent from a GPFS notification agent to the system manager after a startup event

    :param From: location of the notification agent
    :type From: str
    :param To: the address of the system manager
    :type To: str
    """
    def __init__(self, From, To):
        super(GPFSStartupNotification, self).__init__(From, To)

class GPFSUnmountNotification(Envelope):
    """
    A message sent from a GPFS notification agent to the system manager after a unmount event

    :param From: location of the notification agent
    :type From: str
    :param To: the address of the system manager
    :type To: str
    """
    def __init__(self, From, To):
        super(GPFSUnmountNotification, self).__init__(From, To)

class RegisterDeviceManager(Envelope):
    """
    A message from the active system manager to a thin system manager to add device(s)

    :param To: thin system manager node
    :type To: str
    :param deviceList: the devices
    :type deviceList: [str]
    :param deviceType: the type of the device(s) to register
    :type deviceType: str
    """
    def __init__(self, To, deviceList, deviceType):
        super(RegisterDeviceManager, self).__init__('system-manager', To)
        self.Message['devices'] = deviceList
        self.Message['type'] = deviceType

class RegisterDeviceManagerRequest(Envelope):
    """
    A message from REST to tell the active system manager to add device(s) to the configuration

    :param From: locahost/rest
    :type From: str
    :param deviceList: the devices
    :type deviceList: [str]
    :param nodeList: the nodes where the device(s) will be registered
    :type nodeList: [str]
    :param deviceType: the type of the device(s) to register
    :type deviceType: str
    """
    def __init__(self, From, deviceList, nodeList, deviceType):
        super(RegisterDeviceManagerRequest, self).__init__(From, 'system-manager')
        self.Message['devices'] = deviceList
        self.Message['nodes'] = nodeList
        self.Message['type'] = deviceType

class RegistrationNotification(Envelope):
    """
    A message sent from a node to the system manager to register itself.

    :param From: a node's address
    :type From: str
    """
    def __init__(self, From):
        super(RegistrationNotification, self).__init__(From, "system-manager")

class Status(Envelope):
    """
    A message sent from the system manager to device managers to ask for its status.
    The response contains a message with attributes such as ``healthy`` and ``details``.

    :param To: a device manager's address
    :type To: str
    """
    def __init__(self, To):
        super(Status, self).__init__("system-manager", To, "getStatus")

class StartDeviceManagers(Envelope):
    """
    A message sent from the system manager to a node to start device managers

    :param To: node address
    :type To: str
    """
    def __init__(self, To):
        super(StartDeviceManagers, self).__init__("system-manager", To)
        self.Message["devices"] = {}

class StartDeviceManagersRequest(Envelope):
    """
    A message sent from the REST server to the active system manager to start one or more device managers.
    Changes device manager state from REGISTERED to RUNNING or MAINTENANCE to RUNNING.

    :param From: localhost/rest
    :type From: str
    :param deviceList: List of devices to start.  * is a valid device name.
    :type deviceList: list
    :param nodeList: List of nodes where the given device managers will be started.  * is a valid node name.
    :type nodeList: list
    """
    def __init__(self, From, deviceList, nodeList):
        super(StartDeviceManagersRequest, self).__init__(From, "system-manager")
        self.Message["names"] = deviceList
        self.Message["nodes"] = nodeList

class StartNode(Envelope):
    """
    A message sent from the system manager to a node to start it

    :param To: node address
    :type To: str
    """
    def __init__(self, To):
        super(StartNode, self).__init__("system-manager", To)

class SystemManagerUpdate(Envelope):
    """
    A message sent from the active system manager to another system manager to indicate that the system manager has changed

    :param To: node address
    :type To: str
    :param address: sysmgr address in the form of tcp://<address>:<port>
    :type address: str
    """
    def __init__(self, To, address):
        super(SystemManagerUpdate, self).__init__("system-manager", To)
        self.Message['sysmgr_address'] = address

class LocalStartDeviceManager(Envelope):
    """
    A message sent from a node directly to device managers to start them.

    :param From: node address
    :type From: str
    :param To: a device manager's address or a thin system manager
    :type To: str
    """
    def __init__(self, From, To):
        super(LocalStartDeviceManager, self).__init__(From, To)

class LocalStopDeviceManager(Envelope):
    """
    A message sent from a node directly to device managers to stop them.

    :param From: node address
    :type From: str
    :param To: a device manager's address or a thin system manager
    :type To: str
    """
    def __init__(self, From, To):
        super(LocalStopDeviceManager, self).__init__(From, To)

class LocalStopNode(Envelope):
    """
    A message sent from the a node to itself to stop it and all
    its device managers.

    :param To: node address
    :type To: str
    """
    def __init__(self, To):
        super(LocalStopNode, self).__init__(To, To)

class LocalUnregisterDeviceManagers(Envelope):
    """
    A message from  active system manager to itself to remove a device from the configuration
    :param node: the node the devices live on
    :type To: str
    :param deviceList: the devices
    :type deviceList: [str]
    """
    def __init__(self, node, deviceList):
        super(LocalUnregisterDeviceManagers, self).__init__('system-manager', 'system-manager')
        self.Message['node'] = node
        self.Message['devices'] = deviceList

class StopDeviceManagers(Envelope):
    """
    A message sent from an active system manager to another system manager to stop
    its device managers.

    :param To: node address
    :type To: str
    """
    def __init__(self, To):
        super(StopDeviceManagers, self).__init__("system-manager", To)
        self.Message["devices"] = {}

class StopDeviceManagersRequest(Envelope):
    """
    A message sent from the REST server to the active system manager to disable one or more device managers.
    Changes device manager state from RUNNING to REGISTERED.

    :param From: localhost/rest
    :type From: str
    :param deviceList: List of devices to disable.  * is a valid device name.
    :type deviceList: list
    :param nodeList: List of nodes where the given device managers will be disabled.  * is a valid node name.
    :type nodeList: list
    """
    def __init__(self, From, deviceList, nodeList):
        super(StopDeviceManagersRequest, self).__init__(From, "system-manager")
        self.Message["names"] = deviceList
        self.Message["nodes"] = nodeList

class StopNode(Envelope):
    """
    A message sent from the active system manager to another system manager to stop it and all
    its device managers.

    :param To: node address
    :type To: str
    """
    def __init__(self, To):
        super(StopNode, self).__init__("system-manager", To)

class UndeployDeviceManager(Envelope):
    """
    A message from the rest server to the active system manager to delete the egg.  The system manager
    will also tell other system managers to delete the egg.
    Or a message from the active system manager to the other system managers to delete the egg.

    :param From: typically localhost/rest or system-manager
    :type From: str
    :param To: typically system-manager or node (i.e. other system managers)
    :type To: str
    """
    def __init__(self, From, To):
        super(UndeployDeviceManager, self).__init__(From, To, "undeploydevicemanager")

class UndeployPolicy(Envelope):
    """
    A message from the rest server to the active system manager to delete the egg.  The system manager
    will also tell other system managers to delete the egg.
    Or a message from the active system manager to the other system managers to delete the egg.

    :param From: typically localhost/rest or system-manager
    :type From: str
    :param To: typically system-manager or node (i.e. other system managers)
    :type To: str
    """
    def __init__(self, From, To):
        super(UndeployPolicy, self).__init__(From, To, "undeploypolicy")

class Unregister(Envelope):
    """
    A message from the active system manager telling a node to stop itself and its device managers in order to be removed from the
    configuration.

    :param From: the sender
    :type From: str
    :param To: the receiver
    :type To: str
    :param nodes: nodes to remove (if sent to the active sysmgr)
    :type nodes: [str]
    """
    def __init__(self, From, To, nodes=None):
        super(Unregister, self).__init__(From, To, 'unregister')
        self.Message['nodes'] = nodes

class UnregisterDeviceManagersRequest(Envelope):
    """
    A message from REST to tell the active system manager to remove device(s) from the configuration

    :param From: locahost/rest
    :type From: str
    :param nodeList: the nodes the device manager(s) are on
    :type nodeList: [str]
    :param deviceList: the devices
    :type deviceList: [str]
    """
    def __init__(self, From, nodeList, deviceList):
        super(UnregisterDeviceManagersRequest, self).__init__(From, 'system-manager')
        self.Message['nodes'] = nodeList
        self.Message['devices'] = deviceList

class UnregisterNode(Envelope):
    """
    A message from the active system manager telling a node to stop itself and its device managers in order to be removed from the
    configuration.

    :param From: the sender
    :type From: str
    :param To: the receiver
    :type To: str
    """
    def __init__(self, To):
        super(UnregisterNode, self).__init__('system-manager', To)

class UnregisterNodeRequest(Envelope):
    """
    A message from REST to the active system manager requesting that some node(s) be removed from the configuration

    :param From: the sender
    :type From: str
    :param To: the receiver
    :type To: str
    :param nodes: nodes to remove
    :type nodes: [str]
    """
    def __init__(self, From, nodes):
        super(UnregisterNodeRequest, self).__init__(From, 'system-manager')
        self.Message['nodes'] = nodes
