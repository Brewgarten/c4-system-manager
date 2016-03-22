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

class DisableDeviceManager(Envelope):
    """
    A message sent from the active system manager to a thin or passive system manager to
    disable one or more device managers.

    :param To: a system manager
    :type To: str
    """
    def __init__(self, To):
        super(DisableDeviceManager, self).__init__("system-manager", To)

class DisableNode(Envelope):
    """
    A message sent from the active system manager to a thin or passive system manager to
    disable a node.

    :param To: a system manager
    :type To: str
    """
    def __init__(self, To):
        super(DisableNode, self).__init__("system-manager", To)

class EnableDeviceManager(Envelope):
    """
    A message sent from the active system manager to a thin or passive system manager to
    enable a device managers.

    :param To: a system manager
    :type To: str
    """
    def __init__(self, To):
        super(EnableDeviceManager, self).__init__("system-manager", To)

class EnableNode(Envelope):
    """
    A message sent from the active system manager to a thin or passive system manager to
    enable itself.

    :param To: a system manager
    :type To: str
    """
    def __init__(self, To):
        super(EnableNode, self).__init__("system-manager", To)

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

class Operation(Envelope):
    """
    A message sent from the active system manager to a device manager to perform
    a specific operation.

    :param To: a device manager
    :type To: str
    :param name: name of the operation
    :type name: str
    :param arguments: arguments
    :param keywordArguments: keyword arguments
    """
    def __init__(self, To, name, *arguments, **keywordArguments):
        super(Operation, self).__init__("system-manager", To)
        self.Message["name"] = name
        if arguments:
            self.Message["arguments"] = arguments
        if keywordArguments:
            self.Message["keywordArguments"] = keywordArguments

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

class RegistrationNotification(Envelope):
    """
    A message sent from a node to the system manager to register itself.

    :param From: a node's address
    :type From: str
    """
    def __init__(self, From):
        super(RegistrationNotification, self).__init__(From, "system-manager")

class StartDeviceManagers(Envelope):
    """
    A message sent from the system manager to a node to start device managers

    :param To: node address
    :type To: str
    """
    def __init__(self, To):
        super(StartDeviceManagers, self).__init__("system-manager", To)
        self.Message["devices"] = {}

class StartNode(Envelope):
    """
    A message sent from the system manager to a node to start it

    :param To: node address
    :type To: str
    """
    def __init__(self, To):
        super(StartNode, self).__init__("system-manager", To)

class Status(Envelope):
    """
    A message sent from the system manager to device managers to ask for its status.
    The response contains a message with attributes such as ``healthy`` and ``details``.

    :param To: a device manager's address
    :type To: str
    """
    def __init__(self, To):
        super(Status, self).__init__("system-manager", To)

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

class StopNode(Envelope):
    """
    A message sent from the active system manager to another system manager to stop it and all
    its device managers.

    :param To: node address
    :type To: str
    """
    def __init__(self, To):
        super(StopNode, self).__init__("system-manager", To)

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
