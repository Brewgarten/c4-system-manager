import ctypes
import logging
import multiprocessing

from c4.utils.enum import Enum
from c4.utils.jsonutil import JSONSerializable
from c4.utils.logutil import ClassLogger
from c4.utils.version import BasicVersion
from c4.system.backend import Backend
from abc import ABCMeta, abstractmethod


log = logging.getLogger(__name__)

# Lowest version of SQLite with Common Table Expression support
SqliteCTEMinimumVersion=BasicVersion("3.8.3")

class Roles(Enum):
    """
    Enumeration of node roles
    """
    ACTIVE = "active"
    PASSIVE = "passive"
    THIN = "thin"

class States(Enum):
    """
    Enumeration of states
    """
    UNDEPLOYED = "undeployed"

    DEPLOYING = "deploying"
    UNDEPLOYING = "undeploying"

    DEPLOYED = "deployed"

    REGISTERING = "registering"
    UNREGISTERING = "unregistering"

    REGISTERED = "registered"

    ENABLING = "enabling"
    DISABLING = "disabling"

    MAINTENANCE = "maintenance"

    STARTING = "starting"
    STOPPING = "stopping"

    RUNNING = "running"

@ClassLogger
class Configuration(object):
    """
    System configuration interface
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def addAlias(self, alias, node):
        """
        Add an alias for a node.

        :param alias: alias
        :type alias: str
        :param node: node name
        :type node: str
        :returns: alias
        :rtype: str
        """

    @abstractmethod
    def addDevice(self, node, fullDeviceName, device):
        """
        Adds a device to the configuration. Throws exception on error.

        :param node: node name
        :type node: str
        :param fullDeviceName: fully qualified device name
        :type fullDeviceName: str
        :param device: device
        :type device: :class:`~c4.system.configuration.DeviceInfo`
        :returns: device info with ids
        :rtype: :class:`~c4.system.configuration.DeviceInfo`
        """

    @abstractmethod
    def addNode(self, node):
        """
        Add node

        :param node: node
        :type node: :class:`~c4.system.configuration.NodeInfo`
        :returns: node info with database ids
        :rtype: :class:`~c4.system.configuration.NodeInfo`
        """

    @abstractmethod
    def addPlatform(self, platform):
        """
        Add platform information

        :param platform: platform
        :type platform: :class:`~c4.system.configuration.PlatformInfo`
        """

    @abstractmethod
    def clear(self):
        """
        Removes all nodes and devices from the configuration object and the database.
        """

    @abstractmethod
    def changeAlias(self, alias, node):
        """
        Change the node an alias refers to

        :param alias: alias
        :type alias: str
        :param node: node
        :type node: str
        :returns: alias
        :rtype: str
        """

    @abstractmethod
    def changeRole(self, node, role):
        """
        Change role of a system manager

        :param node: node
        :type node: str
        :param state: role
        :type state: :class:`Roles`
        :returns: previous role
        :rtype: :class:`Roles`
        """
        if not isinstance(role, Roles):
            self.log.error("'%s' does not match enum of type '%s'", role, Roles)
            return
        roleName = self.changeDetail(node, None, "role", role.name)
        if not roleName:
            return None
        return Roles.valueOf(roleName)

    @abstractmethod
    def changeState(self, node, name, state):
        """
        Change state of a system or device manager

        :param node: node
        :type node: str
        :param name: device manager name
        :type name: str
        :param state: state
        :type state: :class:`States`
        :returns: previous state
        :rtype: :class:`States`
        """

    @abstractmethod
    def changeTargetState(self, node, name, state):
        """
        Change target state of a system or device manager

        :param node: node
        :type node: str
        :param name: device manager name
        :type name: str
        :param state: state
        :type state: :class:`States`
        :returns: previous target state
        :rtype state: :class:`States`
        """

    def getAddress(self, node):
        """
        Given a node name or node alias,
        look up and return its address or
        return None if not found.

        :param node: node or alias
        :type node: str
        :returns: node address
        :rtype str
        """
        info = self.getNode(node, includeDevices=False)

        if info is not None:
            return info.address

        # try to use the alias
        nodeName = self.resolveAlias(node)
        if nodeName is None:
            self.log.error("could not get address because node '%s' does not exist", node)
            return None

        info =  self.getNode(nodeName, includeDevices=False)
        if info is None:
            self.log.error("could not get address because node for alias '%s' does not exist", nodeName)
            return None
        return info.address

    @abstractmethod
    def getAliases(self):
        """
        Get a mapping of aliases to node names

        :returns: mappings
        :rtype: dict
        """

    def getDevice(self, node, fullDeviceName):
        """
        Get device information

        :param node: node
        :type node: str
        :param fullDeviceName: fully qualified device name
        :type fullDeviceName: str
        :returns: device info
        :rtype: :class:`~c4.system.configuration.DeviceInfo`
        """
        nodeInfo = self.getNode(node)
        if nodeInfo is None:
            self.log.error("could not get device '%s' because node '%s' does not exist", fullDeviceName, node)
            return None

        # iterate over device name parts to go down the hierarchy
        deviceParts = fullDeviceName.split(".")
        deviceInfo = nodeInfo
        for devicePart in deviceParts:

            if devicePart not in deviceInfo.devices:
                self.log.error("unable to get device because device parent '%s' not found for device '%s'", devicePart, fullDeviceName)
                return None
            deviceInfo = deviceInfo.devices[devicePart]

        return deviceInfo

    def getDevices(self, node, flatDeviceHierarchy=False):
        """
        Get device information for the specified system manager

        :param node: node
        :type node: str
        :param flatDeviceHierarchy: flatten device hierarchy
        :type flatDeviceHierarchy: bool
        :returns: device infos
        :rtype: dict
        """
        nodeInfo = self.getNode(node, flatDeviceHierarchy=flatDeviceHierarchy)
        if nodeInfo is None:
            self.log.error("Could get device list because node '%s' does not exist", node)
            return {}
        return nodeInfo.devices

    @abstractmethod
    def getPlatform(self):
        """
        Get platform information

        :returns: platform
        :rtype: :class:`~c4.system.configuration.PlatformInfo`
        """

    def getRole(self, node):
        """
        Get the role of a system manager.

        :param node: node
        :type node: str
        :returns: role
        :rtype: :class:`Roles`
        """
        info = self.getNode(node, includeDevices=False)

        if info is None:
            self.log.error("could not get role because '%s' does not exist", node)
            return None
        return info.role

    def getState(self, node, name=None):
        """
        Get the state of a system or device manager.

        :param node: node
        :type node: str
        :param name: device manager name
        :type name: str
        :returns: :class:`~c4.system.configuration.States`
        """
        if name:
            info = self.getDevice(node, name)
        else:
            info = self.getNode(node, includeDevices=False)

        if info is None:
            self.log.error("could not get state because '%s%s' does not exist", node, "/" + name if name else "")
            return None
        return info.state

    def getSystemManagerNodeName(self):
        """
        Get node name of the active system manager

        :returns: node name
        :rtype: str
        """
        return self.resolveAlias("system-manager")

    @abstractmethod
    def getTargetState(self, node, name=None):
        """
        Get the target state of a node or device manager.

        :param node: node
        :type node: str
        :param name: device manager name
        :type name: str
        :returns: :class:`~c4.system.configuration.States`
        """

    @abstractmethod
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

    @abstractmethod
    def getNodeNames(self):
        """
        Return a list of node names.
        """

    def loadFromInfo(self, configurationInfo):
        """
        Load configuration from the specified configuration information.

        :param configurationInfo: configuration information
        :type configurationInfo: :class:`~c4.system.configuration.ConfigurationInfo`
        :raises: :class:`ConfigurationValidationError` raised when validation fails.
        """
        configurationInfo.validate()
        for node in configurationInfo.nodes.values():
            self.addNode(node)
        for alias, nodeName in configurationInfo.aliases.items():
            self.addAlias(alias, nodeName)
        self.addPlatform(configurationInfo.platform)

    @abstractmethod
    def removeDevice(self, node, fullDeviceName):
        """
        Remove a device from the configuration

        :param node: node name
        :type node: str
        :param fullDeviceName: fully qualified device name
        :type fullDeviceName: str
        """

    @abstractmethod
    def removeNode(self, node):
        """
        Remove node from the configuration

        :param node: node name
        :type node: str
        """

    @abstractmethod
    def removeTargetState(self, node, name=None):
        """
        Remove target state from a system or device manager

        :param node: node
        :type node: str
        :param name: device manager name
        :type name: str
        """

    @abstractmethod
    def resetDeviceStates(self):
        """
        Sets the states of all devices to REGISTERED unless their state is
        MAINTENANCE or UNDEPLOYED.
        """

    @abstractmethod
    def resolveAlias(self, alias):
        """
        Get node name for the specified alias

        :param alias: alias
        :type alias: str
        :returns: node name
        :rtype: str
        """
        rows = self.database.query("""
            select node_name from t_sm_configuration_alias
            where alias is ?""",
            (alias,))
        if rows:
            return rows[0]["node_name"]
        else:
            None

    def toInfo(self):
        """
        Convert the information stored in the configuration into an info object

        :returns: configuration information
        :rtype: :class:`~c4.system.configuration.ConfigurationInfo`
        """
        configurationInfo = ConfigurationInfo()
        configurationInfo.aliases = self.getAliases()
        for nodeName in self.getNodeNames():
            configurationInfo.nodes[nodeName] = self.getNode(nodeName)
        configurationInfo.platform = self.getPlatform()
        return configurationInfo

class ConfigurationInfo(JSONSerializable):
    """
    System configuration information
    """
    def __init__(self):
        self.aliases = {}
        self.nodes = {}
        self.platform = PlatformInfo()

    def validate(self):
        """
        Double-checks node/device names matches dictionary keys.
        Makes sure system-manager alias exists.
        Makes sure the node that the alias points to exists.
        Makes sure there is one and only one active node.

        :raises: :class:`ConfigurationValidationError` raised when validation fails.
        """
        # double-checks node/device names matches dictionary keys
        for name, node in self.nodes.iteritems():
            if name != node.name:
                raise ConfigurationNameMismatchError(name, node.name)
            self.validateName(node)

        # make sure system-manager alias exists
        if "system-manager" not in self.aliases:
            raise ConfigurationMissingSystemManagerAliasError()

        # make sure the node that the alias points to exists
        for alias, nodeName in self.aliases.iteritems():
            if nodeName not in self.nodes:
                raise ConfigurationMissingAliasNodeError(alias, nodeName)

        # make sure there is one and only one active node
        activeNodes = [node.name for node in self.nodes.values() if node.role == Roles.ACTIVE]
        if not activeNodes:
            raise ConfigurationMissingActiveNodeError()
        if len(activeNodes) > 1:
            raise ConfigurationTooManyActiveNodesError(activeNodes)

    def validateName(self, info):
        """
        Validate that mapping key name matches info name

        :param node: info
        :type node: :class:`~c4.system.configuration.DeviceInfo` or :class:`~c4.system.configuration.NodeInfo`
        :raises: :class:`ConfigurationValidationError` raised when validation fails.
        """
        for name, childInfo in info.devices.iteritems():
            if name != childInfo.name:
                raise ConfigurationNameMismatchError(name, childInfo.name)
            self.validateName(childInfo)

class ConfigurationValidationError(Exception):
    """
    Base configuration validation error

    :param: message
    :type: str
    """
    def __init__(self, message):
        super(ConfigurationValidationError, self).__init__(message)

class ConfigurationMissingActiveNodeError(ConfigurationValidationError):
    """
    Error raised when there is no node with the 'ACTIVE' role
    """
    def __init__(self):
        super(ConfigurationMissingActiveNodeError, self).__init__("a node with role 'ACTIVE' is missing")

class ConfigurationMissingAliasNodeError(ConfigurationValidationError):
    """
    Error raised when an alias is pointing to a node that does not exist

    :param alias: alias
    :type alias: str
    :param nodeName: node name
    :type nodeName: str
    """
    def __init__(self, alias, nodeName):
        super(ConfigurationMissingAliasNodeError, self).__init__(
            "alias '{0}' points to node '{1}' that is missing".format(alias, nodeName))

class ConfigurationMissingSystemManagerAliasError(ConfigurationValidationError):
    """
    Error raised when the 'system-manager' alias is missing
    """
    def __init__(self):
        super(ConfigurationMissingSystemManagerAliasError, self).__init__("system-manager alias is missing")

class ConfigurationNameMismatchError(ConfigurationValidationError):
    """
    Error raised when an info key does not match the device or node info name

    :param keyName: key name
    :type keyName: str
    :param infoName: info name
    :type infoName: str
    """
    def __init__(self, keyName, infoName):
        super(ConfigurationNameMismatchError, self).__init__(
            "name '{0}' does not match specified node or device info name '{1}'".format(keyName, infoName))

class ConfigurationTooManyActiveNodesError(ConfigurationValidationError):
    """
    Error raised when too many roles have an 'ACTIVE' role

    :param activeNodes: node names
    :type activeNodes: [str]
    """
    def __init__(self, activeNodes):
        super(ConfigurationTooManyActiveNodesError, self).__init__(
            "too many active nodes '{0}' specified".format(" ".join(activeNodes)))

# TODO: remove or move into separate backend implementation
@ClassLogger
class DBClusterInfo(object):
    """
    A basic cluster information object backed by the database depending on the node role

    :param node: node
    :type node: str
    :param address: address of the node
    :type address: str
    :param systemManagerAddress: address of the active system manager
    :type systemManagerAddress: str
    :param role: role of the node
    :type role: :class:`Roles`
    :param state: state of the node
    :type state: :class:`States`
    """
    def __init__(self, node, address, systemManagerAddress, role=Roles.THIN, state=States.DEPLOYED):
        super(DBClusterInfo, self).__init__()
        self.node = node
        self.address = address
        self._role = multiprocessing.Value(ctypes.c_char_p, role.name)
        self._state = multiprocessing.Value(ctypes.c_char_p, state.name)
        self._systemManagerAddress = multiprocessing.Value(ctypes.c_char_p, systemManagerAddress)

    @property
    def aliases(self):
        """
        Alias mappings
        """
        if self.role == Roles.ACTIVE or self.role == Roles.PASSIVE:
            return Backend().configuration.getAliases()
        else:
            return {}

    def getNodeAddress(self, node):
        """
        Get address for specified node

        :param node: node
        :type node: str
        :returns: str or ``None`` if not found
        """
        if self.role == Roles.ACTIVE or self.role == Roles.PASSIVE:
            return Backend().configuration.getAddress(node)
        else:
            if node == "system-manager":
                return self._systemManagerAddress.value
            elif node == self.node:
                return self.address
            else:
                return None

    @property
    def nodeNames(self):
        """
        Names of the nodes in the cluster
        """
        if self.role == Roles.ACTIVE or self.role == Roles.PASSIVE:
            return Backend().configuration.getNodeNames()
        else:
            return [self.node, "system-manager"]

    @property
    def role(self):
        """
        Node role
        """
        return Roles.valueOf(self._role.value)

    @role.setter
    def role(self, role):
        if isinstance(role, Roles):
            with self._role.get_lock():
                self._role.value = role.name
        else:
            self.log.error("'%s' does not match enum of type '%s'", role, Roles)

    @property
    def state(self):
        """
        Node state
        """
        return States.valueOf(self._state.value)

    @state.setter
    def state(self, state):
        if isinstance(state, States):
            with self._state.get_lock():
                self._state.value = state.name
        else:
            self.log.error("'%s' does not match enum of type '%s'", state, States)

    @property
    def systemManagerAddress(self):
        """
        Active system manager address
        """
        return self._systemManagerAddress.value

    @systemManagerAddress.setter
    def systemManagerAddress(self, address):
        with self._systemManagerAddress.get_lock():
            self._systemManagerAddress.value = address

class DeviceInfo(JSONSerializable):
    """
    Device manager information

    :param name: name
    :type name: str
    :param deviceType: type
    :type deviceType: str
    :param state: state
    :type state: :class:`~c4.system.configuration.States`
    :param deviceId: database id
    :type deviceId: int
    :param parentId: parent database id
    :type parentId: int
    """
    def __init__(self, name, deviceType, state=States.REGISTERED, deviceId=-1, parentId=None):
        self.id = deviceId
        self.parentId = parentId
        self.name = name
        self.details = {
            "properties": {}
        }
        self.devices = {}
        self.type = deviceType
        self.state = state

    def __eq__(self, other):
        if (isinstance(other, DeviceInfo)
            and self.details == other.details
            and self.devices == other.devices
            and self.name == other.name
            and self.state == other.state
            and self.type == other.type):
            return True
        return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def addDevice(self, device):
        """
        Add child device to the device

        :param device: device
        :type device: :class:`~c4.system.configuration.DeviceInfo`
        :returns: :class:`~c4.system.configuration.DeviceInfo`
        """
        if device.name in self.devices:
            log.error("'%s' already part of '%s'", device.name, self.name)
        else:
            self.devices[device.name] = device
        return self

    @classmethod
    def fromJSONSerializable(clazz, d):
        if JSONSerializable.dictHasType(d, DeviceInfo):
            deviceInfo = DeviceInfo(d["name"], d["type"], States.REGISTERED)
            deviceInfo.devices = d.get("devices", {})
            deviceInfo.properties = d.get("properties", {})
            return deviceInfo
        return JSONSerializable.fromJSONSerializable(d)

    @property
    def properties(self):
        return self.details.get("properties", {})

    @properties.setter
    def properties(self, properties):
        self.details["properties"] = properties

    def toJSONSerializable(self, includeClassInfo=False):
        serializableDict = JSONSerializable.toJSONSerializable(self, includeClassInfo=includeClassInfo)
        # add properties
        serializableDict["properties"] = self.properties
        if not serializableDict["properties"]:
            del serializableDict["properties"]
        # remove database and transient information
        if "id" in serializableDict:
            del serializableDict["id"]
        if "parentId" in serializableDict:
            del serializableDict["parentId"]
        del serializableDict["state"]
        # remove properties from details
        del serializableDict["details"]["properties"]
        if not serializableDict["details"]:
            del serializableDict["details"]
        if not serializableDict["devices"]:
            del serializableDict["devices"]
        return serializableDict

class NodeInfo(JSONSerializable):
    """
    Node information

    :param name: name
    :type name: str
    :param address: address
    :type address: str
    :param role: role
    :type role: :class:`~c4.system.configuration.Roles`
    :param state: state
    :type state: :class:`~c4.system.configuration.States`
    :param nodeId: database id
    :type nodeId: int
    """
    def __init__(self, name, address, role=Roles.THIN, state=States.DEPLOYED, nodeId=-1):
        self.id = nodeId
        self.name = name
        self.details = {
            "address": address,
            "role": role.name
        }
        self.devices = {}
        self.state = state

    def __eq__(self, other):
        if (isinstance(other, NodeInfo)
            and self.name == other.name
            and self.details == other.details
            and self.devices == other.devices
            and self.state == other.state):
            return True
        return False

    def __ne__(self, other):
        return not self.__eq__(other)

    @property
    def address(self):
        return self.details["address"]

    @address.setter
    def address(self, address):
        self.details["address"] = address

    def addDevice(self, device):
        """
        Add device to the node

        :param device: device
        :type device: :class:`~c4.system.configuration.DeviceInfo`
        :returns: :class:`~c4.system.configuration.NodeInfo`
        """
        if device.name in self.devices:
            log.error("'%s' already part of '%s'", device.name, self.name)
        else:
            self.devices[device.name] = device
        return self

    @classmethod
    def fromJSONSerializable(clazz, d):
        if JSONSerializable.dictHasType(d, NodeInfo):
            nodeInfo = NodeInfo(d["name"],
                                d["address"],
                                role=d.get("role", Roles.THIN),
                                state=d.get("state", States.DEPLOYED))
            nodeInfo.devices = d.get("devices", {})
            return nodeInfo
        return JSONSerializable.fromJSONSerializable(d)

    # FIXME: check if necessary
    def isDeviceRunning(self, deviceName):
        """
        Returns True if the given device is started, False otherwise
        """
        if deviceName in self.devices:
            deviceInfo = self.devices[deviceName]
            if deviceInfo.state == States.RUNNING:
                return True
        return False

    @property
    def role(self):
        return Roles.valueOf(self.details["role"])

    @role.setter
    def role(self, role):
        if isinstance(role, Roles):
            self.details["role"] = role.name
        else:
            self.log.error("'%s' does not match enum of type '%s'", role, Roles)

    def toJSONSerializable(self, includeClassInfo=False):
        serializableDict = JSONSerializable.toJSONSerializable(self, includeClassInfo=includeClassInfo)
        # add properties
        serializableDict["address"] = self.address
        serializableDict["role"] = self.role
        # remove database and transient information
        if "id" in serializableDict:
            del serializableDict["id"]
        # TODO: check where needed
#         del serializableDict["state"]
        # remove properties from details
        del serializableDict["details"]["address"]
        del serializableDict["details"]["role"]
        if not serializableDict["details"]:
            del serializableDict["details"]
        if not serializableDict["devices"]:
            del serializableDict["devices"]
        return serializableDict

class PlatformInfo(JSONSerializable):
    """
    Platform information

    :param name: name
    :type name: str
    :param platformType: type
    :type platformType: str
    :param description: description
    :type description: str
    :param settings: settings
    :type settings: dict
    """
    def __init__(self, name="unknown", platformType="c4.system.platforms.Unknown", description="", settings=None):
        self.name = name
        self.type = platformType
        self.description = description
        self.settings = settings or {}

    def __eq__(self, other):
        if (isinstance(other, PlatformInfo)
            and self.name == other.name
            and self.description == other.description
            and self.settings == other.settings
            and self.type == other.type):
            return True
        return False

    def __ne__(self, other):
        return not self.__eq__(other)

# TODO: need to make ClusterInfo more abstract and then have the specific implementation inherit properly
@ClassLogger
class SharedClusterInfo(object):
    """
    A basic cluster information object backed by a shared configuration

    :param backend: backend implementation
    :type backend: :class:`~BackendImplementation`
    :param node: node
    :type node: str
    :param address: address of the node
    :type address: str
    :param systemManagerAddress: address of the active system manager
    :type systemManagerAddress: str
    :param role: role of the node
    :type role: :class:`Roles`
    :param state: state of the node
    :type state: :class:`States`
    """
    def __init__(self, backend, node, address, systemManagerAddress, role, state):
        super(SharedClusterInfo, self).__init__()
        self.backend = backend
        self.node = node
        self.address = address
        self._role = multiprocessing.Value(ctypes.c_char_p, role.name)
        self._state = multiprocessing.Value(ctypes.c_char_p, state.name)
        self._systemManagerAddress = multiprocessing.Value(ctypes.c_char_p, systemManagerAddress)

    @property
    def aliases(self):
        """
        Alias mappings
        """
        return self.backend.configuration.getAliases()

    def getNodeAddress(self, node):
        """
        Get address for specified node

        :param node: node
        :type node: str
        :returns: str or ``None`` if not found
        """
        return self.backend.configuration.getAddress(node)

    @property
    def nodeNames(self):
        """
        Names of the nodes in the cluster
        """
        return self.backend.configuration.getNodeNames()

    @property
    def role(self):
        """
        Node role
        """
        return Roles.valueOf(self._role.value)

    @role.setter
    def role(self, role):
        if isinstance(role, Roles):
            with self._role.get_lock():
                self._role.value = role.name
        else:
            self.log.error("'%s' does not match enum of type '%s'", role, Roles)

    @property
    def state(self):
        """
        Node state
        """
        return States.valueOf(self._state.value)

    @state.setter
    def state(self, state):
        if isinstance(state, States):
            with self._state.get_lock():
                self._state.value = state.name
        else:
            self.log.error("'%s' does not match enum of type '%s'", state, States)

    @property
    def systemManagerAddress(self):
        """
        Active system manager address
        """
        return self._systemManagerAddress.value

    @systemManagerAddress.setter
    def systemManagerAddress(self, address):
        with self._systemManagerAddress.get_lock():
            self._systemManagerAddress.value = address
