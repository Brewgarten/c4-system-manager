"""
etcd based backend implementation
"""
import re

import etcd3

from c4.system.backend import BackendImplementation
from c4.system.configuration import (Configuration,
                                     DeviceInfo,
                                     NodeInfo,
                                     PlatformInfo,
                                     Roles,
                                     SharedClusterInfo,
                                     States)
from c4.utils.logutil import ClassLogger
from c4.utils.jsonutil import JSONSerializable


class EtcdBackend(BackendImplementation):
    """
    etcd backend implementation

    :param info: backend info
    :type info: :class:`~BackendInfo`
    """
    def __init__(self, info):
        super(EtcdBackend, self).__init__(info)

    @property
    def client(self):
        """
        etcd client instance
        """
        return etcd3.Etcd3Client(
            host=self.info.properties.get("client.host", "localhost"),
            port=self.info.properties.get("client.port", 2379),
            ca_cert=self.info.properties.get("security.ca.path"),
            cert_key=self.info.properties.get("security.key.path"),
            cert_cert=self.info.properties.get("security.certificate.path"),
            timeout=self.info.properties.get("client.timeout", None)
        )

    @property
    def configuration(self):
        """
        etcd based configuration instance
        """
        return EtcdConfiguration(self.client)

    def ClusterInfo(self, node, address, systemManagerAddress, role, state):
        return SharedClusterInfo(self, node, address, systemManagerAddress, role, state)

@ClassLogger
class EtcdConfiguration(Configuration):
    """
    etcd backend configuration implementation

    :param client: etcd client
    :type client: :class:`~etcd3.Etcd3Client`
    """
    PLATFORM = "/platform"
    PLATFORM_DESCRIPTION = "/platform/description"
    PLATFORM_SETTINGS = "/platform/settings"
    PLATFORM_TYPE = "/platform/type"

    def __init__(self, client):
        self.client = client

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
        # check if node exists
        nodeInfo = self.getNode(node, includeDevices=False)
        if nodeInfo is None:
            self.log.error("could not add alias '%s' because node '%s' does not exist", alias, node)
            return None
        # check if we are already aliased
        existingAliasedNode = self.resolveAlias(alias)
        if existingAliasedNode == node:
            self.log.error("'%s' is already an alias", alias)
            return None
        # add alias for the node
        aliasKey = "/aliases/{alias}".format(alias=alias)
        self.client.put(aliasKey, node)
        return alias

    def addDevice(self, node, fullDeviceName, device):
        """
        Adds a device to the configuration db. Throws exception on error.

        :param node: node name
        :type node: str
        :param fullDeviceName: fully qualified device name
        :type fullDeviceName: str
        :param device: device
        :type device: :class:`~c4.system.configuration.DeviceInfo`
        :returns: device info with database ids
        :rtype: :class:`~c4.system.configuration.DeviceInfo`
        """
        nodeInfo = self.getNode(node)
        if nodeInfo is None:
            self.log.error("could not add device '%s' because node '%s' does not exist", fullDeviceName, node)
            return None

        deviceParts = fullDeviceName.split(".")
        # delete the last part of the name because that is the device that we want to add
        deviceParts.pop()
        # go through the hierarchy
        existingDevices = nodeInfo.devices
        for devicePart in deviceParts:

            if devicePart not in existingDevices:
                self.log.error("unable to add device because device parent '%s' not found for node '%s'", devicePart, node)
                return None
            existingDevices = existingDevices[devicePart].devices

        if device.name in existingDevices:
            self.log.error("unable to add device because device '%s' already exists for node '%s'", device.name, node)
            return None

        # add to the node
        existingDevices[device.name] = device

        parentPath = "".join(["/{devicePart}/devices".format(devicePart=devicePart) for devicePart in deviceParts])
        deviceKey = "/nodes/{nodeName}/devices{parentPath}/{deviceName}".format(nodeName=node, parentPath=parentPath, deviceName=device.name)
        propertiesKey = "{deviceKey}/properties".format(deviceKey=deviceKey)
        stateKey = "{deviceKey}/state".format(deviceKey=deviceKey)
        typeKey = "{deviceKey}/type".format(deviceKey=deviceKey)

        transaction = EtcdTransaction(self.client)
        transaction.put(deviceKey, device.name)
        transaction.put(stateKey, serialize(device.state))
        transaction.put(typeKey, device.type)
        for key, value in device.properties.items():
            propertyKey = "{propertiesKey}/{key}".format(propertiesKey=propertiesKey, key=key)
            transaction.put(propertyKey, serialize(value))
        transaction.commit()

        # save child devices
        childDevices = device.devices.values()
        for childDevice in childDevices:
            deviceInfo = self.addDevice(node, "{0}.{1}".format(fullDeviceName, childDevice.name), childDevice)
            if deviceInfo:
                device.devices[childDevice.name] = deviceInfo

        return device

    def addNode(self, node):
        """
        Add node

        :param node: node
        :type node: :class:`~c4.system.configuration.NodeInfo`
        :returns: node info with database ids
        :rtype: :class:`~c4.system.configuration.NodeInfo`
        """
        nodeInfo = self.getNode(node.name, includeDevices=False)
        if nodeInfo:
            self.log.error("node '%s' already exists", node.name)
            return None

        nodeKey = "/nodes/{name}".format(name=node.name)
        propertiesKey = "{nodeKey}/properties".format(nodeKey=nodeKey)
        roleKey = "{nodeKey}/role".format(nodeKey=nodeKey)
        stateKey = "{nodeKey}/state".format(nodeKey=nodeKey)
        typeKey = "{nodeKey}/type".format(nodeKey=nodeKey)

        transaction = EtcdTransaction(self.client)
        transaction.put(nodeKey, node.name)
        transaction.put(roleKey, serialize(node.role))
        transaction.put(stateKey, serialize(node.state))
        transaction.put(typeKey, "c4.system.manager.SystemManager")
        for key, value in node.properties.items():
            propertyKey = "{propertiesKey}/{key}".format(propertiesKey=propertiesKey, key=key)
            transaction.put(propertyKey, serialize(value))
        transaction.commit()

        nodeInfo = self.getNode(node.name, includeDevices=False)

        # save child devices
        childDevices = node.devices.values()
        for childDevice in childDevices:
            deviceInfo = self.addDevice(node.name, childDevice.name, childDevice)
            if deviceInfo:
                nodeInfo.devices[childDevice.name] = deviceInfo

        return nodeInfo

    def addPlatform(self, platform):
        """
        Add platform information

        :param platform: platform
        :type platform: :class:`~c4.system.configuration.PlatformInfo`
        """
        # TODO: combine into single transaction once bindings support delete_prefix in transactions
        platformPrefix = "{platformKey}/".format(platformKey=self.PLATFORM)
        self.client.delete_prefix(platformPrefix)

        transaction = EtcdTransaction(self.client)
        transaction.put(self.PLATFORM, platform.name)
        transaction.put(self.PLATFORM_TYPE, platform.type)
        transaction.put(self.PLATFORM_DESCRIPTION, platform.description)
        for key, value in platform.settings.items():
            settingKey = "{settingsKey}/{key}".format(settingsKey=self.PLATFORM_SETTINGS, key=key)
            transaction.put(settingKey, serialize(value))
        transaction.commit()

    def clear(self):
        """
        Removes all nodes and devices from the configuration object and the database.
        """
        self.client.delete_prefix("/")

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
        nodeKey = self.getKey(node, None)
        nodeName, _ = self.client.get(nodeKey)
        if not nodeName:
            self.log.error("could not change alias '%s' because node '%s' does not exist", alias, node)
            return None

        aliasKey = "/aliases/{alias}".format(alias=alias)
        # check if the alias exists
        compare = [
            etcd3.transactions.Version(aliasKey) > 0
        ]
        # set new value
        success = [
            etcd3.transactions.Put(aliasKey, node)
        ]

        succeeded, _ = self.client.transaction(
            compare=compare,
            success=success,
            failure=[]
        )
        if not succeeded:
            self.log.error("alias '%s' does not exist", alias)
            return None

        return alias

    def changeProperty(self, node, name, propertyName, value, setIfNotExist=False):
        """
        Change property property of a system or device manager to the specified value

        :param node: node
        :type node: str
        :param name: device manager name
        :type name: str
        :param propertyName: property name
        :type propertyName: str
        :param value: property value
        :type value: str
        :returns: previous value
        """
        key = self.getKey(node, name)
        keyName, _ = self.client.get(key)
        if not keyName:
            return None

        propertyKey = self.getKey(node, name, "properties", propertyName)
        serializedValue = serialize(value)

        # check if the key exists
        compare = [
            etcd3.transactions.Version(propertyKey) > 0
        ]
        # get previous value and set new value
        success = [
            etcd3.transactions.Get(propertyKey),
            etcd3.transactions.Put(propertyKey, serializedValue)
        ]
        # just set the new value
        failure = [
            etcd3.transactions.Put(propertyKey, serializedValue)
        ]

        if setIfNotExist:
            succeeded, responses = self.client.transaction(
                compare=compare,
                success=success,
                failure=failure
            )
            if not succeeded:
                # just set a value that did not exist so no previous value
                return None
        else:
            succeeded, responses = self.client.transaction(
                compare=compare,
                success=success,
                failure=[]
            )
            if not succeeded:
                self.log.error("property '%s' of '%s/%s' does not exist", propertyName, node, name)
                return None

        # get previous value from the the Get response of the transaction
        previousValue, _ = responses[0][0]
        return deserialize(previousValue)

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

        key = self.getKey(node, None, "role")
        serializedValue = serialize(role)

        # check if the key exists
        compare = [
            etcd3.transactions.Version(key) > 0
        ]
        # get previous value and set new value
        success = [
            etcd3.transactions.Get(key),
            etcd3.transactions.Put(key, serializedValue)
        ]
        # just set the new value
        failure = [
            etcd3.transactions.Put(key, serializedValue)
        ]

        succeeded, responses = self.client.transaction(
            compare=compare,
            success=success,
            failure=failure
        )
        if not succeeded:
            # just set a value that did not exist so no previous value
            return None

        # get previous value from the the Get response of the transaction
        previousValue, _ = responses[0][0]
        return deserialize(previousValue)

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
        if not isinstance(state, States):
            self.log.error("'%s' does not match enum of type '%s'", state, States)
            return None

        stateKey = self.getKey(node, name, "state")
        serializedState = serialize(state)

        # check if the state stateKey exists
        compare = [
            etcd3.transactions.Version(stateKey) > 0
        ]
        # get previous state and set new state
        success = [
            etcd3.transactions.Get(stateKey),
            etcd3.transactions.Put(stateKey, serializedState)
        ]
        succeeded, responses = self.client.transaction(
            compare=compare,
            success=success,
            failure=[]
        )
        if not succeeded:
            self.log.error("could not change state of '%s%s' to '%s' because it does not exist", node, "/" + name if name else "", state)
            return None

        # get previous state from the the Get response of the transaction
        previousValue, _ = responses[0][0]

        # check if we are dealing with a node and expected to set a special state
        if not name and (state == States.REGISTERED or state == States.MAINTENANCE):
            transaction = EtcdTransaction(self.client)
            devicesPrefix = self.getKey(node, None, "devices", "")
            for value, metadata in self.client.get_prefix(devicesPrefix):
                if metadata.key.endswith("/state") and deserialize(value) != States.MAINTENANCE:
                    transaction.put(metadata.key, serializedState)
            transaction.commit()

        return deserialize(previousValue)

    def getAliases(self):
        """
        Get a mapping of aliases to node names

        :returns: mappings
        :rtype: dict
        """
        aliasesPrefix = "/aliases/"
        # note that key is the alias and value is the node name
        return {
            metadata.key.replace(aliasesPrefix, ""): value
            for value, metadata in self.client.get_prefix(aliasesPrefix)
        }

    def getKey(self, node, name, *additionalParts):
        """
        Assemble an etcd key based on node, device and property names

        :param node: node
        :type node: str
        :param name: device manager name
        :type name: str
        :returns: key
        :rtype: str
        """
        keyParts = ["/nodes", node]
        if name:
            for namePart in name.split("."):
                keyParts.append("devices")
                keyParts.append(namePart)
        keyParts.extend(additionalParts)
        return "/".join(keyParts)

    def getPlatform(self):
        """
        Get platform information

        :returns: platform
        :rtype: :class:`~c4.system.configuration.PlatformInfo`
        """
        platformName, _ = self.client.get(self.PLATFORM)
        platformPrefix = "{platformKey}/".format(platformKey=self.PLATFORM)
        # map from key to value and deserialize value automatically
        platform = {
            metadata.key : deserialize(value)
            for value, metadata in self.client.get_prefix(platformPrefix)
        }

        # filter out settings
        platformSettingsPrefix = "{settingsKey}/".format(settingsKey=self.PLATFORM_SETTINGS)
        settings = {
            key.replace(platformSettingsPrefix, ""): value
            for key, value in platform.items()
            if key.startswith(platformSettingsPrefix)
        }
        return PlatformInfo(
            name=platformName or "unknown",
            platformType=platform.get(self.PLATFORM_TYPE, "c4.system.platforms.Unknown"),
            description=platform.get(self.PLATFORM_DESCRIPTION, ""),
            settings=settings
        )

    def getProperty(self, node, name, propertyName):
        """
        Get the property of a system or device manager.

        :param node: node
        :type node: str
        :param name: device manager name
        :type name: str
        :param propertyName: property name
        :type propertyName: str
        :returns: str
        """
        propertyKey = self.getKey(node, name, "properties", propertyName)
        value, _ = self.client.get(propertyKey)
        if value is None:
            self.log.error("could not get property because '%s%s' does not exist", node, "/" + name if name else "")
            return None
        return deserialize(value)

    def getProperties(self, node, name=None):
        """
        Get the properties of a system or device manager.

        :param node: node
        :type node: str
        :param name: device manager name
        :type name: str
        :returns: properties or ``None`` if node or device does not exist
        :rtype: dict
        """
        key = self.getKey(node, name)
        value, _ = self.client.get(key)
        if value is None:
            self.log.error("could not get property because '%s%s' does not exist", node, "/" + name if name else "")
            return None

        propertiesPrefix = self.getKey(node, name, "properties/")
        # map from key to value and deserialize value automatically
        properties = {
            metadata.key.replace(propertiesPrefix, "") : deserialize(value)
            for value, metadata in self.client.get_prefix(propertiesPrefix)
        }
        return properties

    def getRole(self, node):
        """
        Get the role of a system manager.

        :param node: node
        :type node: str
        :returns: role
        :rtype: :class:`Roles`
        """
        roleKey = self.getKey(node, None, "role")
        value, _ = self.client.get(roleKey)
        if value is None:
            self.log.error("could not get role because '%s' does not exist", node)
            return None
        return deserialize(value)

    def getState(self, node, name=None):
        """
        Get the state of a system or device manager.

        :param node: node
        :type node: str
        :param name: device manager name
        :type name: str
        :returns: :class:`~c4.system.configuration.States`
        """
        stateKey = self.getKey(node, name, "state")
        value, _ = self.client.get(stateKey)
        if value is None:
            self.log.error("could not get state because '%s%s' does not exist", node, "/" + name if name else "")
            return None
        return deserialize(value)

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
        nodeKey = self.getKey(node, None)
        nodeName, _ = self.client.get(nodeKey)
        if not nodeName:
            return None

        nodePrefix = nodeKey + "/"
        # map from key to value and deserialize value automatically
        nodeInfoMapping = {
            metadata.key : deserialize(value)
            for value, metadata in self.client.get_prefix(nodePrefix)
        }

        # deal with node information
        propertiesKey = "{nodeKey}/properties/".format(nodeKey=nodeKey)
        roleKey = "{nodeKey}/role".format(nodeKey=nodeKey)
        stateKey = "{nodeKey}/state".format(nodeKey=nodeKey)
        nodeProperties = {
            key.replace(propertiesKey, ""): value
            for key, value in nodeInfoMapping.items()
            if key.startswith(propertiesKey)
        }

        nodeInfo = NodeInfo(node, nodeProperties["address"], role=nodeInfoMapping[roleKey], state=nodeInfoMapping[stateKey])
        nodeInfo.properties = nodeProperties

        if includeDevices:

            def getDevices(parentKey):
                """
                Get devices based on parent key and the already retrieved values
                """
                devices = {}
                deviceKeyExpression = re.compile(r"(?P<deviceKey>{parentKey}/devices/[^/]+)$".format(parentKey=parentKey))
                for key in nodeInfoMapping.keys():
                    match = deviceKeyExpression.match(key)
                    if match:
                        deviceKey = match.group("deviceKey")
                        propertiesKey = "{devicesKey}/properties/".format(devicesKey=deviceKey)
                        stateKey = "{devicesKey}/state".format(devicesKey=deviceKey)
                        typeKey = "{devicesKey}/type".format(devicesKey=deviceKey)
                        deviceProperties = {
                            key.replace(propertiesKey, ""): value
                            for key, value in nodeInfoMapping.items()
                            if key.startswith(propertiesKey)
                        }

                        deviceInfo = DeviceInfo(nodeInfoMapping[deviceKey], nodeInfoMapping[typeKey], nodeInfoMapping[stateKey])
                        deviceInfo.properties = deviceProperties
                        deviceInfo.devices = getDevices(deviceKey)
                        devices[deviceInfo.name] = deviceInfo
                return devices

            nodeInfo.devices = getDevices(nodeKey)

            if flatDeviceHierarchy:

                def getFlatDeviceHierarchy(devices):
                    """
                    Get a flattened mapping of devices
                    """
                    flattenedDevices = {}
                    for name, device in devices.items():
                        flattenedDevices[name] = device
                        for childName, childDevice in getFlatDeviceHierarchy(device.devices).items():
                            flattenedDevices["{parent}.{child}".format(parent=name, child=childName)] = childDevice
                    return flattenedDevices

                nodeInfo.devices = getFlatDeviceHierarchy(nodeInfo.devices)

        return nodeInfo

    def getNodeNames(self):
        """
        Return a list of node names.

        :returns: list of node names
        :rtype: list
        """
        nodesKey = "/nodes/"
        nodeNameExpression = re.compile(r"/nodes/[^/]+$")
        return [
            value
            for value, metadata in self.client.get_prefix(nodesKey)
            if nodeNameExpression.match(metadata.key)
        ]

    def removeDevice(self, node, fullDeviceName):
        """
        Remove a device from the configuration

        :param node: node name
        :type node: str
        :param fullDeviceName: fully qualified device name
        :type fullDeviceName: str
        """
        deviceKey = self.getKey(node, fullDeviceName)

        # TODO: change to transaction once the bindings support delete_prefix
        value, _ = self.client.get(deviceKey)
        if value:
            self.client.delete(deviceKey)
            self.client.delete_prefix(deviceKey + "/")
        else:
            self.log.error("could not remove '%s' from '%s' because it does not exist", fullDeviceName, node)

    def removeNode(self, node):
        """
        Remove node from the configuration

        :param node: node name
        :type node: str
        """
        nodeKey = self.getKey(node, None)

        # TODO: change to transaction once the bindings support delete_prefix
        value, _ = self.client.get(nodeKey)
        if value:
            self.client.delete(nodeKey)
            self.client.delete_prefix(nodeKey + "/")

            # remove aliases for node
            for alias, nodeName in self.getAliases().items():
                if nodeName == node:
                    self.client.delete("/aliases/{alias}".format(alias=alias))

        else:
            self.log.error("could not remove '%s' because it does not exist", node)

    def removeProperty(self, node, name, propertyName):
        """
        Remove property property from a system or device manager

        :param node: node
        :type node: str
        :param name: device manager name
        :type name: str
        :param property: property
        :type property: str
        """
        key = self.getKey(node, name, "properties", propertyName)

        # check if the key exists
        compare = [
            etcd3.transactions.Version(key) > 0
        ]
        # remove value
        success = [
            etcd3.transactions.Delete(key)
        ]

        succeeded, _ = self.client.transaction(
            compare=compare,
            success=success,
            failure=[]
        )
        if not succeeded:
            self.log.error("could not remove '%s' from '%s/%s' because it property does not exist", propertyName, node, name)

    def resetDeviceStates(self):
        """
        Sets the states of all devices to REGISTERED unless their state is
        MAINTENANCE or UNDEPLOYED.
        """
        serializedState = serialize(States.REGISTERED)
        deviceStateKeyExpression = re.compile(r".*/devices/[^/]+/state")
        ignoreStates = {States.MAINTENANCE, States.REGISTERED, States.UNDEPLOYED}
        transaction = EtcdTransaction(self.client)
        for value, metadata in self.client.get_all():
            if deviceStateKeyExpression.match(metadata.key) and deserialize(value) not in ignoreStates:
                transaction.put(metadata.key, serializedState)
        transaction.commit()

    def resolveAlias(self, alias):
        """
        Get node name for the specified alias

        :param alias: alias
        :type alias: str
        :returns: node name
        :rtype: str
        """
        aliasKey = "/aliases/{alias}".format(alias=alias)
        value, _ = self.client.get(aliasKey)
        return value

class EtcdValue(JSONSerializable):
    """
    Wrapper around non-string values to be stored in etcd

    :param value: value
    """
    def __init__(self, value):
        self.value = value

@ClassLogger
class EtcdTransaction(object):
    """
    ectd transaction that does not require a comparison but simply behaves like
    a regular database transaction that automatically begins, wrapping multiple
    statements and then commits

    :param client: etcd client
    :type client: :class:`~etcd3.Etcd3Client`
    :param statements: statements
    :type statements: [:class:`~etcd3.transactions.Delete` or etcd3.transactions.Put]
    """
    def __init__(self, client, statements=None):
        self.client = client
        self.statements = statements if statements else []

    def commit(self):
        """
        Commit all non-committed statements
        """
        if not self.statements:
            self.log.warn("No statements to commit")
            return
        # note that since a comparison is required we compare non existing things
        # and execute the statements as part of the comparison failure branch
        self.client.transaction(
            compare=[etcd3.transactions.Value("__invalid__") == "__invalid__"],
            success=[],
            failure=self.statements
        )
        del self.statements[:]

    def delete(self, key):
        """
        Add a delete statement to the transaction to delete the specified key

        :param key: key
        :type key: str
        """
        self.statements.append(etcd3.transactions.Delete(key))

    def put(self, key, value, lease=None):
        """
        Add a put statement to the transaction to put the value at the specified key

        :param key: key
        :type key: str
        :param value: value
        :type value: str
        """
        self.statements.append(etcd3.transactions.Put(key, value, lease=lease))

def deserialize(value):
    """
    Deserialize a value retrieved from etcd

    - JSONSerializable values get converted into their respective value
    - EtcdValue wrapped values get extracted
    - everything else is treated as a string

    :param value: etcd value
    :type value: str
    :returns: deserialized value
    """
    if JSONSerializable.classAttribute in value:
        deserialized = JSONSerializable.fromJSON(value)
        if isinstance(deserialized, EtcdValue):
            return deserialized.value
        return deserialized
    return value

def serialize(value):
    """
    Serialize a value so that it can be stored properly in etcd without losing
    its type

    - JSONSerializable values get converted into json strings
    - strings stay strings
    - everything else gets wrapped in an EtcdValue

    :param value: value
    :returns: serialized value
    """
    if isinstance(value, JSONSerializable):
        return value.toJSON(includeClassInfo=True)
    elif isinstance(value, (str, unicode)):
        return value
    return EtcdValue(value).toJSON(includeClassInfo=True)
