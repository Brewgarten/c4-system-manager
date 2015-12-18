import ctypes
import logging
import multiprocessing
import json
import gpfs
import sqlite3
import traceback

from c4.utils.enum import Enum
from c4.utils.jsonutil import JSONSerializable
from c4.utils.logutil import ClassLogger
from c4.system.db import DBManager

log = logging.getLogger(__name__)

class GPFSRoles(Enum):
    """
    Enumeration of GPFS roles
    """
    CLIENT = "client"
    MANAGER = "manager"
    NON_QUORUM = "nonquorum"
    QUORUM = "quorum"
    QUORUM_MANAGER = "quorum-manager"

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
    STOPPED = "stopped"
    DOWN = "down"

    RUNNING = "running"
    FAILED = "failed"

    UNKNOWN = "unknown"

@ClassLogger
class Configuration():
    """
    System configuration interface
    """
    def __init__(self):
        self.database = DBManager()

    @staticmethod
    def _dbGetRole(details):
        nodeDetails = json.loads(details)
        return nodeDetails.get('role')

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
        rows = self.database.query("""
            select name from t_sm_configuration
            where parent_id is null and name is ?""", (node,))
        if not rows:
            self.log.error("could not add alias '%s' because node '%s' does not exist", alias, node)
            return None
        # attempt to add alias for the node
        inserted = self.database.writeCommit("""
            insert into t_sm_configuration_alias (alias, node_name) values (?, ?)""",
            (alias, node))
        if inserted < 1:
            self.log.error("'%s' is already an alias", alias)
            return None
        return alias

    def addDevice(self, node, fullDeviceName, device):
        """
        Adds a device to the configuration db. Throws exception on error.

        :param node: node name
        :type node: str
        :param fullDeviceName: fully qualified device name
        :type fullDeviceName: str
        :param device: device
        :type device: :class:`~dynamite.system.configuration.DeviceInfo`
        :returns: device info with database ids
        :rtype: :class:`~dynamite.system.configuration.DeviceInfo`

        """
        nodeInfo = self.getNode(node)
        if nodeInfo is None:
            self.log.error("could not add device '%s' because node '%s' does not exist", fullDeviceName, node)
            return None

        deviceParts = fullDeviceName.split(".")
        # delete the last part of the name because that is the device that we want to add
        deviceParts.pop()
        # go through the hierarchy and get each device id
        # we want the last id of the hierarchy
        parentId = nodeInfo.id
        existingDevices = nodeInfo.devices
        for devicePart in deviceParts:

            if devicePart not in existingDevices:
                self.log.error("unable to add device because device parent '%s' not found for node '%s'", devicePart, node)
                return None
            parentId = existingDevices[devicePart].id
            existingDevices = existingDevices[devicePart].devices

        if device.name in existingDevices:
            self.log.error("unable to add device because device '%s' already exists for node '%s'", device.name, node)
            return None

        self.database.writeCommit("""
            insert into t_sm_configuration (parent_id, name, state, type, details)
            values (?, ?, ?, ?, ?)""",
            (parentId, device.name, device.state.name, device.type, json.dumps(device.details)))
        # get the id of the last configuration record
        rows = self.database.query("select id from t_sm_configuration where parent_id is ? and name = ?",
            (parentId, device.name))
        device.id = rows[0]["id"]
        device.parentId = parentId

        # save child devices
        childDevices = device.devices.values()
        for childDevice in childDevices:
            dbDevice = self.addDevice(node, "{0}.{1}".format(fullDeviceName, childDevice.name), childDevice)
            if dbDevice:
                device.devices[childDevice.name] = dbDevice

        return device

    def addNode(self, node):
        """
        Add node

        :param node: node
        :type node: :class:`~dynamite.system.configuration.NodeInfo`
        :returns: node info with database ids
        :rtype: :class:`~dynamite.system.configuration.NodeInfo`
        """
        nodeInfo = self.getNode(node.name, includeDevices=False)
        if nodeInfo:
            self.log.error("node '%s' already exists", node.name)
            return None

        self.database.writeCommit("""
            insert into t_sm_configuration (name, state, type, details)
            values (?, ?, ?, ?)""",
            (node.name, node.state.name, "dynamite.system.manager.SystemManager", json.dumps(node.details)))
        nodeInfo = self.getNode(node.name, includeDevices=False)

        # save child devices
        childDevices = node.devices.values()
        for childDevice in childDevices:
            dbDevice = self.addDevice(node.name, childDevice.name, childDevice)
            if dbDevice:
                nodeInfo.devices[childDevice.name] = dbDevice

        return nodeInfo

    def addPlatform(self, platform):
        """
        Add platform information

        :param platform: platform
        :type platform: :class:`~dynamite.system.configuration.PlatformInfo`
        """
        self.database.write("begin")
        self.database.write("delete from t_sm_platform")
        self.database.writeMany("""
            insert into t_sm_platform (property, value) values (?, ?)""",
            ("name", platform.name),
            ("type", platform.type),
            ("description", platform.description),
            ("settings", json.dumps(platform.settings)),
            ("storage", json.dumps(platform.storage))
        )
        self.database.write("commit")

    def clear(self, node=None):
        """
        Removes all nodes and devices from the configuration object and the database
        if no node is Give, else clears information only for that node
        """
        if node:
            with self.database.connection as txn:
                node = self.getNode(node, True, True)
                if node is not None:
                    ids = [node.id] + [node.devices[d].id for d in node.devices]
                    binding = ','.join('?' * len(ids))
                    self.database.write("delete from t_sm_configuration where id in ({0})".format(binding), ids)
        else:
            self.database.write("begin")
            self.database.write("delete from t_sm_configuration")
            self.database.write("delete from t_sm_configuration_alias")
            self.database.write("delete from t_sm_platform")
            self.database.write("commit")

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
        # check if node exists
        rows = self.database.query("""
            select name from t_sm_configuration
            where parent_id is null and name is ?""", (node,))
        if not rows:
            self.log.error("could not change alias '%s' because node '%s' does not exist", alias, node)
            return None
        # attempt to change alias to the node
        updated = self.database.writeCommit("update t_sm_configuration_alias set node_name=? where alias=?", (node, alias))
        if updated < 1:
            self.log.error("alias '%s' does not exist", alias)
            return None
        return alias

    def changeDetail(self, node, name, detail, value, setIfNotExist=False):
        """
        Change detail property of a system or device manager to the specified value

        :param node: node
        :type node: str
        :param name: device manager name
        :type name: str
        :param detail: detail property
        :type detail: str
        :param value: detail value
        :type value: str
        :returns: previous value
        """
        rowId, details = self.getDetails(node, name)
        if rowId < 0:
            return None
        if detail not in details and not setIfNotExist:
            self.log.error("detail '%s' of '%s/%s' does not exist", detail, node, name)
            return None
        previousValue = details.get(detail, None)
        details[detail] = value
        updated = self.database.writeCommit("update t_sm_configuration set details = ? where id is ?", (json.dumps(details), rowId))
        if updated < 1:
            self.log.error("could not change detail '%s' of '%s/%s' to '%s'", detail, node, name, value)
            return None
        return previousValue

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
        try:
            if name:
                deviceInfo = self.getDevice(node, name)

                if deviceInfo is None:
                    self.log.error("could not change state of '%s/%s' to '%s' because it does not exist", node, name, state)
                    return None

                previousState = deviceInfo.state
                self.database.writeCommit("""
                    update t_sm_configuration set state = ? where id is ?""",
                    (state.name, deviceInfo.id))

            else:
                if state == States.REGISTERED or state == States.MAINTENANCE:
                    nodeInfo = self.getNode(node, flatDeviceHierarchy=True)
                else:
                    nodeInfo = self.getNode(node, includeDevices=False)

                if nodeInfo is None:
                    self.log.error("could not change state of '%s' to '%s' because node does not exist", node, state)
                    return None

                previousState = nodeInfo.state
                self.database.writeCommit("""
                    update t_sm_configuration set state = ? where id is ?""",
                    (state.name, nodeInfo.id))

                # handle special cases
                if state == States.REGISTERED or state == States.MAINTENANCE:

                    deviceList = sorted(nodeInfo.devices.values())
                    # note that we do not automatically change the state of children if they are in Maintenance mode
                    self.database.writeMany("""
                        update t_sm_configuration set state = ? where id is ? and state is not 'MAINTENANCE'""",
                        *[(state.name, device.id) for device in deviceList])

            self.log.debug("changed state of '%s%s' from '%s' to '%s'", node, "/" + name if name else "", previousState, state.name)
            return previousState

        except Exception as e:
            self.log.error("could not change state of '%s%s' to '%s': %s", node, "/" + name if name else "", state, e)
            return None

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
        if not isinstance(state, States):
            self.log.error("'%s' does not match enum of type '%s'", state, States)
            return
        stateName = self.changeDetail(node, name, "targetState", state.name, setIfNotExist=True)
        if not stateName:
            return None
        return States.valueOf(stateName)

    def getNodeByRole(self, *roles):
        ret = []
        for node in self.getNodeNames():
            node = self.getNode(node, includeDevices=False)
            if node and (not roles or node.role in roles):
                ret.append(node)

        return ret

    @property
    def systemManagerNode(self):
        nodes = self.getNodeByRole(Roles.ACTIVE)
        if len(nodes) > 1:
            self.log.error("%d nodes with role ACTIVE : %s", len(nodes), nodes)
        elif len(nodes) == 0:
            self.log.warning("No nodes with ACTIVE role")
        else:
            return nodes[0]

        return None

    @property
    def systemManagerAddress(self):
        node = self.systemManagerNode
        return node.address if node is not None else None

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
        info = self.systemManagerNode if node == 'system-manager' else self.getNode(node, includeDevices=False)
        return info.address if info is not None else None

    def getAliases(self):
        """
        Get a mapping of aliases to node names

        :returns: mappings
        :rtype: dict
        """
        rows = self.database.query("select alias, node_name from t_sm_configuration_alias")
        d = {row["alias"]: row["node_name"] for row in rows}
        if 'system-manager' in d:
            sm = self.getSystemManagerNodeName()
            d['system-manager'] = sm
        return d

    def getDetail(self, node, name, detail):
        """
        Get the detail property of a system or device manager.

        :param node: node
        :type node: str
        :param name: device manager name
        :type name: str
        :param detail: detail property
        :type detail: str
        :returns: str
        """
        rowId, details = self.getDetails(node, name)
        if rowId < 0:
            self.log.error("could not get details because '%s%s' does not exist", node, "/" + name if name else "")
        return details.get(detail)

    def getDetails(self, node, name=None):
        """
        Get the details of a system or device manager.

        :param node: node
        :type node: str
        :param name: device manager name
        :type name: str
        :returns: (id, dict)
        :rtype: tuple
        """
        if name:
            info = self.getDevice(node, name)
        else:
            info = self.getNode(node, includeDevices=False)

        if info is None:
            self.log.error("could not get details because '%s%s' does not exist", node, "/" + name if name else "")
            return (-1, {})
        return (info.id, info.details)

    def getDevice(self, node, fullDeviceName):
        """
        Get device information

        :param node: node
        :type node: str
        :param fullDeviceName: fully qualified device name
        :type fullDeviceName: str
        :returns: device info
        :rtype: :class:`~dynamite.system.configuration.DeviceInfo`
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

    def getPlatform(self):
        """
        Get platform information

        :returns: platform
        :rtype: :class:`~dynamite.system.configuration.PlatformInfo`
        """
        rows = self.database.query("select property, value from t_sm_platform")
        data = {}
        for row in rows:
            data[row["property"]] = row["value"]
        return PlatformInfo(
            name=data.get("name", "unknown"),
            platformType=data.get("type", "dynamite.system.platforms.Unknown"),
            description=data.get("description", ""),
            settings=json.loads(data.get("settings", "{}")),
            storage=json.loads(data.get("storage", "{}"))
        )

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
        :returns: :class:`~dynamite.system.configuration.States`
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
        node = self.systemManagerNode
        return node.name if node is not None else None

    def getTargetState(self, node, name=None):
        """
        Get the target state of a node or device manager.

        :param node: node
        :type node: str
        :param name: device manager name
        :type name: str
        :returns: :class:`~dynamite.system.configuration.States`
        """
        state = self.getDetail(node, name, "targetState");
        if state is None:
            return None
        return States.valueOf(state)

    def _createNodeInfo(self, nodeRow):
        nodeDetailsJSON = nodeRow["details"]
        nodeDetails = json.loads(nodeDetailsJSON)
        nodeRole = Roles.valueOf(nodeDetails["role"])
        nodeState = States.valueOf(nodeRow["state"])
        nodeInfo = NodeInfo(nodeRow["name"], nodeDetails["address"], nodeRole, nodeState, nodeRow["id"])
        nodeInfo.details = nodeDetails
        return nodeInfo

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
        :rtype: :class:`~dynamite.system.configuration.NodeInfo`
        """
        try:
            if includeDevices:
                rows = self.database.query("""
                    with recursive
                        configuration(id, level, name, state, type, details, parent_id) as (
                            select id, 0, name, state, type, details, parent_id
                            from t_sm_configuration
                            where parent_id is null and name is ?
                            union all
                            select t.id, configuration.level+1, configuration.name || "." || t.name, t.state, t.type, t.details, t.parent_id
                            from t_sm_configuration as t join configuration on t.parent_id=configuration.id
                         order by 2 desc
                        )
                    select * from configuration;""", (node,))
            else:
                rows = self.database.query("""
                    select * from t_sm_configuration
                    where parent_id is null and name is ?""", (node,))

            if not rows:
                return None

            # deal with node information
            nodeRow = rows.pop(0)
            nodeInfo = self._createNodeInfo(nodeRow)

            if rows:

                if not flatDeviceHierarchy:
                    root = NodeInfo("root", None)
                    root.devices[nodeRow["name"]] = nodeInfo

                for row in rows:

                    # split fully qualified name into path and name
                    currentPath = row["name"].split(".")

                    detailsJSON = row["details"]
                    details = json.loads(detailsJSON)

                    if flatDeviceHierarchy:

                        # strip node name from device name
                        currentPath.pop(0)
                        deviceName = ".".join(currentPath)

                        # create device information
                        deviceInfo = DeviceInfo(deviceName, row["type"], States.valueOf(row["state"]), row["id"], row["parent_id"])
                        deviceInfo.details = details
                        nodeInfo.devices[deviceName] = deviceInfo

                    else:
                        # create device information
                        name = currentPath.pop()
                        deviceInfo = DeviceInfo(name, row["type"], States.valueOf(row["state"]), row["id"], row["parent_id"])
                        deviceInfo.details = details

                        # traverse path to parent
                        currentDeviceInfo = root
                        for pathElement in currentPath:
                            currentDeviceInfo = currentDeviceInfo.devices[pathElement]
                        currentDeviceInfo.addDevice(deviceInfo)

            return nodeInfo

        except Exception as e:
            import traceback
            self.log.error(traceback.format_exc())
            self.log.error("could not get node info for '%s': '%s'", node, e)
            return None

    def getNodeNames(self):
        """
        Return a list of node names.
        """
        rows = self.database.query("""
            select name from t_sm_configuration
            where parent_id is null""")
        return [row["name"] for row in rows]

    def loadFromInfo(self, configurationInfo):
        """
        Load configuration from the specified configuration information.

        :param configurationInfo: configuration information
        :type configurationInfo: :class:`~dynamite.system.configuration.ConfigurationInfo`
        :raises: :class:`ConfigurationValidationError` raised when validation fails.
        """
        configurationInfo.validate()
        for node in configurationInfo.nodes.values():
            self.addNode(node)
        for alias, nodeName in configurationInfo.aliases.items():
            self.addAlias(alias, nodeName)
        self.addPlatform(configurationInfo.platform)

    def removeDetail(self, node, name, detail):
        """
        Remove detail property from a system or device manager

        :param node: node
        :type node: str
        :param name: device manager name
        :type name: str
        :param detail: detail property
        :type detail: str
        """
        rowId, details = self.getDetails(node, name)
        if detail in details:
            del details[detail]
            self.database.writeCommit("update t_sm_configuration set details = ? where id is ?", (json.dumps(details), rowId))
        else:
            self.log.error("could not remove '%s' from '%s/%s' because it detail does not exist", detail, node, name)

    def removeDevice(self, node, fullDeviceName):
        """
        Remove a device from the configuration

        :param node: node name
        :type node: str
        :param fullDeviceName: fully qualified device name
        :type fullDeviceName: str
        """
        devices = self.getDevices(node, flatDeviceHierarchy=True)

        # get matching device and its children
        rowIds = sorted([(device.id,) for device in devices.values() if device.name.startswith(fullDeviceName)])
        if rowIds:
            self.database.writeMany("""
                delete from t_sm_configuration where id is ?""",
                *rowIds)
        else:
            self.log.error("could not remove '%s' from '%s' because it does not exist", fullDeviceName, node)

    def removeNode(self, node):
        """
        Remove node from the configuration

        :param node: node name
        :type node: str
        """
        nodeInfo = self.getNode(node, flatDeviceHierarchy=True)
        if nodeInfo is None:
            self.log.error("could not remove '%s' because it does not exist", node)
            return

        rowIds = [(nodeInfo.id,)]
        rowIds.extend([(device.id,) for device in nodeInfo.devices.values()])
        rowIds = sorted(rowIds)

        # removing node and its alias should be in a single transaction
        self.database.writeMany("""
            delete from t_sm_configuration where id is ?""",
            *rowIds)

        # remove aliases for node
        self.removeAlias(node=node)

    def removeTargetState(self, node, name=None):
        """
        Remove target state from a system or device manager

        :param node: node
        :type node: str
        :param name: device manager name
        :type name: str
        """
        self.removeDetail(node, name, "targetState")

    def resetDeviceStates(self):
        """
        Sets the states of all devices to REGISTERED unless their state is
        MAINTENANCE or UNDEPLOYED.
        """
        self.database.writeCommit("""
            update t_sm_configuration set state = ?
            where parent_id is not null
            and state is not 'MAINTENANCE'
            and state is not 'REGISTERED'
            and state is not 'UNDEPLOYED'""",
            (States.REGISTERED.name,))

    def resetNodes(self):
        """
        Reset the roles of all nodes in the database. This is typically useful
        for starting system
        """
        for node in self.getNodeNames():
            if self.getRole(node) == Roles.ACTIVE:
                self.changeRole(node, Roles.PASSIVE)
            self.changeState(node, None, States.MAINTENANCE)

        self.resetDeviceStates()

    def removeAlias(self, alias=None, node=None):
        """
        Remove the specified alias mapping. If none is specified, then remove
        all aliases

        :param str alias: the alias
        :param str node: the node name
        """
        # remove aliases for node
        where = []
        param = []
        if alias:
            where.append("alias=?")
            param.append(alias)
        if node:
            where.append("node_name=?")
            param.append(node)

        where = ("where " + " and ".join(where)) if where else ""
        query = "delete from t_sm_configuration_alias {0}".format(where)

        self.database.writeCommit(query, tuple(param))

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
        :rtype: :class:`~dynamite.system.configuration.ConfigurationInfo`
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
        :type node: :class:`~dynamite.system.configuration.DeviceInfo` or :class:`~dynamite.system.configuration.NodeInfo`
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

@ClassLogger
class DBClusterInfo(object):
    """
    A basic cluster information object backed by the database depending on the node role

    :param node: node
    :type node: str
    :param address: address of the node
    :type address: str
    """
    def __init__(self, node, address):
        super(DBClusterInfo, self).__init__()
        self.node = node
        self.address = address

    @property
    def aliases(self):
        """
        Alias mappings
        """
        return Configuration().getAliases()

    @aliases.setter
    def aliases(self, value):
        raise AttributeError, "Attribute aliases is read-only"

    def getNodeAddress(self, node):
        """
        Get address for specified node

        :param node: node
        :type node: str
        :returns: str or ``None`` if not found
        """
        # Configuration.getAddress does resolve 'system-manager' to the correct address
        # if one is known
        address = Configuration().getAddress(node)
        if not address and node == self.node:
            address = self.address
        return address

    @property
    def nodeNames(self):
        """
        Names of the nodes in the cluster
        """
        return Configuration().getNodeNames()

    @nodeNames.setter
    def nodeNames(self, value):
        raise AttributeError, "Attribute nodeNames is read-only"

    @property
    def role(self):
        """
        Node role
        """
        return Configuration().getRole(self.node)

    @role.setter
    def role(self, role):
        if isinstance(role, Roles):
            Configuration().changeRole(self.node, role)
        else:
            self.log.error("'%s' does not match enum of type '%s'", role, Roles)

    @property
    def state(self):
        """
        Node state
        """
        return Configuration().getState(self.node)

    @state.setter
    def state(self, state):
        if isinstance(state, States):
            Configuration().changeState(self.node, None, state)
        else:
            self.log.error("'%s' does not match enum of type '%s'", state, States)

    @property
    def systemManagerAddress(self):
        """
        Active system manager address
        """
        return Configuration().systemManagerAddress

    @property
    def systemManagerNodeName(self):
        """
        Active system manager node name
        """
        return Configuration().getSystemManagerNodeName()

    # ClusterInfo shouldn't allow updates to systemManager, that would be done
    # by the application by changing the role to ACTIVE in SMDB

    @systemManagerAddress.setter
    def systemManagerAddress(self, value):
        raise AttributeError, "Attribute systemManagerAddress is read-only"

    @systemManagerNodeName.setter
    def systemManagerNodeName(self, value):
        raise AttributeError, "Attribute systemManagerNode is read-only"

@ClassLogger
class GPFSClusterInfo(DBClusterInfo):
    """
    A basic cluster information object backed by the database depending on the node role

    :param node: node
    :type node: str
    :param address: address of the node
    :type address: str
    :param systemManagerAddress: address of the system manager
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

            database = DBManager()
            rows = database.query("select alias, node_name from t_sm_configuration_alias")
            return {row[0]: row[1] for row in rows}
        else:
            return {"system-manager": "system-manager"}

    @property
    def GPFSrole(self):
        """
        GPFS Node Role
        :returns: str or ``None`` if no node role
        """
        node_info = gpfs.getNodes()[self.node]
        if node_info.manager and node_info.quorum:
            return GPFSRoles.QUORUM_MANAGER
        elif node_info.manager:
            return GPFSRoles.MANAGER
        elif node_info.quorum:
            return GPFSRoles.QUORUM
        else:
            return GPFSRoles.CLIENT

    @GPFSrole.setter
    def GPFSrole(self, gpfsrole):
        """
        Sets GPFS node role
        :param gpfsroles: the role to assign to the node
        :type gpfsroles: :py:class:~`dynamite.system.configuration.GPFSRoles`
        """
        if isinstance(gpfsrole, GPFSRoles):
            if gpfsrole == GPFSRoles.QUORUM_MANAGER:
                gpfs.changeNode(self.node, {'--quorum':'', '--manager':''})
            else:
                role_string = "--{0}".format(gpfsrole.value)
                gpfs.changeNode(self.node, {role_string:''})
        else:
            logging.error("'%s' does not match enum of type '%s'", gpfsrole, GPFSRoles)

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

    def getNodeAddress(self, node):
        """
        Get address for specified node

        :param node: node
        :type node: str
        :returns: str or ``None`` if not found
        """
        if self.role == Roles.ACTIVE or self.role == Roles.PASSIVE:

            database = DBManager()
            if node == "system-manager":
                rows = database.query("select node_name from t_sm_configuration_alias where alias is ?", ("system-manager",))
                if rows:
                    node = rows[0][0]
                else:
                    return None

            rows = database.query("select details from t_sm_configuration where name is ?", (node,))
            if rows:
                return json.loads(rows[0][0])["address"]
            else:
                return None
        else:
            if node == "system-manager":
                return self._systemManagerAddress.value
            elif node == self.node:
                return self.address
            else:
                return None

    @property
    def nodeAddresses(self):
        """
        Addressess of the nodes in the cluster
        """
        if self.role == Roles.ACTIVE or self.role == Roles.PASSIVE:
            database = DBManager()
            rows = database.query("select details from t_sm_configuration where parent_id is null")
            return [json.loads(row[0])["address"] for row in rows]
        else:
            return [self.address, self._systemManagerAddress.value]

    @property
    def nodeNames(self):
        """
        Names of the nodes in the cluster
        """
        if self.role == Roles.ACTIVE or self.role == Roles.PASSIVE:
            database = DBManager()
            rows = database.query("select name from t_sm_configuration where parent_id is null")
            return [row[0] for row in rows]
        else:
#            return [node.name for node in gpfs.getNodes()]
            return [self.node, "system-manager"]

class DeviceInfo(JSONSerializable):
    """
    Device manager information

    :param name: name
    :type name: str
    :param deviceType: type
    :type deviceType: str
    :param state: state
    :type state: :class:`~dynamite.system.configuration.States`
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
        :type device: :class:`~dynamite.system.configuration.DeviceInfo`
        :returns: :class:`~dynamite.system.configuration.DeviceInfo`
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
    :type role: :class:`~dynamite.system.configuration.Roles`
    :param state: state
    :type state: :class:`~dynamite.system.configuration.States`
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

    def __str__(self):
        return '%s(%s, %s, %s)' % (self.name, self.role.name, self.state.name, self.address)
    __repr__ = __str__

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
        :type device: :class:`~dynamite.system.configuration.DeviceInfo`
        :returns: :class:`~dynamite.system.configuration.NodeInfo`
        """
        if device.name in self.devices:
            log.error("'%s' already part of '%s'", device.name, self.name)
        else:
            self.devices[device.name] = device
        return self

    @classmethod
    def fromJSONSerializable(clazz, d):
        if JSONSerializable.dictHasType(d, NodeInfo):
            nodeInfo = NodeInfo(d["name"], d["address"], d["role"], States.DEPLOYED)
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
        del serializableDict["state"]
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
    :param storage: storage
    :type storage: dict
    """
    def __init__(self, name="unknown", platformType="dynamite.system.platforms.Unknown", description="", settings=None, storage=None):
        self.name = name
        self.type = platformType
        self.description = description
        self.settings = settings or {}
        self.storage = storage or {}

    def __eq__(self, other):
        if (isinstance(other, PlatformInfo)
            and self.name == other.name
            and self.description == other.description
            and self.settings == other.settings
            and self.storage == other.storage
            and self.type == other.type):
            return True
        return False

    def __ne__(self, other):
        return not self.__eq__(other)
