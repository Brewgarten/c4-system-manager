"""
Copyright (c) IBM 2015-2017. All Rights Reserved.
Project name: c4-system-manager
This project is licensed under the MIT License, see LICENSE
"""
import datetime
import glob
import json
import logging
import os
import sqlite3

from c4.system.backend import Backend, BackendImplementation, BackendKeyValueStore
from c4.system.configuration import (Configuration,
                                     DeviceInfo,
                                     NodeInfo,
                                     PlatformInfo,
                                     Roles,
                                     SharedClusterInfo,
                                     States)
from c4.system.deviceManager import DeviceManagerStatus
from c4.system.history import (DeviceHistory,
                               Entry,
                               NodeHistory)
from c4.system.manager import SystemManagerStatus
from c4.utils.jsonutil import JSONSerializable
from c4.utils.logutil import ClassLogger
from c4.utils.util import getPackageData
from c4.utils.version import BasicVersion


# Lowest version of SQLite with Common Table Expression support
SqliteCTEMinimumVersion = BasicVersion("3.8.3")

@ClassLogger
class DBManager(object):
    """
    Simple database wrapper that shares a single database connection.

    Connects to the database.  If the database does not exist we check backup files
    and if there is a backup we automatically restore the latest, if not then a new
    database will be created with the schema file that is in the c4.data directory.

    :param databasePath: database path
    :type databasePath: str
    :param backupPath: backup path
    :type backupPath: str
    :param dbName: name of the database.  Defaults to sysmgr.db
    :type dbName: str
    :param enableNamedColumns: enable named columns (returns rows as :class:`sqlite3.Row`)
    :type enableNamedColumns: bool
    """
    def __init__(self, info, dbName="sysmgr.db", enableNamedColumns=True):
        self.databasePath = info.properties.get("path.database", "/dev/shm")
        self.backupPath = info.properties.get("path.backup", "/tmp")
        self.fullDBName = os.path.join(self.databasePath, dbName)
        # if the database does not exist, then create it
        if not os.path.isfile(self.fullDBName):
            # look for backup files
            backupFiles = DBManager.getBackupFiles(self.backupPath)
            if backupFiles:
                # restore latest backup
                self.restore(backupFiles.pop())
            else:
                DBManager.create(self.databasePath, self.fullDBName)

        # connect to the database
        self.conn = sqlite3.connect(self.fullDBName)
        # turn off autocommit
        self.conn.isolation_level = None
        if enableNamedColumns:
            # allow columns to be accessed by name
            self.conn.row_factory = sqlite3.Row

    @classmethod
    def create(cls, databasePath, dbName="sysmgr.db", overwrite=False):
        """
        Creates the sysmgr.db database if it does not already exist.

        :param dbName: name of the database.  Defaults to sysmgr.db
        :type dbName: str
        :param overwrite: If overwrite is True and the database exists, then it will be deleted.  Defaults to False.
        :type overwrite: bool
        :returns: True if successful, False if not
        """
        # if the database exists
        fullDBName = os.path.join(databasePath, dbName)
        if os.path.isfile(fullDBName):
            # if we are allowed to overwrite it, then delete it
            if overwrite:
                cls.log.warn("Database %s exists.  Deleting.", fullDBName)
                os.remove(fullDBName)
            # else error
            else:
                cls.log.error("Error creating database: %s.  Already exists.", fullDBName)
                return False
        # the schema file is in the data dir
        schema = getPackageData("c4.data", "sql/sysmgr.sql")
        # create the database
        connection = sqlite3.connect(fullDBName)
        cursor = connection.cursor()
        cursor.executescript(schema)
        return True

    @classmethod
    def getBackupFiles(cls, backupPath):
        """
        Get backup files

        :param backupPath: backup path
        :type backupPath: str
        :returns: list of backup files sorted by date (from earliest to latest)
        :rtype: [str]
        """
        return sorted(glob.glob(os.path.join(backupPath, "sysmgr.db_backup-*")))

    def backup(self, backupPath=None):
        """
        Perform a backup of the stored data

        :param backupPath: backup path
        :type backupPath: str
        :returns: backup file name
        :rtype: str
        """
        try:
            start = datetime.datetime.utcnow()
            backupPath = backupPath or self.backupPath
            backupFileName = os.path.join(backupPath, "sysmgr.db_backup-{:%Y-%m-%dT%H:%M:%SZ}".format(start))
            with open(backupFileName, "w") as f:
                batch = []
                counter = 0

                dumpIterator = self.conn.iterdump()

                # get BEGIN TRANSACTION part
                batch.append(dumpIterator.next())

                # add table drops
                rows = self.query("""
                    select name from sqlite_master
                    where sql not null and type is 'table'
                    order by name""")
                for row in rows:
                    batch.append("DROP TABLE IF EXISTS {0};".format(row["name"]))

                # continue with create and insert statements
                for line in dumpIterator:
                    batch.append(line)
                    counter += 1
                    if counter % 500 == 0:
                        f.write("\n".join(batch))
                        f.write("\n")
                        batch[:] = []

                if batch:
                    f.write("\n".join(batch))
                    f.write("\n")
                    batch[:] = []

            end = datetime.datetime.utcnow()
            self.log.debug("Backing up database to %s took %s", backupFileName, end-start)
            return backupFileName
        except Exception as e:
            self.log.error("Could perform backup %s", e)

    def close(self):
        """
        Close the connection to the database
        """
        self.conn.close()

    def restore(self, backupFileName):
        """
        Restore backed up data from the specified backup file

        :param backupFileName: name of a backup file
        :type backupFileName: str
        :returns: ``True`` if successful, ``False`` if not
        :rtype: bool
        """
        if not os.path.exists(backupFileName):
            self.log.error("Backup file '%s' does not exist", backupFileName)
            return False
        start = datetime.datetime.utcnow()
        with open(backupFileName) as f:
            schema = f.read()
            connection = sqlite3.connect(self.fullDBName)
            cursor = connection.cursor()
            cursor.executescript(schema)
        end = datetime.datetime.utcnow()
        self.log.debug("Restoring database from %s took %s", backupFileName, end-start)
        return True

    def query(self, statement, *parameters):
        """
        Select query

        :param statement: select statement
        :type statement: str
        :param parameters: optional sql parameters
        :returns: rows
        """
        try:
            start = datetime.datetime.utcnow()
            cur = self.conn.cursor()
            cur.execute(statement, *parameters)
            rows = cur.fetchall()
            end = datetime.datetime.utcnow()
            self.log.debug("Executing sql query: %s %s took %s", statement, parameters, end-start)
        except sqlite3.Error as e:
            self.log.error("Could not execute sql query: %s %s, %s", statement, parameters, e)
            rows = []
        return rows

    def writeCommit(self, statement, *parameters):
        """
        Write to the database. Commits.

        :param statement: insert or update statement
        :type statement: str
        :param parameters: optional sql parameters
        :returns: affected rows
        :rtype: int
        """
        try:
            start = datetime.datetime.utcnow()
            cur = self.conn.cursor()
            cur.execute(statement, *parameters)
            self.conn.commit()
            end = datetime.datetime.utcnow()
            self.log.debug("Executing sql update/insert with commit: %s %s took %s", statement, parameters, end-start)
            return cur.rowcount
        except sqlite3.Error, message:
            self.log.error("Could not execute sql update/insert with commit: %s %s, %s", statement, parameters, message)

    def write(self, statement, *parameters):
        """
        Write to the database. Does not commit.
        Must start transaction with a write("begin") and
        end with a write("commit")

        :param statement: insert or update statement
        :type statement: str
        :param parameters: optional sql parameters
        :returns: affected rows
        :rtype: int
        """
        try:
            start = datetime.datetime.utcnow()
            cur = self.conn.cursor()
            cur.execute(statement, *parameters)
            end = datetime.datetime.utcnow()
            self.log.debug("Executing sql update/insert: %s %s took %s", statement, parameters, end-start)
            return cur.rowcount
        except sqlite3.Error, message:
            self.log.error("Could not execute sql update/insert: %s %s, %s", statement, parameters, message)

    # TODO: merge with write
    def writeMany(self, statement, *parameters):
        """
        Write to the database. Does not commit.
        Must start transaction with a write("begin") and
        end with a write("commit")

        :param statement: insert or update statement
        :type statement: str
        :param parameters: parameter sequences
        :returns: affected rows
        :rtype: int
        """
        try:
            start = datetime.datetime.utcnow()
            cur = self.conn.cursor()
            cur.executemany(statement, parameters)
            end = datetime.datetime.utcnow()
            if self.log.isEnabledFor(logging.DEBUG):
                self.log.debug("Executing sql update/insert: \n%s\n%s\ntook %s",
                               str.strip(statement), "\n".join(str(p) for p in parameters), end-start)
            return cur.rowcount
        except sqlite3.Error, message:
            self.log.error("Could not execute sql update/insert: %s %s, %s", statement, parameters, message)

@ClassLogger
class SharedSqliteDBBackend(BackendImplementation):
    """
    Shared SQLite database backend implementation

    :param info: backend info
    :type info: :class:`~BackendInfo`
    """
    def __init__(self, info):
        super(SharedSqliteDBBackend, self).__init__(info)

    @property
    def configuration(self):
        """
        Shared SQLite database backend based configuration instance
        """
        return SharedSqliteDBConfiguration(self.database)

    @property
    def database(self):
        """
        Shared SQLite database client instance
        """
        return DBManager(self.info)

    def ClusterInfo(self, node, address, systemManagerAddress, role, state):
        return SharedClusterInfo(self, node, address, systemManagerAddress, role, state)

    @property
    def deviceHistory(self):
        """
        Shared SQLite database backend device manager history implementation
        """
        return SharedSqliteDBDeviceHistory(self.database)

    @property
    def keyValueStore(self):
        """
        Shared SQLite database based key-value store instance
        """
        return SharedSqliteDBKeyValueStore(self.database)

    @property
    def nodeHistory(self):
        """
        Shared SQLite database backend node history implementation
        """
        return SharedSqliteDBNodeHistory(self.database)

@ClassLogger
class SharedSqliteDBConfiguration(Configuration):
    """
    Shared SQLite database backend configuration implementation

    :param database: database manager
    :type database: :class:`~DBManager`
    """
    def __init__(self, database):
        self.database = database
        self.store = Backend().keyValueStore

    def _getDetails(self, node, name=None):
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

        details = {
            key: value
            for key, value in info.properties.items()
        }
        if name is None:
            details["role"] = info.role.name
        return (info._id, details)

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
            insert into t_sm_configuration_alias (alias, node_name) values (?, ?)""", (alias, node))
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
        # go through the hierarchy and get each device id
        # we want the last id of the hierarchy
        parentId = nodeInfo._id
        existingDevices = nodeInfo.devices
        for devicePart in deviceParts:

            if devicePart not in existingDevices:
                self.log.error("unable to add device because device parent '%s' not found for node '%s'", devicePart, node)
                return None
            parentId = existingDevices[devicePart]._id
            existingDevices = existingDevices[devicePart].devices

        if device.name in existingDevices:
            self.log.error("unable to add device because device '%s' already exists for node '%s'", device.name, node)
            return None

        self.database.writeCommit("""
            insert into t_sm_configuration (parent_id, name, state, type, details)
            values (?, ?, ?, ?, ?)""", (parentId, device.name, device.state.name, device.type, json.dumps(device.properties)))
        # get the id of the last configuration record
        rows = self.database.query("select id from t_sm_configuration where parent_id is ? and name = ?", (parentId, device.name))
        device._id = rows[0]["id"]
        device._parentId = parentId

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
        :type node: :class:`~c4.system.configuration.NodeInfo`
        :returns: node info with database ids
        :rtype: :class:`~c4.system.configuration.NodeInfo`
        """
        nodeInfo = self.getNode(node.name, includeDevices=False)
        if nodeInfo:
            self.log.error("node '%s' already exists", node.name)
            return None

        details = {
            key: value
            for key, value in node.properties.items()
        }
        details["role"] = node.role.name
        self.database.writeCommit("""
            insert into t_sm_configuration (name, state, type, details)
            values (?, ?, ?, ?)""", (node.name, node.state.name, "c4.system.manager.SystemManager", json.dumps(details)))
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
        :type platform: :class:`~c4.system.configuration.PlatformInfo`
        """
        self.database.write("begin")
        self.database.write("delete from t_sm_platform")
        self.database.writeMany(
            """insert into t_sm_platform (property, value) values (?, ?)""",
            ("name", platform.name),
            ("type", platform.type),
            ("description", platform.description),
            ("settings", json.dumps(platform.settings)))
        self.database.write("commit")

    def addRoleInfo(self, role):
        """
        Add a role information object with expected devices.

        :param role: role
        :type role: :class:`~c4.system.configuration.RoleInfo`
        :returns: role info
        :rtype: :class:`~c4.system.configuration.RoleInfo`
        """
        key = "/roles/{role}".format(role=role.role.name)
        value = serialize(role)
        self.store.put(key, value)
        return role
    
    def clear(self):
        """
        Removes all nodes and devices from the configuration object and the database.
        """
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
        rowId, details = self._getDetails(node, name)
        if rowId < 0:
            return None
        if propertyName not in details and not setIfNotExist:
            self.log.error("property '%s' of '%s/%s' does not exist", propertyName, node, name)
            return None
        previousValue = details.get(propertyName, None)
        details[propertyName] = value
        updated = self.database.writeCommit("update t_sm_configuration set details = ? where id is ?", (json.dumps(details), rowId))
        if updated < 1:
            self.log.error("could not change property '%s' of '%s/%s' to '%s'", propertyName, node, name, value)
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
        roleName = self.changeProperty(node, None, "role", role.name)
        if not roleName:
            return None
        return Roles.valueOf(roleName)

    def changeRoleInfo(self, role, info):
        """
        Change the role information for a given role

        :param role: role
        :type role: :class:`Roles`
        :param info: roleInfo
        :type info: :class:`~c4.system.configuration.RoleInfo`
        :returns: role info
        :rtype: :class:`~c4.system.configuration.RoleInfo`
        """
        key = "/roles/{role}".format(role=role.name)
        value = serialize(info)
        self.store.put(key, value)
        return info

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
                self.database.writeCommit(
                    """update t_sm_configuration set state = ? where id is ?""",
                    (state.name, deviceInfo._id))

            else:
                if state == States.REGISTERED or state == States.MAINTENANCE:
                    nodeInfo = self.getNode(node, flatDeviceHierarchy=True)
                else:
                    nodeInfo = self.getNode(node, includeDevices=False)

                if nodeInfo is None:
                    self.log.error("could not change state of '%s' to '%s' because node does not exist", node, state)
                    return None

                previousState = nodeInfo.state
                self.database.writeCommit(
                    """update t_sm_configuration set state = ? where id is ?""",
                    (state.name, nodeInfo._id))

                # handle special cases
                if state == States.REGISTERED or state == States.MAINTENANCE:

                    deviceList = sorted(nodeInfo.devices.values())
                    # note that we do not automatically change the state of children if they are in Maintenance mode
                    self.database.writeMany(
                        """update t_sm_configuration set state = ? where id is ? and state is not 'MAINTENANCE'""",
                        *[(state.name, device._id) for device in deviceList])

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
            return None
        previousStateName = self.changeProperty(node, name, "targetState", state.name, setIfNotExist=True)
        if previousStateName is None:
            return None
        return States.valueOf(previousStateName)

    def getAliases(self):
        """
        Get a mapping of aliases to node names

        :returns: mappings
        :rtype: dict
        """
        rows = self.database.query("select alias, node_name from t_sm_configuration_alias")
        return {row["alias"]: row["node_name"] for row in rows}

    def getPlatform(self):
        """
        Get platform information

        :returns: platform
        :rtype: :class:`~c4.system.configuration.PlatformInfo`
        """
        rows = self.database.query("select property, value from t_sm_platform")
        data = {}
        for row in rows:
            data[row["property"]] = row["value"]
        return PlatformInfo(
            data.get("name", "unknown"),
            data.get("type", "c4.system.platforms.Unknown"),
            data.get("description", ""),
            json.loads(data.get("settings", "{}"))
        )

    def getProperty(self, node, name, propertyName, default=None):
        """
        Get the property of a system or device manager.

        :param node: node
        :type node: str
        :param name: device manager name
        :type name: str
        :param propertyName: property name
        :type propertyName: str
        :param default: default value to return if property does not exist
        :returns: property value
        """
        rowId, details = self._getDetails(node, name)
        if rowId < 0:
            return default
        return details.get(propertyName, default)

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
        _, details = self._getDetails(node, name)
        if name is None and "role" in details:
            del details["role"]
        return details

    def getRoleInfo(self, role):
        """
        Get role information for the specified role

        :param role: role
        :type role: :class:`Roles`
        :returns: role info
        :rtype: :class:`~c4.system.configuration.RoleInfo`
        """
        key = "/roles/{role}".format(role=role.name)
        value = self.store.get(key)
        if value:
            return deserialize(value)
        return None

    def getRoles(self):
        """
        Get a mapping of roles to role info objects

        :returns: mappings
        :rtype: dict
        """
        rolesPrefix = "/roles/"
        
        # note that key is the role name and value is the role info
        return {
            key.replace(rolesPrefix, ""): deserialize(value)
            for key, value in self.store.getPrefix(rolesPrefix)
        }

    def getTargetState(self, node, name=None):
        """
        Get the target state of a node or device manager.

        :param node: node
        :type node: str
        :param name: device manager name
        :type name: str
        :returns: :class:`~c4.system.configuration.States`
        """
        state = self.getProperty(node, name, "targetState")
        if state is None:
            return None
        return States.valueOf(state)

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
        try:
            if includeDevices:
                if BasicVersion(sqlite3.sqlite_version) < SqliteCTEMinimumVersion:
                    # For versions of sqlite without common table expressions it is necessary to
                    # emulate a hierarchical query

                    # Start from the system manager
                    frontier = self.database.query("""
                                select id, 0, name, state, type, details, parent_id
                                from t_sm_configuration
                                where parent_id is null and name is ?""", (node,))
                    if len(frontier) == 0:
                        raise Exception("Invalid name for system manager")
                    rows = []
                    while len(frontier) > 0:
                        visiting = frontier.pop(0)
                        rows.append(visiting)
                        # Add the children of current device
                        frontier.extend(self.database.query("""
                                            select t.id as id,
                                                   ? as level,
                                                   ? || "." || t.name as name,
                                                   t.state as state,
                                                   t.type as type,
                                                   t.details as details,
                                                   t.parent_id as parent_id
                                            from t_sm_configuration as t
                                            where parent_id = ?""", (visiting[1]+1, visiting["name"], visiting["id"])))
                else:
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
            nodeDetailsJSON = nodeRow["details"]
            nodeProperties = json.loads(nodeDetailsJSON)
            nodeRole = Roles.valueOf(nodeProperties.pop("role"))
            nodeState = States.valueOf(nodeRow["state"])
            nodeInfo = NodeInfo(nodeRow["name"], nodeProperties["address"], role=nodeRole, state=nodeState)
            nodeInfo._id = nodeRow["id"]
            nodeInfo.properties = nodeProperties

            if rows:

                if not flatDeviceHierarchy:
                    root = NodeInfo("root", None)
                    root.devices[nodeRow["name"]] = nodeInfo

                for row in rows:

                    # split fully qualified name into path and name
                    currentPath = row["name"].split(".")

                    detailsJSON = row["details"]
                    properties = json.loads(detailsJSON)

                    if flatDeviceHierarchy:

                        # strip node name from device name
                        currentPath.pop(0)
                        deviceName = ".".join(currentPath)

                        # create device information
                        deviceInfo = DeviceInfo(deviceName, row["type"], state=States.valueOf(row["state"]))
                        deviceInfo._id = row["id"]
                        deviceInfo._parentId = row["parent_id"]
                        deviceInfo.properties = properties
                        nodeInfo.devices[deviceName] = deviceInfo

                    else:
                        # create device information
                        name = currentPath.pop()
                        deviceInfo = DeviceInfo(name, row["type"], state=States.valueOf(row["state"]))
                        deviceInfo._id = row["id"]
                        deviceInfo._parentId = row["parent_id"]
                        deviceInfo.properties = properties

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

    def refresh(self):
        """
        Refresh information from backend
        """
        #FIXME: not implemented, used to refresh cached values for etcd        
        pass

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
        rowIds = sorted([(device._id,) for device in devices.values() if device.name.startswith(fullDeviceName)])
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

        rowIds = [(nodeInfo._id,)]
        rowIds.extend([(device._id,) for device in nodeInfo.devices.values()])
        rowIds = sorted(rowIds)
        self.database.writeMany(
            """delete from t_sm_configuration where id is ?""",
            *rowIds)

        # remove aliases for node
        self.database.writeCommit(
            """delete from t_sm_configuration_alias where node_name=?""",
            (node,))

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
        rowId, details = self._getDetails(node, name)
        if propertyName in details:
            del details[propertyName]
            self.database.writeCommit("update t_sm_configuration set details = ? where id is ?", (json.dumps(details), rowId))

    def removeRoleInfo(self, role):
        """
        Remove role information

        :param role: role
        :type role: :class:`Roles`
        """
        key = "/roles/{role}".format(role=role.name)
        self.store.delete(key)

    def resetDeviceStates(self):
        """
        Sets the states of all devices to REGISTERED unless their state is
        MAINTENANCE or UNDEPLOYED.
        """
        self.database.writeCommit(
            """
            update t_sm_configuration set state = ?
            where parent_id is not null
            and state is not 'MAINTENANCE'
            and state is not 'REGISTERED'
            and state is not 'UNDEPLOYED'""",
            (States.REGISTERED.name,))

    def resolveAlias(self, alias):
        """
        Get node name for the specified alias

        :param alias: alias
        :type alias: str
        :returns: node name
        :rtype: str
        """
        rows = self.database.query(
            """
            select node_name from t_sm_configuration_alias
            where alias is ?""",
            (alias,))
        if rows:
            return rows[0]["node_name"]
        return None

@ClassLogger
class SharedSqliteDBDeviceHistory(DeviceHistory):
    """
    Shared SQLite database backend device manager history implementation

    :param database: database manager
    :type database: :class:`~DBManager`
    """
    def __init__(self, database):
        self.database = database

    def add(self, node, name, status, ttl=None):
        """
        Add status for device manager with specified name on specified node

        :param node: node name
        :type node: str
        :param name: device manager name
        :type name: str
        :param status: status
        :type status: :class:`DeviceManagerStatus`
        :param ttl: time to live (in seconds), infinite by default
        :type ttl: int
        """
        if not isinstance(status, DeviceManagerStatus):
            raise ValueError("'{0}' needs to be a '{1}'".format(status, DeviceManagerStatus))

        if ttl is not None:
            raise NotImplementedError("SQLite does not support time to live attributes")

        timestamp = status.timestamp.toISOFormattedString()
        serializedStatus = status.toJSON(includeClassInfo=True)

        self.database.write("begin")
        self.database.write("""
            insert into history (node, name, timestamp, status) values (?, ?, ?, ?)""",
            (node, name, timestamp, serializedStatus))
        self.database.write("""
            insert or replace into status (node, name, status) values (?, ?, ?)""",
            (node, name, serializedStatus))
        self.database.write("commit")

    def get(self, node, name, limit=None):
        """
        Get status history for device manager with specified name on specified node

        :param node: node name
        :type node: str
        :param name: device manager name
        :type name: str
        :param limit: number of statuses to return
        :type limit: int
        :returns: list of history entries
        :rtype: [:class:`Entry`]
        """
        rows = self.database.query("""
            select status from history
            where node=? and name=?
            order by timestamp desc
            limit ?""",
            (node, name, limit or -1))
        entries = []
        for row in rows:
            status = JSONSerializable.fromJSON(row["status"])
            entries.append(Entry(status.timestamp, status))
        return entries

    def getAll(self):
        """
        Get status history for all device managers on all nodes

        :returns: list of history entries
        :rtype: [:class:`Entry`]
        """
        rows = self.database.query("""
            select status from history
            where name is not null
            order by timestamp desc""")
        entries = []
        for row in rows:
            status = JSONSerializable.fromJSON(row["status"])
            entries.append(Entry(status.timestamp, status))
        return entries

    def getLatest(self, node, name):
        """
        Get latest status for device manager with specified name on specified node

        :param node: node name
        :type node: str
        :param name: device manager name
        :type name: str
        :returns: history entry
        :rtype: :class:`Entry`
        """
        rows = self.database.query(
            """
            select status from status
            where node=? and name=?""",
            (node, name))
        if rows:
            status = JSONSerializable.fromJSON(rows[0]["status"])
            return Entry(status.timestamp, status)
        return None

    def remove(self, node=None, name=None):
        """
        Remove status history for device managers with specified names on specified nodes.

        node and name:
            remove history for specific device on a specific node

        node and no name
            remove history for all devices on a specific node

        no node and name
            remove history for specific device on all nodes

        no node and no name
            remove history for all devices on all nodes

        :param node: node name
        :type node: str
        :param name: device manager name
        :type name: str
        """
        self.database.write("begin")
        if node:
            if name:
                # remove history for specific device on a specific node
                self.database.write("""
                    delete from status
                    where node=? and name=?""",
                    (node, name))
                self.database.write("""
                    delete from history
                    where node=? and name=?""",
                    (node, name))
            else:
                # remove history for all devices on a specific node
                self.database.write("""
                    delete from status
                    where node=? and name is not null""",
                    (node,))
                self.database.write("""
                    delete from history
                    where node=? and name is not null""",
                    (node,))
        else:
            if name:
                # remove history for specific device on all nodes
                self.database.write("""
                    delete from status
                    where name=?""",
                    (name,))
                self.database.write("""
                    delete from history
                    where name=?""",
                    (name,))
            else:
                # remove history for all devices on all nodes
                self.database.write("""
                    delete from status
                    where name is not null""")
                self.database.write("""
                    delete from history
                    where name is not null""")
        self.database.write("commit")

@ClassLogger
class SharedSqliteDBKeyValueStore(BackendKeyValueStore):
    """
    Shared SQLite database backend key-value store implementation

    :param database: database manager
    :type database: :class:`~DBManager`
    """
    def __init__(self, database):
        self.database = database

    def delete(self, key):
        """
        Delete value at specified key

        :param key: key
        :type key: str
        """
        try:
            self.database.writeCommit("""
                delete from key_value_store
                where key=?""",
                (key,))
        except Exception as exception:
            self.log.error(exception)

    def deletePrefix(self, keyPrefix):
        """
        Delete all values with the specified key prefix

        :param keyPrefix: key prefix
        :type keyPrefix: str
        """
        try:
            # note that we cannot use the parameterized version with like
            self.database.writeCommit("""
                delete from key_value_store
                where key like '{keyPrefix}%'""".format(
                    keyPrefix=keyPrefix))
        except Exception as exception:
            self.log.error(exception)

    def get(self, key, default=None):
        """
        Get value at specified key

        :param key: key
        :type key: str
        :param default: default value to return if key not found
        :returns: value or default value
        :rtype: str
        """
        try:
            rows = self.database.query("""
                select value from key_value_store
                where key=?""",
                (key,))
            if rows:
                return rows[0]["value"]
        except Exception as exception:
            self.log.error(exception)
        return default

    def getAll(self):
        """
        Get all key-value pairs

        :returns: key-value pairs
        :rtype: [(key, value), ...]
        """
        try:
            rows = self.database.query("select key, value from key_value_store")
            return [
                (row["key"], row["value"])
                for row in rows
            ]
        except Exception as exception:
            self.log.error(exception)
        return []

    def getPrefix(self, keyPrefix):
        """
        Get all key-value pairs with the specified key prefix

        :param keyPrefix: key prefix
        :type keyPrefix: str
        :returns: key-value pairs
        :rtype: [(key, value), ...]
        """
        try:
            rows = self.database.query("""
                select key, value from key_value_store
                where key like '{keyPrefix}%'""".format(
                    keyPrefix=keyPrefix))
            return [
                (row["key"], row["value"])
                for row in rows
            ]
        except Exception as exception:
            self.log.error(exception)
        return []

    def put(self, key, value):
        """
        Put value at specified key

        :param key: key
        :type key: str
        :param value: value
        :type value: str
        """
        try:
            self.database.writeCommit("""
                insert or replace into key_value_store (key, value) values (?, ?)""",
                (key, value))
        except Exception as exception:
            self.log.error(exception)

    @property
    def transaction(self):
        """
        A transaction to perform puts and deletes in an atomic manner

        :returns: transaction object
        """
        return SharedSqliteDBTransaction(self.database)

@ClassLogger
class SharedSqliteDBNodeHistory(NodeHistory):
    """
    Shared SQLite database backend node history implementation

    :param database: database manager
    :type database: :class:`~DBManager`
    """
    def __init__(self, database):
        self.database = database

    def add(self, node, status, ttl=None):
        """
        Add status for system manager with on specified node

        :param node: node name
        :type node: str
        :param status: status
        :type status: :class:`SystemManagerStatus`
        :param ttl: time to live (in seconds), infinite by default
        :type ttl: int
        """
        if not isinstance(status, SystemManagerStatus):
            raise ValueError("'{0}' needs to be a '{1}'".format(status, SystemManagerStatus))

        if ttl is not None:
            raise NotImplementedError("SQLite does not support time to live attributes")

        timestamp = status.timestamp.toISOFormattedString()
        serializedStatus = status.toJSON(includeClassInfo=True)

        self.database.write("begin")
        self.database.write("""
            insert into history (node, name, timestamp, status) values (?, ?, ?, ?)""",
            (node, None, timestamp, serializedStatus))
        updated = self.database.write("""
            update status set status=? where node=? and name is null""",
            (serializedStatus, node))
        if updated < 1:
            self.database.write("""
                insert into status (node, name, status) values (?, ?, ?)""",
                (node, None, serializedStatus))
        self.database.write("commit")

    def get(self, node, limit=None):
        """
        Get status history for system manager on specified node

        :param node: node name
        :type node: str
        :param limit: number of statuses to return
        :type limit: int
        :returns: list of history entries
        :rtype: [:class:`Entry`]
        """
        rows = self.database.query("""
            select status from history
            where node=? and name is null
            order by timestamp desc
            limit ?""",
            (node, limit or -1))
        entries = []
        for row in rows:
            status = JSONSerializable.fromJSON(row["status"])
            entries.append(Entry(status.timestamp, status))
        return entries

    def getAll(self):
        """
        Get status history for all system managers on all nodes

        :returns: list of history entries
        :rtype: [:class:`Entry`]
        """
        rows = self.database.query("""
            select status from history
            where name is null
            order by timestamp desc""")
        entries = []
        for row in rows:
            status = JSONSerializable.fromJSON(row["status"])
            entries.append(Entry(status.timestamp, status))
        return entries

    def getLatest(self, node):
        """
        Get latest status for system manager on specified node

        :param node: node name
        :type node: str
        :returns: history entry
        :rtype: :class:`Entry`
        """
        rows = self.database.query(
            """
            select status from status
            where node=? and name is null""",
            (node,))
        if rows:
            status = JSONSerializable.fromJSON(rows[0]["status"])
            return Entry(status.timestamp, status)
        return None

    def remove(self, node=None):
        """
        Remove status history for system managers on specified nodes.

        node:
            remove history for specific node

        no node
            remove history for all nodes

        :param node: node name
        :type node: str
        """
        self.database.write("begin")
        if node:
            # remove history for specific node
            self.database.write("""
                delete from status
                where node=? and name is null""",
                (node,))
            self.database.write("""
                delete from history
                where node=? and name is null""",
                (node,))
        else:
            # remove history for all nodes
            self.database.write("""
                delete from status
                where name is null""")
            self.database.write("""
                delete from history
                where name is null""")
        self.database.write("commit")

@ClassLogger
class SharedSqliteDBTransaction(object):
    """
    Shared SQLite database transaction that behaves like a regular database
    transaction that automatically begins, wrapping multiple statements and
    then commits

    :param database: database manager
    :type database: :class:`~DBManager`
    :param statements: statements
    :type statements: [:class:`~etcd3.transactions.Delete` or etcd3.transactions.Put]
    """
    def __init__(self, database, statements=None):
        self.database = database
        self.statements = statements if statements else []

    def commit(self):
        """
        Commit all non-committed statements
        """
        if not self.statements:
            self.log.warn("No statements to commit")
            return
        self.database.write("begin")
        for statement in self.statements:
            self.database.write(*statement)
        self.database.write("commit")
        del self.statements[:]

    def delete(self, key):
        """
        Add a delete statement to the transaction to delete the specified key

        :param key: key
        :type key: str
        """
        self.statements.append(("""
            delete from key_value_store
            where key=?""",
            (key,)))
        return self

    def put(self, key, value, lease=None):
        """
        Add a put statement to the transaction to put the value at the specified key

        :param key: key
        :type key: str
        :param value: value
        :type value: str
        """
        self.statements.append(("""
            insert or replace into key_value_store (key, value) values (?, ?)""",
            (key, value)))
        return self
