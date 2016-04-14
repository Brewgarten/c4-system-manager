import datetime
import glob
import json
import logging
import os
import sqlite3

import pkg_resources

from c4.system.backend import BackendImplementation, BackendInfo
from c4.system.configuration import (Configuration,
                                     DeviceInfo,
                                     NodeInfo,
                                     PlatformInfo,
                                     Roles,
                                     SharedClusterInfo,
                                     States)
from c4.utils.logutil import ClassLogger
from c4.utils.util import getFullModuleName
from c4.utils.version import BasicVersion


# Lowest version of SQLite with Common Table Expression support
SqliteCTEMinimumVersion=BasicVersion("3.8.3")

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
        schema = pkg_resources.resource_string("c4.data", "sql/sysmgr.sql")  # @UndefinedVariable
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
            if (self.log.isEnabledFor(logging.DEBUG)):
                self.log.debug("Executing sql update/insert: \n%s\n%s\ntook %s",
                               str.strip(statement), "\n".join(str(p) for p in parameters), end-start)
            return cur.rowcount
        except sqlite3.Error, message:
            self.log.error("Could not execute sql update/insert: %s %s, %s", statement, parameters, message)

class SharedSqliteDBBackend(BackendImplementation):
    """
    Shared SQLite database backend implementation

    :param info: backend info
    :type info: :class:`~BackendInfo`
    """
    def __init__(self, info):
        super(SharedSqliteDBBackend, self).__init__(info)
        self.database = DBManager(self.info)
        self._configuration = SharedSqliteDBConfiguration(self.database)

    @property
    def configuration(self):
        return self._configuration

    def ClusterInfo(self, node, address, systemManagerAddress, role, state):
        return SharedClusterInfo(self, node, address, systemManagerAddress, role, state)

@ClassLogger
class SharedSqliteDBConfiguration(Configuration):
    """
    Shared SQLite database backend configuration implementation

    :param database: database manager
    :type database: :class:`~DBManager`
    """
    def __init__(self, database):
        self.database = database

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
        :type node: :class:`~c4.system.configuration.NodeInfo`
        :returns: node info with database ids
        :rtype: :class:`~c4.system.configuration.NodeInfo`
        """
        nodeInfo = self.getNode(node.name, includeDevices=False)
        if nodeInfo:
            self.log.error("node '%s' already exists", node.name)
            return None

        self.database.writeCommit("""
            insert into t_sm_configuration (name, state, type, details)
            values (?, ?, ?, ?)""",
            (node.name, node.state.name, "c4.system.manager.SystemManager", json.dumps(node.details)))
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
        self.database.writeMany("""
            insert into t_sm_platform (property, value) values (?, ?)""",
            ("name", platform.name),
            ("type", platform.type),
            ("description", platform.description),
            ("settings", json.dumps(platform.settings))
        )
        self.database.write("commit")

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

    def getAliases(self):
        """
        Get a mapping of aliases to node names

        :returns: mappings
        :rtype: dict
        """
        rows = self.database.query("select alias, node_name from t_sm_configuration_alias")
        return {row["alias"]: row["node_name"] for row in rows}

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

    def getTargetState(self, node, name=None):
        """
        Get the target state of a node or device manager.

        :param node: node
        :type node: str
        :param name: device manager name
        :type name: str
        :returns: :class:`~c4.system.configuration.States`
        """
        state = self.getDetail(node, name, "targetState");
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
            nodeDetails = json.loads(nodeDetailsJSON)
            nodeRole = Roles.valueOf(nodeDetails["role"])
            nodeState = States.valueOf(nodeRow["state"])
            nodeInfo = NodeInfo(nodeRow["name"], nodeDetails["address"], nodeRole, nodeState, nodeRow["id"])
            nodeInfo.details = nodeDetails

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
        self.database.writeMany("""
            delete from t_sm_configuration where id is ?""",
            *rowIds)

        # remove aliases for node
        self.database.writeCommit("""
            delete from t_sm_configuration_alias where node_name=?""",
            (node,))

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
