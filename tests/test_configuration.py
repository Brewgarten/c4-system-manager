import logging

import pytest

from c4.system.backend import Backend
from c4.system.configuration import (ConfigurationInfo, ConfigurationMissingSystemManagerAliasError,
                                     ConfigurationNameMismatchError, ConfigurationMissingAliasNodeError,
                                     ConfigurationMissingActiveNodeError, ConfigurationTooManyActiveNodesError,
                                     DeviceInfo,
                                     NodeInfo,
                                     PlatformInfo,
                                     Roles,
                                     States)


log = logging.getLogger(__name__)


@pytest.fixture
def nodes():
    """
    Basic set of nodes
    """
    node1 = NodeInfo("node1", "tcp://1.2.3.4:5000", role=Roles.ACTIVE)
    node2 = NodeInfo("node2", "tcp://5.6.7.8:5000", role=Roles.PASSIVE)
    node3 = NodeInfo("node3", "tcp://9.10.11.12:5000", role=Roles.THIN)
    systemSetup = {
       node1.name: node1,
       node2.name: node2,
       node3.name: node3
    }
    return systemSetup

def test_aliases(backend, nodes):

    configuration = Backend().configuration

    configuration.addNode(nodes["node1"])

    assert configuration.addAlias("system-manager", nodes["node1"].name)
    # check adding alias for node that does not exist
    assert configuration.addAlias("system-manager", "nonExistingNode") is None
    # alias already exists
    assert configuration.addAlias("system-manager", nodes["node2"].name) is None

    assert configuration.resolveAlias("system-manager") == nodes["node1"].name
    # check alias that does not exist
    assert configuration.resolveAlias("nonExistingAlias") is None

    configuration.addNode(nodes["node2"])

    assert configuration.changeAlias("system-manager", nodes["node2"].name)
    # check alias that does not exist
    assert configuration.changeAlias("nonExistingAlias", "test") is None

    # check shortcut system manager alias
    assert configuration.getSystemManagerNodeName() == nodes["node2"].name

def test_clear(backend, nodes):

    configuration = Backend().configuration

    for node in nodes.values():
        configuration.addNode(node)

    configuration.addAlias("system-manager", nodes["node1"].name)

    platform = PlatformInfo("im-devops", "c4.system.platforms.devops.IMDevOps", "development platform", {"test": 0})
    configuration.addPlatform(platform)

    configuration.clear()

    # check that nodes are cleared
    assert set(configuration.getNodeNames()) == set()

    # check that aliases are cleared
    assert configuration.getSystemManagerNodeName() is None

    # check that platform is cleared
    platformInfo = configuration.getPlatform()
    assert platformInfo.name != platform.name
    assert platformInfo.type != platform.type
    assert platformInfo.description != platform.description
    assert platformInfo.settings != platform.settings

def test_clusterInfo(backend, nodes):

    # setup test configuration
    configuration = Backend().configuration
    platform = PlatformInfo("im-devops", "c4.system.platforms.devops.IMDevOps")
    platform.settings["my_timer_interval"] = 3000
    configuration.addPlatform(platform)

    db2Instance1 = DeviceInfo("instance1", "c4.devices.db2.Instance")
    db2Instance1.addDevice(DeviceInfo("mln1", "c4.devices.db2.MLN"))
    db2Instance1.addDevice(DeviceInfo("mln2", "c4.devices.db2.MLN"))
    db2Instance1.addDevice(DeviceInfo("mln3", "c4.devices.db2.MLN"))
    db2Instance1.addDevice(DeviceInfo("mln4", "c4.devices.db2.MLN"))

    db2 = DeviceInfo("db2", "c4.devices.db2.DB2")
    db2.addDevice(db2Instance1)

    node1 = nodes["node1"]
    node1.addDevice(db2)

    configuration.addNode(node1)
    configuration.addAlias("system-manager", node1.name)

    node2 = nodes["node2"]
    node2.addDevice(DeviceInfo("cpu", "c4.devices.cpu.Cpu"))
    node2.addDevice(DeviceInfo("memory", "c4.devices.mem.Memory"))
    configuration.addNode(node2)

    dbClusterInfo = Backend().ClusterInfo(nodes["node1"].name,
                                          nodes["node1"].address,
                                          nodes["node1"].address,
                                          Roles.ACTIVE,
                                          States.DEPLOYED)

    # check db cluster information for ACTIVE role
    # need to check aliases individually because itearting over a shared dict isn't supported
    for key, value in configuration.getAliases().iteritems():
        assert dbClusterInfo.aliases[key] == value
    assert dbClusterInfo.getNodeAddress(nodes["node1"].name) == nodes["node1"].address
    assert dbClusterInfo.getNodeAddress(nodes["node2"].name) == nodes["node2"].address
    assert dbClusterInfo.getNodeAddress("system-manager") == nodes["node1"].address
    assert dbClusterInfo.getNodeAddress("nonExistingNode") is None
    assert set(dbClusterInfo.nodeNames) == set([nodes["node1"].name, nodes["node2"].name])
    assert dbClusterInfo.role == Roles.ACTIVE
    assert dbClusterInfo.state == States.DEPLOYED
    assert dbClusterInfo.systemManagerAddress == nodes["node1"].address

    # TODO: need to revisit this check since by default backend is shared now
#     # check db cluster information for THIN role
#     dbClusterInfo.role = Roles.THIN
#     assert dbClusterInfo.aliases == {}
#     assert dbClusterInfo.getNodeAddress(nodes["node1"].name) == nodes["node1"].address
#     assert dbClusterInfo.getNodeAddress(nodes["node2"].name) is None
#     assert dbClusterInfo.getNodeAddress("system-manager") == nodes["node1"].address
#     assert dbClusterInfo.getNodeAddress("nonExistingNode") is None
#     assert set(dbClusterInfo.nodeNames) == set([nodes["node1"].name, "system-manager"])
#     assert dbClusterInfo.role == Roles.THIN
#     assert dbClusterInfo.state == States.DEPLOYED
#     assert dbClusterInfo.systemManagerAddress == nodes["node1"].address

def test_properties(backend, nodes):

    configuration = Backend().configuration

    configuration.addNode(nodes["node1"])

    # change a node property
    assert configuration.changeProperty(nodes["node1"].name, None, "address", "tcp://1.1.1.1:10000") == nodes["node1"].address
    properties = configuration.getProperties(nodes["node1"].name)
    assert properties == {"address": "tcp://1.1.1.1:10000"}

    # check node property that does not exist
    assert configuration.changeProperty(nodes["node1"].name, None, "nonExistingProperty", "test") is None
    assert configuration.getProperty(nodes["node1"].name, None, "nonExistingProperty") is None
    assert configuration.changeProperty(nodes["node1"].name, None, "nonExistingProperty", "test", setIfNotExist=True) is None
    assert configuration.getProperty(nodes["node1"].name, None, "nonExistingProperty") == "test"

    # add a new node property
    configuration.changeProperty(nodes["node1"].name, None, "testProperty", "test", setIfNotExist=True)
    assert configuration.getProperty(nodes["node1"].name, None, "testProperty") == "test"

    # remove a node property
    configuration.removeProperty(nodes["node1"].name, None, "testProperty")
    configuration.removeProperty(nodes["node1"].name, None, "nonExistingProperty")
    assert configuration.getProperty(nodes["node1"].name, None, "testProperty") is None

    configuration.addNode(nodes["node2"])

    disk = DeviceInfo("disk", "c4.devices.disk.Disk")
    disk.properties["disks"] = "*"
    configuration.addDevice(nodes["node2"].name, "disk", disk)

    # change a device property
    assert configuration.changeProperty(nodes["node2"].name, disk.name, "disks", "sda") == disk.properties["disks"]
    properties = configuration.getProperties(nodes["node2"].name, disk.name)
    assert properties == {"disks": "sda"}

    # check device property that does not exist
    assert configuration.changeProperty(nodes["node2"].name, disk.name, "nonExistingProperty", "test") is None
    assert configuration.getProperty(nodes["node2"].name, None, "nonExistingProperty") is None
    assert configuration.changeProperty(nodes["node2"].name, None, "nonExistingProperty", "test", setIfNotExist=True) is None
    assert configuration.getProperty(nodes["node2"].name, None, "nonExistingProperty") == "test"

    configuration.removeProperty(nodes["node2"].name, disk.name, "properties")
    configuration.removeProperty(nodes["node2"].name, disk.name, "nonExistingProperty")
    assert configuration.getProperty(nodes["node2"].name, disk.name, "properties") is None

def test_devices(backend, nodes):

    configuration = Backend().configuration

    configuration.addNode(nodes["node1"])
    node1DeviceNames = set()

    db2 = DeviceInfo("db2", "c4.devices.db2.DB2")
    db2Info = configuration.addDevice(nodes["node1"].name, "db2", db2)
    assert db2Info

    # cannot add the same device twice
    assert configuration.addDevice(nodes["node1"].name, "db2", db2) is None
    node1DeviceNames.add("db2")

    # check that we can get the device
    db2DeviceInfo = configuration.getDevice(nodes["node1"].name, "db2")
    assert db2DeviceInfo.name == db2.name
    assert db2DeviceInfo.properties == db2.properties
    assert db2DeviceInfo.devices == db2.devices
    assert db2DeviceInfo.type == db2.type

    # check non existing device
    assert configuration.getNode("nonExistingNode") is None
    assert configuration.getDevice("nonExistingNode", "db2.instance1") is None
    assert configuration.getDevice(nodes["node1"].name, "db2.nonExistingDevice") is None

    # make sure child devices are added as well
    parentDevice = DeviceInfo("parent", "c4.devices.test.Test")
    node1DeviceNames.add("parent")
    for childNumber in range(4):
        parentDevice.addDevice(DeviceInfo("child{0}".format(childNumber+1), "c4.devices.test.Test"))
        node1DeviceNames.add("parent.child{0}".format(childNumber+1))
    configuration.addDevice(nodes["node1"].name, "parent", parentDevice)

    node1Devices = configuration.getDevices(nodes["node1"].name, flatDeviceHierarchy=True)
    assert node1Devices["parent"]
    for childNumber in range(4):
        assert node1Devices["parent.child{0}".format(childNumber+1)]

    db2Instance1 = DeviceInfo("instance1", "c4.devices.db2.Instance")
    db2Instance1Info = configuration.addDevice(nodes["node1"].name, "db2.instance1", db2Instance1)
    assert db2Instance1Info
    node1DeviceNames.add("db2.instance1")

    for mlnNumber in range(4):
        mln = DeviceInfo("mln{0}".format(mlnNumber+1), "c4.devices.db2.MLN")
        mlnInfo = configuration.addDevice(nodes["node1"].name, "db2.instance1.{0}".format(mln.name), mln)
        assert mlnInfo
        node1DeviceNames.add("db2.instance1.{0}".format(mln.name))

    node1Info = configuration.getNode(nodes["node1"].name)
    assert node1Info.devices["db2"]
    assert node1Info.devices["db2"].devices["instance1"]
    for mlnNumber in range(4):
        assert node1Info.devices["db2"].devices["instance1"].devices["mln{0}".format(mlnNumber+1)]

    # get device information with child devices
    db2DeviceInfo = configuration.getDevice(nodes["node1"].name, "db2")
    assert db2DeviceInfo.devices
    assert db2DeviceInfo.devices["instance1"]
    db2Instance1Info = configuration.getDevice(nodes["node1"].name, "db2.instance1")
    for mlnNumber in range(4):
        assert db2Instance1Info.devices["mln{0}".format(mlnNumber+1)]

    # check flat hierarchy
    assert set(configuration.getNode(nodes["node1"].name, flatDeviceHierarchy=True).devices.keys()) == node1DeviceNames

    configuration.addNode(nodes["node2"])

    cpu = DeviceInfo("cpu", "c4.devices.cpu.Cpu")
    cpuInfo = configuration.addDevice(nodes["node2"].name, "cpu", cpu)
    assert cpuInfo

    disk = DeviceInfo("disk", "c4.devices.disk.Disk")
    disk.properties["disks"] = "*"
    diskInfo = configuration.addDevice(nodes["node2"].name, "disk", disk)
    assert diskInfo

    memory = DeviceInfo("memory", "c4.devices.mem.Memory")
    memoryInfo = configuration.addDevice(nodes["node2"].name, "memory", memory)
    assert memoryInfo

    node2Info = configuration.getNode(nodes["node2"].name)
    assert node2Info.devices["cpu"]
    assert node2Info.devices["disk"]
    assert node2Info.devices["disk"].properties["disks"] == "*"
    assert node2Info.devices["memory"]

    # remove devices and its children
    configuration.removeDevice(nodes["node1"].name, "db2")
    configuration.removeDevice(nodes["node1"].name, "nonExistingDevice")
    assert configuration.getDevice(nodes["node1"].name, "db2") is None

def test_devices_no_sqlite_cte(monkeypatch, nodes, temporaryBackend):
    """
    Test without support for common table expressions
    """
    monkeypatch.setattr("sqlite3.sqlite_version", "3.8.2")
    configuration = Backend().configuration

    configuration.addNode(nodes["node1"])
    node1DeviceNames = set()

    db2 = DeviceInfo("db2", "c4.devices.db2.DB2")
    db2Info = configuration.addDevice(nodes["node1"].name, "db2", db2)
    assert db2Info._id > 0
    assert db2Info._parentId > 0
    # cannot add the same device twice
    assert configuration.addDevice(nodes["node1"].name, "db2", db2) is None
    node1DeviceNames.add("db2")

    # check that we can get the device
    db2DeviceInfo = configuration.getDevice(nodes["node1"].name, "db2")
    assert db2DeviceInfo._id > 0
    assert db2DeviceInfo._parentId > 0
    assert db2DeviceInfo.name == db2.name
    assert db2DeviceInfo.properties == db2.properties
    assert db2DeviceInfo.devices == db2.devices
    assert db2DeviceInfo.type == db2.type

    # check non existing device
    assert configuration.getNode("nonExistingDevice") is None
    assert configuration.getDevice("nonExistingNode", "db2.instance1") is None
    assert configuration.getDevice(nodes["node1"].name, "db2.nonExistingDevice") is None

    # make sure child devices are added as well
    parentDevice = DeviceInfo("parent", "c4.devices.test.Test")
    node1DeviceNames.add("parent")
    for childNumber in range(4):
        parentDevice.addDevice(DeviceInfo("child{0}".format(childNumber+1), "c4.devices.test.Test"))
        node1DeviceNames.add("parent.child{0}".format(childNumber+1))
    configuration.addDevice(nodes["node1"].name, "parent", parentDevice)

    node1Devices = configuration.getDevices(nodes["node1"].name, flatDeviceHierarchy=True)
    assert node1Devices["parent"]._id > 0
    assert node1Devices["parent"]._parentId > 0
    for childNumber in range(4):
        assert node1Devices["parent.child{0}".format(childNumber+1)]._id > 0
        assert node1Devices["parent.child{0}".format(childNumber+1)]._parentId > 0

    db2Instance1 = DeviceInfo("instance1", "c4.devices.db2.Instance")
    db2Instance1Info = configuration.addDevice(nodes["node1"].name, "db2.instance1", db2Instance1)
    assert db2Instance1Info._id > 0
    assert db2Instance1Info._parentId > 0
    node1DeviceNames.add("db2.instance1")

    for mlnNumber in range(4):
        mln = DeviceInfo("mln{0}".format(mlnNumber+1), "c4.devices.db2.MLN")
        mlnInfo = configuration.addDevice(nodes["node1"].name, "db2.instance1.{0}".format(mln.name), mln)
        assert mlnInfo._id > 0
        assert mlnInfo._parentId > 0
        node1DeviceNames.add("db2.instance1.{0}".format(mln.name))

    node1Info = configuration.getNode(nodes["node1"].name)
    assert node1Info.devices["db2"]
    assert node1Info.devices["db2"].devices["instance1"]
    for mlnNumber in range(4):
        assert node1Info.devices["db2"].devices["instance1"].devices["mln{0}".format(mlnNumber+1)]

    # get device information with child devices
    db2DeviceInfo = configuration.getDevice(nodes["node1"].name, "db2")
    assert db2DeviceInfo.devices
    assert db2DeviceInfo.devices["instance1"]
    db2Instance1Info = configuration.getDevice(nodes["node1"].name, "db2.instance1")
    for mlnNumber in range(4):
        assert db2Instance1Info.devices["mln{0}".format(mlnNumber+1)]

    # check flat hierarchy
    assert set(configuration.getNode(nodes["node1"].name, flatDeviceHierarchy=True).devices.keys()) == node1DeviceNames

    configuration.addNode(nodes["node2"])

    cpu = DeviceInfo("cpu", "c4.devices.cpu.Cpu")
    cpuInfo = configuration.addDevice(nodes["node2"].name, "cpu", cpu)
    assert cpuInfo._id > 0
    assert cpuInfo._parentId > 0

    disk = DeviceInfo("disk", "c4.devices.disk.Disk")
    disk.properties["disks"] = "*"
    diskInfo = configuration.addDevice(nodes["node2"].name, "disk", disk)
    assert diskInfo._id > 0
    assert diskInfo._parentId > 0

    memory = DeviceInfo("memory", "c4.devices.mem.Memory")
    memoryInfo = configuration.addDevice(nodes["node2"].name, "memory", memory)
    assert memoryInfo._id > 0
    assert memoryInfo._parentId > 0

    node2Info = configuration.getNode(nodes["node2"].name)
    assert node2Info.devices["cpu"]
    assert node2Info.devices["disk"]
    assert node2Info.devices["disk"].properties["disks"] == "*"
    assert node2Info.devices["memory"]

    # remove devices and its children
    configuration.removeDevice(nodes["node1"].name, "db2")
    configuration.removeDevice(nodes["node1"].name, "nonExistingDevice")
    assert configuration.getDevice(nodes["node1"].name, "db2") is None

def test_json(backend, nodes):

    configuration = Backend().configuration

    for node in nodes.values():
        configuration.addNode(node)

    configuration.addAlias("system-manager", nodes["node1"].name)
    configuration.addAlias("backup-node", nodes["node2"].name)

    platform = PlatformInfo(name="test",
                            platformType="c4.system.platforms.Test",
                            description="test platform",
                            settings={
                                "setting1": 1,
                                "setting2": "test"
                            })
    configuration.addPlatform(platform)

    db2 = DeviceInfo("db2", "c4.devices.db2.DB2")

    db2Instance1 = DeviceInfo("instance1", "c4.devices.db2.Instance")
    db2.addDevice(db2Instance1)

    for mlnNumber in range(4):
        mln = DeviceInfo("mln{0}".format(mlnNumber+1), "c4.devices.db2.MLN")
        db2Instance1.addDevice(mln)

    configuration.addDevice(nodes["node1"].name, "db2", db2)

    cpu = DeviceInfo("cpu", "c4.devices.cpu.Cpu")
    configuration.addDevice(nodes["node2"].name, "cpu", cpu)

    disk = DeviceInfo("disk", "c4.devices.disk.Disk")
    disk.properties["disks"] = "*"
    configuration.addDevice(nodes["node2"].name, "disk", disk)

    memory = DeviceInfo("memory", "c4.devices.mem.Memory")
    configuration.addDevice(nodes["node2"].name, "memory", memory)

    # check conversion to configuration info
    configurationInfo = configuration.toInfo()
    assert configurationInfo.aliases == configuration.getAliases()
    assert configurationInfo.nodes["node1"] == configuration.getNode(nodes["node1"].name)
    assert configurationInfo.nodes["node2"] == configuration.getNode(nodes["node2"].name)
    assert configurationInfo.nodes["node3"] == configuration.getNode(nodes["node3"].name)
    assert configurationInfo.platform == configuration.getPlatform()

    # serialize to JSON and load back into configuration info
    configurationJSONString = configurationInfo.toJSON(includeClassInfo=True, pretty=True)
    loadedConfigurationInfo = ConfigurationInfo.fromJSON(configurationJSONString)

    # check that transient information is reset
    for loadedNodeInfo in loadedConfigurationInfo.nodes.values():
        assert loadedNodeInfo
        assert loadedNodeInfo.state == States.DEPLOYED

    # check that transient information is reset
    loadedDB2Info = loadedConfigurationInfo.nodes["node1"].devices["db2"]
    assert loadedDB2Info

    # make sure that loaded configuration info matches existing configuration
    assert loadedConfigurationInfo.aliases == configuration.getAliases()
    assert loadedConfigurationInfo.nodes["node1"] == configuration.getNode(nodes["node1"].name)
    assert loadedConfigurationInfo.nodes["node2"] == configuration.getNode(nodes["node2"].name)
    assert loadedConfigurationInfo.nodes["node3"] == configuration.getNode(nodes["node3"].name)
    assert loadedConfigurationInfo.platform == configuration.getPlatform()

    # load configuration from configuration info
    configuration.clear()
    configuration.loadFromInfo(configurationInfo)

    # make sure that loaded configuration matches configuration info
    assert configuration.getAliases() == configurationInfo.aliases
    assert configuration.getNode(nodes["node1"].name) == configurationInfo.nodes["node1"]
    assert configuration.getNode(nodes["node2"].name) == configurationInfo.nodes["node2"]
    assert configuration.getNode(nodes["node3"].name) == configurationInfo.nodes["node3"]
    assert configuration.getPlatform() == configurationInfo.platform

def test_jsonInfos():

    # check node info
    node1 = NodeInfo("node1", "tcp://1.2.3.4:5000", Roles.ACTIVE)
    node1JSON = node1.toJSON(includeClassInfo=True, pretty=True)

    loadedNode1 = NodeInfo.fromJSON(node1JSON)
    assert loadedNode1.name == node1.name
    assert loadedNode1.address == node1.address
    assert loadedNode1.role == node1.role
    assert loadedNode1.state == node1.state

    # check device info
    device1 = DeviceInfo("test", "c4.devices.test.Test")
    device1JSON = device1.toJSON(includeClassInfo=True, pretty=True)

    loadedDevice1 = DeviceInfo.fromJSON(device1JSON)
    assert loadedDevice1.name == device1.name
    assert loadedDevice1.type == device1.type
    assert loadedDevice1.state == device1.state

    # check device hierarchy info
    device1Child = DeviceInfo("child", "c4.devices.test.Test")
    device1.addDevice(device1Child)
    device1JSON = device1.toJSON(includeClassInfo=True, pretty=True)

    loadedDevice1 = DeviceInfo.fromJSON(device1JSON)
    assert isinstance(loadedDevice1.devices["child"], DeviceInfo)
    assert loadedDevice1.devices["child"].name == device1Child.name
    assert loadedDevice1.devices["child"].type == device1Child.type
    assert loadedDevice1.devices["child"].state == device1Child.state

    # check node info with device
    node1.addDevice(device1)
    node1JSON = node1.toJSON(includeClassInfo=True, pretty=True)

    loadedNode1 = NodeInfo.fromJSON(node1JSON)
    assert isinstance(loadedNode1.devices[device1.name], DeviceInfo)
    assert isinstance(loadedNode1.devices[device1.name].devices[device1Child.name], DeviceInfo)

def test_nodes(backend, nodes):

    configuration = Backend().configuration

    # make sure child devices are added as well
    parentDevice = DeviceInfo("parent", "c4.devices.test.Test")
    for childNumber in range(5):
        parentDevice.addDevice(DeviceInfo("child{0}".format(childNumber+1), "c4.devices.test.Test"))
    nodes["node1"].addDevice(parentDevice)

    # add nodes
    for node in nodes.values():

        nodeInfo = configuration.addNode(node)
        assert nodeInfo

        nodeInfo = configuration.getNode(node.name)
        assert nodeInfo.name == node.name
        assert nodeInfo.address == node.address
        assert nodeInfo.role == node.role
        assert nodeInfo.state == node.state

    # cannot add node  that already exists
    assert configuration.addNode(nodes["node1"]) is None

    # check non existing node
    assert configuration.getNode("nonExistingNode") is None

    assert configuration.getAddress(nodes["node1"].name) == nodes["node1"].address
    # check non existing node address
    assert configuration.getAddress("nonExistingNode") is None

    configuration.addAlias("system-manager", nodes["node1"].name)
    assert configuration.getAddress("system-manager") == nodes["node1"].address
    # check non existing node address
    assert configuration.getAddress("nonExistingNodeAlias") is None

    assert set(configuration.getNodeNames()) == set([node.name for node in nodes.values()])

    # check that child devices were added correctly
    nodeInfo = configuration.getNode(nodes["node1"].name, includeDevices=True, flatDeviceHierarchy=True)
    assert nodeInfo.devices
    parentDevice = nodeInfo.devices["parent"]
    assert parentDevice
    for childNumber in range(5):
        childDevice = nodeInfo.devices["parent.child{0}".format(childNumber+1)]
        assert childDevice

    # remove node and its devices
    configuration.removeNode(nodes["node1"].name)
    configuration.removeNode("nonExistingNode")
    assert configuration.getNode(nodes["node1"].name) is None

def test_platform(backend):

    configuration = Backend().configuration
    platform = PlatformInfo("im-devops", "c4.system.platforms.devops.IMDevOps", "development platform", {"test": 0})
    platform.settings["my_timer_interval"] = 3000
    configuration.addPlatform(platform)

    platformInfo = configuration.getPlatform()
    assert platformInfo.name == platform.name
    assert platformInfo.type == platform.type
    assert platformInfo.description == platform.description
    assert platformInfo.settings == platform.settings

def test_resetDeviceStates(backend, nodes):

    configuration = Backend().configuration

    configuration.addNode(nodes["node1"])

    parentDevice = DeviceInfo("parent", "c4.devices.test.Test", state=States.MAINTENANCE)
    for childNumber in range(4):
        parentDevice.addDevice(DeviceInfo("child{0}".format(childNumber+1), "c4.devices.test.Test", state=States.STARTING))
    for childNumber in range(4):
        parentDevice.addDevice(DeviceInfo("child{0}".format(childNumber+5), "c4.devices.test.Test", state=States.RUNNING))
    configuration.addDevice(nodes["node1"].name, "parent", parentDevice)

    configuration.resetDeviceStates()

    node1Info = configuration.getNode(nodes["node1"].name, flatDeviceHierarchy=True)
    # make sure that the node state has not changed
    assert node1Info.state == States.DEPLOYED

    devices = node1Info.devices
    # make sure that the 'parent' device is still in MAINTENANCE
    assert devices.pop("parent").state == States.MAINTENANCE
    # make sure all other devices have been reset to REGISTERED
    for deviceInfo in devices.values():
        assert deviceInfo.state == States.REGISTERED

def test_roles(backend, nodes):

    configuration = Backend().configuration

    configuration.addNode(nodes["node1"])

    # change node role
    assert configuration.changeRole(nodes["node1"].name, Roles.THIN) == nodes["node1"].role
    assert configuration.getRole(nodes["node1"].name) == Roles.THIN

    # make sure that we only allow changing to proper roles
    assert configuration.changeRole(nodes["node1"].name, "testRole") is None
    assert configuration.getRole(nodes["node1"].name) == Roles.THIN

    # check non existing node
    assert configuration.getRole("nonExistingNode") is None

def test_states(backend, nodes):

    configuration = Backend().configuration

    configuration.addNode(nodes["node1"])

    db2 = DeviceInfo("db2", "c4.devices.db2.DB2")
    configuration.addDevice(nodes["node1"].name, "db2", db2)

    db2Instance1 = DeviceInfo("instance1", "c4.devices.db2.Instance")
    configuration.addDevice(nodes["node1"].name, "db2.instance1", db2Instance1)

    for mlnNumber in range(4):
        mln = DeviceInfo("mln{0}".format(mlnNumber+1), "c4.devices.db2.MLN")
        configuration.addDevice(nodes["node1"].name, "db2.instance1.{0}".format(mln.name), mln)

    assert configuration.changeState(nodes["node1"].name, None, States.MAINTENANCE) == nodes["node1"].state
    node1DeviceList = configuration.getDevices(nodes["node1"].name, flatDeviceHierarchy=True).values()
    for device in node1DeviceList:
        assert device.state == States.MAINTENANCE

    assert configuration.changeState(nodes["node1"].name, None, States.REGISTERED) == States.MAINTENANCE
    node1DeviceList = configuration.getDevices(nodes["node1"].name, flatDeviceHierarchy=True).values()
    for device in node1DeviceList:
        assert device.state == States.MAINTENANCE

    # single device
    assert configuration.changeState(nodes["node1"].name, "db2", States.REGISTERED) == States.MAINTENANCE
    assert configuration.getState(nodes["node1"].name, "db2") == States.REGISTERED

    # node does not exist
    assert configuration.changeState("testNode", None, States.RUNNING) is None
    assert configuration.getState("testNode", None) is None

    # device does not exist
    assert configuration.changeState(nodes["node1"].name, "testDevice", States.RUNNING) is None
    assert configuration.getState(nodes["node1"].name, "testDevice") is None

def test_targetStates(backend, nodes):

    configuration = Backend().configuration

    configuration.addNode(nodes["node1"])

    assert configuration.changeTargetState(nodes["node1"].name, None, States.MAINTENANCE) is None
    # check node that does not exist
    assert configuration.changeTargetState("nonExistingNode", None, States.MAINTENANCE) is None

    assert configuration.getTargetState(nodes["node1"].name) == States.MAINTENANCE
    # check node that does not exist
    assert configuration.getTargetState("nonExistingNode") is None

    configuration.removeTargetState(nodes["node1"].name)
    assert configuration.getTargetState(nodes["node1"].name) is None

    disk = DeviceInfo("disk", "c4.devices.disk.Disk")
    configuration.addDevice(nodes["node1"].name, "disk", disk)

    assert configuration.changeTargetState(nodes["node1"].name, disk.name, States.MAINTENANCE) is None
    # check device that does not exist
    assert configuration.changeTargetState(nodes["node1"].name, "nonExistingDevice", States.MAINTENANCE) is None

    assert configuration.getTargetState(nodes["node1"].name, disk.name) == States.MAINTENANCE
    # check device that does not exist
    assert configuration.getTargetState(nodes["node1"].name, "nonExistingDevice") is None

    configuration.removeTargetState(nodes["node1"].name, disk.name)
    assert configuration.getTargetState(nodes["node1"].name, disk.name) is None

def test_validation(backend, nodes):

    configurationInfo = ConfigurationInfo()

    configurationInfo.nodes[nodes["node1"].name] = nodes["node1"]
    configurationInfo.nodes[nodes["node2"].name] = nodes["node2"]

    # check invalid name to node info name mapping
    configurationInfo.nodes["invalidNodeName"] = nodes["node1"]
    with pytest.raises(ConfigurationNameMismatchError):
        configurationInfo.validate()
    del configurationInfo.nodes["invalidNodeName"]

    testDevice = DeviceInfo("test", "test")
    configurationInfo.nodes[nodes["node1"].name].devices["invalidDeviceName"] = testDevice
    with pytest.raises(ConfigurationNameMismatchError):
        configurationInfo.validate()
    del configurationInfo.nodes[nodes["node1"].name].devices["invalidDeviceName"]

    # check missing system manager alias
    with pytest.raises(ConfigurationMissingSystemManagerAliasError):
        configurationInfo.validate()
    configurationInfo.aliases["system-manager"] = nodes["node1"].name

    # check alias to missing node
    configurationInfo.aliases["test"] = "nonExistingNode"
    with pytest.raises(ConfigurationMissingAliasNodeError):
        configurationInfo.validate()
    del configurationInfo.aliases["test"]

    # check active nodes
    configurationInfo.nodes[nodes["node1"].name].role = Roles.ACTIVE
    configurationInfo.nodes[nodes["node2"].name].role = Roles.ACTIVE
    with pytest.raises(ConfigurationTooManyActiveNodesError):
        configurationInfo.validate()

    configurationInfo.nodes[nodes["node1"].name].role = Roles.PASSIVE
    configurationInfo.nodes[nodes["node2"].name].role = Roles.PASSIVE
    with pytest.raises(ConfigurationMissingActiveNodeError):
        configurationInfo.validate()
