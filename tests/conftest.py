import logging
import os
import shutil
import tempfile

import pytest

from c4.system.configuration import (Configuration,
                                     DBClusterInfo, DeviceInfo,
                                     NodeInfo,
                                     PlatformInfo,
                                     Roles)
from c4.system.manager import SystemManager


logging.basicConfig(format='%(asctime)s [%(levelname)s] <%(processName)s> [%(name)s(%(filename)s:%(lineno)d)] - %(message)s', level=logging.INFO)
log = logging.getLogger(__name__)

@pytest.fixture()
def cleandir(request):
    """
    Create a new temporary directory and change the current working directory to it
    """
    oldCurrentWorkingDirectory = os.getcwd()
    newCurrentWorkingDirectory = tempfile.mkdtemp(dir="/dev/shm")
#     newCurrentWorkingDirectory = tempfile.mkdtemp(dir="/tmp")
    os.chdir(newCurrentWorkingDirectory)

    def removeTemporaryDirectory():
        os.chdir(oldCurrentWorkingDirectory)
        shutil.rmtree(newCurrentWorkingDirectory)
    request.addfinalizer(removeTemporaryDirectory)
    return newCurrentWorkingDirectory

@pytest.fixture
def system(request, temporaryDatabasePaths, cleandir, temporaryIPCPath):
    """
    Set up a basic system configuration
    """
    configuration = Configuration()
    platform = PlatformInfo("im-devops", "c4.system.platforms.devops.IMDevOps")
    configuration.addPlatform(platform)

    node1 = NodeInfo("rack1-master1", "tcp://127.0.0.1:5000", role=Roles.ACTIVE)
    node1.addDevice(DeviceInfo("info", "c4.system.devices.cluster.info.Info"))
    node1.addDevice(DeviceInfo("cpu", "c4.system.devices.cpu.Cpu"))
    node1.addDevice(DeviceInfo("unknown", "c4.system.devices.Unknown"))
    node1.addDevice(DeviceInfo("disk", "c4.system.devices.disk.Disk"))
    node1.addDevice(DeviceInfo("memory", "c4.system.devices.mem.Memory"))

    configuration.addNode(node1)
    # TODO: this should automatically set role of the node to active
    configuration.addAlias("system-manager", "rack1-master1")

    node2 = NodeInfo("rack1-master2", "tcp://127.0.0.1:6000", role=Roles.PASSIVE)
    node2.addDevice(DeviceInfo("cpu", "c4.system.devices.cpu.Cpu"))
    node2.addDevice(DeviceInfo("memory", "c4.system.devices.mem.Memory"))
    configuration.addNode(node2)
    node3 = NodeInfo("rack1-master3", "tcp://127.0.0.1:7000")
    node3.addDevice(DeviceInfo("cpu", "c4.system.devices.cpu.Cpu"))
    node3.addDevice(DeviceInfo("memory", "c4.system.devices.mem.Memory"))
    configuration.addNode(node3)
    log.debug(configuration.toInfo().toJSON(pretty=True))

    systemSetup = {}
    node1ClusterInfo = DBClusterInfo("rack1-master1", "tcp://127.0.0.1:5000", "tcp://127.0.0.1:5000", role=Roles.ACTIVE)
    systemSetup["rack1-master1"] = SystemManager("rack1-master1", node1ClusterInfo)
    node2ClusterInfo = DBClusterInfo("rack1-master2", "tcp://127.0.0.1:6000", "tcp://127.0.0.1:5000", role=Roles.PASSIVE)
    systemSetup["rack1-master2"] = SystemManager("rack1-master2", node2ClusterInfo)
    node3ClusterInfo = DBClusterInfo("rack1-master3", "tcp://127.0.0.1:7000", "tcp://127.0.0.1:5000")
    systemSetup["rack1-master3"] = SystemManager("rack1-master3", node3ClusterInfo)

    def systemTeardown():
        log.debug("clean up")

        systemManagerNode = Configuration().getSystemManagerNodeName()
        activeSystemManager = systemSetup.pop(systemManagerNode)

        for node, systemManager in systemSetup.items():
            log.debug("stopping %s", node)
            systemManager.stop()

        log.debug("stopping active system manager %s", systemManagerNode)
        activeSystemManager.stop()

    request.addfinalizer(systemTeardown)

    return systemSetup

@pytest.fixture
def temporaryDatabasePaths(request, monkeypatch):
    """
    Create a new temporary directory and set c4.system.db.BACKUP_PATH
    and c4.system.db.DATABASE_PATH to it
    """
    newpath = tempfile.mkdtemp(dir="/dev/shm")
#     newpath = tempfile.mkdtemp(dir="/tmp")
    monkeypatch.setattr("c4.system.db.BACKUP_PATH", newpath)
    monkeypatch.setattr("c4.system.db.DATABASE_PATH", newpath)

    def removeTemporaryDirectory():
        shutil.rmtree(newpath)
    request.addfinalizer(removeTemporaryDirectory)
    return newpath

@pytest.fixture
def temporaryIPCPath(request, monkeypatch):
    """
    Create a new temporary directory and set c4.messaging.zeromqMessaging.DEFAULT_IPC_PATH to it
    """
    newpath = tempfile.mkdtemp(dir="/dev/shm")
#     newpath = tempfile.mkdtemp(dir="/tmp")
    monkeypatch.setattr("c4.messaging.zeromqMessaging.DEFAULT_IPC_PATH", newpath)

    def removeTemporaryDirectory():
        shutil.rmtree(newpath)
    request.addfinalizer(removeTemporaryDirectory)
    return newpath
