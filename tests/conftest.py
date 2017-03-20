import logging
import os
import shlex
import shutil
import subprocess
import tempfile
import time

import pytest

from c4.backends.etcdBackend import EtcdBackend
from c4.backends.sharedSQLite import SharedSqliteDBBackend
from c4.system.backend import Backend, BackendInfo
from c4.system.configuration import (DeviceInfo,
                                     NodeInfo,
                                     PlatformInfo,
                                     Roles, States)
from c4.system.manager import SystemManager


logging.basicConfig(format='%(asctime)s [%(levelname)s] <%(processName)s> [%(name)s(%(filename)s:%(lineno)d)] - %(message)s', level=logging.INFO)
log = logging.getLogger(__name__)

@pytest.fixture(params=[EtcdBackend, SharedSqliteDBBackend])
def backend(request):
    """
    Parameterized testing backend
    """
    if request.param == EtcdBackend:

        if not os.path.exists("/opt/etcd/etcd"):
            pytest.skip("could not find etcd installation in '/opt/etcd/'")

        newpath = tempfile.mkdtemp(dir="/tmp")

        # set up etcd process
        etcdCommand = shlex.split("/opt/etcd/etcd --data-dir {dataDir}".format(dataDir=newpath))
        etcdProcess = subprocess.Popen(etcdCommand, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        def finalizeBackend():
            # stop etcd process
            etcdProcess.terminate()
            etcdProcess.wait()

            # remove data directory
            shutil.rmtree(newpath)

        # make sure etcd process is started
        clusterHealthCommand = shlex.split("/opt/etcd/etcdctl cluster-health")
        end = time.time() + 5
        while time.time() < end:
            if subprocess.call(clusterHealthCommand, stdout=subprocess.PIPE, stderr=subprocess.PIPE) == 0:
                break
            time.sleep(0.1)
        else:
            finalizeBackend()
            raise RuntimeError("Could not set up etcd process")

        infoProperties = {
            "client.host": "localhost",
            "client.port": 2379,
        }
        info = BackendInfo("c4.backends.etcdBackend.EtcdBackend", properties=infoProperties)
        testBackendImplementation = EtcdBackend(info)

    elif request.param == SharedSqliteDBBackend:

        newpath = tempfile.mkdtemp(dir="/dev/shm")
        infoProperties = {
            "path.database": newpath,
            "path.backup": newpath
        }
        info = BackendInfo("c4.backends.sharedSQLite.SharedSqliteDBBackend", properties=infoProperties)
        testBackendImplementation = SharedSqliteDBBackend(info)

        def finalizeBackend():
            # remove data directory
            shutil.rmtree(newpath)

    # set backend
    testBackend = Backend(implementation=testBackendImplementation)

    request.addfinalizer(finalizeBackend)
    return testBackend

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
def system(request, temporaryBackend, cleandir, temporaryIPCPath):
    """
    Set up a basic system configuration
    """
    configuration = temporaryBackend.configuration
    platform = PlatformInfo("im-devops", "c4.system.platforms.devops.IMDevOps")
    configuration.addPlatform(platform)

    node1 = NodeInfo("rack1-master1", "tcp://127.0.0.1:5000", role=Roles.ACTIVE)
    node1.addDevice(DeviceInfo("info", "c4.devices.cluster.info.Info"))
    node1.addDevice(DeviceInfo("cpu", "c4.devices.cpu.Cpu"))
    node1.addDevice(DeviceInfo("unknown", "c4.devices.Unknown"))
    node1.addDevice(DeviceInfo("disk", "c4.devices.disk.Disk"))
    node1.addDevice(DeviceInfo("memory", "c4.devices.mem.Memory"))

    configuration.addNode(node1)
    # TODO: this should automatically set role of the node to active
    configuration.addAlias("system-manager", "rack1-master1")

    node2 = NodeInfo("rack1-master2", "tcp://127.0.0.1:6000", role=Roles.PASSIVE)
    node2.addDevice(DeviceInfo("cpu", "c4.devices.cpu.Cpu"))
    node2.addDevice(DeviceInfo("memory", "c4.devices.mem.Memory"))
    configuration.addNode(node2)
    node3 = NodeInfo("rack1-master3", "tcp://127.0.0.1:7000")
    node3.addDevice(DeviceInfo("cpu", "c4.devices.cpu.Cpu"))
    node3.addDevice(DeviceInfo("memory", "c4.devices.mem.Memory"))
    configuration.addNode(node3)
    log.debug(configuration.toInfo().toJSON(pretty=True))

    systemSetup = {}
    node1ClusterInfo = temporaryBackend.ClusterInfo("rack1-master1", "tcp://127.0.0.1:5000", "tcp://127.0.0.1:5000", Roles.ACTIVE, States.DEPLOYED)
    systemSetup["rack1-master1"] = SystemManager(node1ClusterInfo)
    node2ClusterInfo = temporaryBackend.ClusterInfo("rack1-master2", "tcp://127.0.0.1:6000", "tcp://127.0.0.1:5000", Roles.PASSIVE, States.DEPLOYED)
    systemSetup["rack1-master2"] = SystemManager(node2ClusterInfo)
    node3ClusterInfo = temporaryBackend.ClusterInfo("rack1-master3", "tcp://127.0.0.1:7000", "tcp://127.0.0.1:5000", Roles.THIN, States.DEPLOYED)
    systemSetup["rack1-master3"] = SystemManager(node3ClusterInfo)

    def systemTeardown():
        log.debug("clean up")

        systemManagerNode = temporaryBackend.configuration.getSystemManagerNodeName()
        activeSystemManager = systemSetup.pop(systemManagerNode)

        for node, systemManager in systemSetup.items():
            log.debug("stopping %s", node)
            systemManager.stop()

        log.debug("stopping active system manager %s", systemManagerNode)
        activeSystemManager.stop()

    request.addfinalizer(systemTeardown)

    return systemSetup

@pytest.fixture(scope="function")
def temporaryBackend(request):
    """
    Set backend to something temporary for testing
    """
    # save old backend
    try:
        oldBackend = Backend()
    except ValueError:
        newpath = tempfile.mkdtemp(dir="/dev/shm")
        log.info("setting default temp backend to use '%s' as part of testing", newpath)
        infoProperties = {
            "path.database": newpath,
            "path.backup": newpath
        }
        info = BackendInfo("c4.backends.sharedSQLite.SharedSqliteDBBackend", properties=infoProperties)
        oldBackend = SharedSqliteDBBackend(info)

    newpath = tempfile.mkdtemp(dir="/dev/shm")
#     newpath = tempfile.mkdtemp(dir="/tmp")
    infoProperties = {
        "path.database": newpath,
        "path.backup": newpath
    }
    info = BackendInfo("c4.backends.sharedSQLite.SharedSqliteDBBackend", properties=infoProperties)
    testBackendImplementation = SharedSqliteDBBackend(info)

    # change backend
    newBackend = Backend(implementation=testBackendImplementation)

    def removeTemporaryDirectory():
        # change backend back
        Backend(implementation=oldBackend)
        shutil.rmtree(newpath)
    request.addfinalizer(removeTemporaryDirectory)
    return newBackend

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
