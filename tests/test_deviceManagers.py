import logging

import pytest

from _abcoll import Iterable

from c4.devices.cpu import Cpu
from c4.devices.disk import Disk
from c4.devices.loadavg import LoadAvg
from c4.devices.mem import Memory
from c4.devices.swap import Swap

from c4.system.deviceManager import DeviceManagerStatus
from c4.system.configuration import DBClusterInfo, Roles, States

log = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s [%(levelname)s] <%(processName)s> [%(name)s(%(filename)s:%(lineno)d)] - %(message)s', level=logging.INFO)

@pytest.fixture
def systemManagerClusterInfo():
    return DBClusterInfo("localhost", "ipc://localhost.ipc", "ipc://localhost.ipc", role=Roles.ACTIVE, state=States.RUNNING)

def pytest_generate_tests(metafunc):
    if "implementation" in metafunc.fixturenames and "statusMethod" in metafunc.fixturenames:
        metafunc.parametrize(["implementation", "statusMethod"], [
                                   (Cpu, Cpu.calculateCPUUsage),
                                   (LoadAvg, LoadAvg.calculateLoadAvg),
                                   (Memory, Memory.calculateMemoryInfo),
                                   (Swap, Swap.calculateSwapUsage),
                              ])

def test_status(implementation, statusMethod, systemManagerClusterInfo):

    info = statusMethod(implementation(systemManagerClusterInfo, "testDeviceManager"))
    assert info is not None
    if isinstance(info, Iterable):
        for i in info:
            assert i is not None
    elif isinstance(info, DeviceManagerStatus):
        for i, value in vars(info).items():
            assert value is not None

def test_disk(systemManagerClusterInfo):

    deviceManager = Disk(systemManagerClusterInfo, "disk")
    info = deviceManager.calculateDiskUsage("/")
    assert info is not None
    for i in info:
        assert i is not None
