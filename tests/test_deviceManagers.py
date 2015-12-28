#!/usr/bin/env python

import logging

from _abcoll import Iterable

from c4.system.devices.cpu import Cpu
from c4.system.devices.disk import Disk
from c4.system.devices.loadavg import LoadAvg
from c4.system.devices.mem import Memory
from c4.system.devices.swap import Swap

from c4.system.deviceManager import DeviceManagerStatus

log = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s [%(levelname)s] <%(processName)s> [%(name)s(%(filename)s:%(lineno)d)] - %(message)s', level=logging.INFO)

def pytest_generate_tests(metafunc):
    if "implementation" in metafunc.fixturenames and "statusMethod" in metafunc.fixturenames:
        metafunc.parametrize(["implementation", "statusMethod"], [
                                   (Cpu, Cpu.calculateCPUUsage),
                                   (LoadAvg, LoadAvg.calculateLoadAvg),
                                   (Memory, Memory.calculateMemoryInfo),
                                   (Swap, Swap.calculateSwapUsage),
                              ])

def test_status(implementation, statusMethod):

    info = statusMethod(implementation("localhost", "testDeviceManager"))
    assert info is not None
    if isinstance(info, Iterable):
        for i in info:
            assert i is not None
    elif isinstance(info, DeviceManagerStatus):
        for i, value in vars(info).items():
            assert value is not None

def test_disk():

    deviceManager = Disk("localhost", "disk")
    info = deviceManager.calculateDiskUsage("/")
    assert info is not None
    for i in info:
        assert i is not None
