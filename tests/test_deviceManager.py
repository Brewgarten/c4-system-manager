import logging
import multiprocessing

import pytest

from c4.messaging import Router, RouterClient
from c4.system.configuration import States
from c4.system.deviceManager import (DeviceManager, DeviceManagerImplementation, DeviceManagerStatus)
from c4.system.messages import (LocalStartDeviceManager, LocalStopDeviceManager,
                                Status)


log = logging.getLogger(__name__)

pytestmark = pytest.mark.usefixtures("temporaryIPCPath")

class SampleDeviceManager(DeviceManagerImplementation):

    def __init__(self, node, name, properties=None):
        super(SampleDeviceManager, self).__init__(node, name, properties=None)
        self.counter = multiprocessing.Value("i", 0)

    def handleStatus(self, message):
        """
        The handler for an incoming Status message.
        """
        log.debug("Received status request: %s", message)
        with self.counter.get_lock():
            self.counter.value += 1
        status = DeviceManagerStatus()
        status.Message = self.properties
        return status

class TestDeviceManager(object):

    def test_status(self):

        router = Router("test")  # @UnusedVariable

        deviceManager = DeviceManager("test", "testDM", SampleDeviceManager)
        assert deviceManager.start()

        client = RouterClient("test/testDM")
        startResponse = client.sendRequest(LocalStartDeviceManager("test", "test/testDM"))

        statuses = []
        for _ in range(3):
            statuses.append(client.sendRequest(Status("test/testDM")))

        stopResponse = client.sendRequest(LocalStopDeviceManager("test", "test/testDM"))

        assert deviceManager.stop()

        assert startResponse["state"] == States.RUNNING
        assert stopResponse["state"] == States.REGISTERED
        assert deviceManager.implementation.counter.value == 3
        assert deviceManager.implementation.state == States.REGISTERED

    def test_stop(self):

        router = Router("test")  # @UnusedVariable

        deviceManager = DeviceManager("test", "testDM", SampleDeviceManager)
        assert deviceManager.start()

        client = RouterClient("test/testDM")
        startResponse = client.sendRequest(LocalStartDeviceManager("test", "test/testDM"))

        assert deviceManager.stop()

        assert startResponse["state"] == States.RUNNING
        assert deviceManager.implementation.state == States.REGISTERED

    def test_stopWithoutStartMessage(self):

        router = Router("test")  # @UnusedVariable

        deviceManager = DeviceManager("test", "testDM", SampleDeviceManager)
        assert deviceManager.start()

        assert deviceManager.stop()

        assert deviceManager.implementation.state == States.REGISTERED
