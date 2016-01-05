import logging
import multiprocessing
import time

from c4.messaging import Router, RouterClient
from c4.system.deviceManager import DeviceManager, DeviceManagerImplementation, DeviceManagerStatus
from c4.system.messages import Status


log = logging.getLogger(__name__)

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

def test_deviceManager(temporaryIPCPath):

    router = Router("test")  # @UnusedVariable

    deviceManager = DeviceManager("test", "testDM", SampleDeviceManager)
    deviceManager.start()

    time.sleep(0.1)

    client = RouterClient("test/testDM")
    client.forwardMessage(Status("test/testDM"))
    client.forwardMessage(Status("test/testDM"))
    client.forwardMessage(Status("test/testDM"))

    time.sleep(0.1)

    deviceManager.stop(wait=True)

    assert deviceManager.implementation.counter.value == 3
