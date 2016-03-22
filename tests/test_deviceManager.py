import logging
import multiprocessing

import pytest

from c4.messaging import Router, RouterClient
from c4.system.configuration import States, DBClusterInfo, Roles
from c4.system.deviceManager import (DeviceManager, DeviceManagerImplementation, DeviceManagerStatus,
                                     operation)
from c4.system.messages import (LocalStartDeviceManager, LocalStopDeviceManager,
                                Operation,
                                Status)


log = logging.getLogger(__name__)

pytestmark = pytest.mark.usefixtures("temporaryIPCPath")

@pytest.fixture
def systemManagerClusterInfo():
    return DBClusterInfo("test", "ipc://test.ipc", "ipc://test.ipc", role=Roles.ACTIVE, state=States.RUNNING)

class SampleDeviceManager(DeviceManagerImplementation):

    def __init__(self, clusterInfo, name, properties=None):
        super(SampleDeviceManager, self).__init__(clusterInfo, name, properties=None)
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

    @operation
    def fireAndForget(self):
        """
        Fire and forget test operation
        """
        with self.counter.get_lock():
            self.counter.value += 1

    @operation
    def request(self, a, b, c="c"):
        """
        Request test operation

        :param a: a
        :param b: b
        :param c: c
        :returns: argument dictionary
        """
        return {
            "a": a,
            "b": b,
            "c": c
        }

class TestDeviceManager(object):

    def test_getOperations(self):

        operations = SampleDeviceManager.getOperations()
        assert "fireAndForget" in operations
        assert operations["fireAndForget"]["name"] == "fireAndForget"
        assert operations["fireAndForget"]["description"] == "Fire and forget test operation"
        assert "request" in operations
        assert operations["request"]["name"] == "request"
        assert operations["request"]["required"] == ["a", "b"]
        assert operations["request"]["optional"] == ["c"]
        assert operations["request"]["description"] == "Request test operation\n:param a: a\n:param b: b\n:param c: c\n:returns: argument dictionary"

    def test_handleOperation(self, systemManagerClusterInfo):

        router = Router("test")  # @UnusedVariable

        deviceManager = DeviceManager(systemManagerClusterInfo, "testDM", SampleDeviceManager)
        assert deviceManager.start()

        client = RouterClient("test/testDM")
        client.forwardMessage(Operation("test/testDM", "fireAndForget"))

        # check missing operation
        missingOperationResponse = client.sendRequest(Operation("test/testDM", "missingOperation"))

        # check missing required arguments
        missingArgumentsResponse = client.sendRequest(Operation("test/testDM", "request"))

        response = client.sendRequest(Operation("test/testDM", "request", 1, 2, 3, 4, d=5))

        assert deviceManager.stop()

        assert deviceManager.implementation.counter.value == 1
        assert missingOperationResponse == {"error": "unsupported operation 'missingOperation'"}
        assert missingArgumentsResponse == {"error": "'request' is missing required arguments 'a,b'"}
        assert response == {"a": 1, "b": 2, "c": 3, "warning": "'request' has left over arguments '4'\n'request' has left over keyword arguments 'd'"}

    def test_status(self, systemManagerClusterInfo):

        router = Router("test")  # @UnusedVariable

        deviceManager = DeviceManager(systemManagerClusterInfo, "testDM", SampleDeviceManager)
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

    def test_stop(self, systemManagerClusterInfo):

        router = Router("test")  # @UnusedVariable

        deviceManager = DeviceManager(systemManagerClusterInfo, "testDM", SampleDeviceManager)
        assert deviceManager.start()

        client = RouterClient("test/testDM")
        startResponse = client.sendRequest(LocalStartDeviceManager("test", "test/testDM"))

        assert deviceManager.stop()

        assert startResponse["state"] == States.RUNNING
        assert deviceManager.implementation.state == States.REGISTERED

    def test_stopWithoutStartMessage(self, systemManagerClusterInfo):

        router = Router("test")  # @UnusedVariable

        deviceManager = DeviceManager(systemManagerClusterInfo, "testDM", SampleDeviceManager)
        assert deviceManager.start()

        assert deviceManager.stop()

        assert deviceManager.implementation.state == States.REGISTERED
