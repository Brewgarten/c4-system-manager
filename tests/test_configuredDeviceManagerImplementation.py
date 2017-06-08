import logging
import mock
import pytest

from c4.system.deviceManager import ConfiguredDeviceManagerImplementation
from c4.messaging import Router, RouterClient
from c4.system.configuration import States, Roles, DeviceManagerConfiguration
from c4.system.deviceManager import DeviceManager, ConfiguredDeviceManagerStatus
from c4.system.messages import (LocalStartDeviceManager, LocalStopDeviceManager,
                                Status)


log = logging.getLogger(__name__)

pytestmark = pytest.mark.usefixtures("temporaryIPCPath")

@pytest.fixture
def systemManagerClusterInfo(temporaryBackend):
    return temporaryBackend.ClusterInfo("test", "ipc://test.ipc", "ipc://test.ipc", Roles.ACTIVE, States.RUNNING)

# regex-based configuration
@pytest.fixture
def myServiceConfigProperties_regex():
    dmConfig = DeviceManagerConfiguration("/usr/bin/myService start", "/usr/bin/myService status", "/usr/bin/myService stop", ".* running")
    return {"configuration": dmConfig}

# return code-based configuration
@pytest.fixture
def myServiceConfigProperties_rc():
    dmConfig = DeviceManagerConfiguration("/usr/bin/myService start", "/usr/bin/myService status", "/usr/bin/myService stop", rc=0)
    return {"configuration": dmConfig}

class TestconfiguredDeviceManagerImplementation(object):

    def test_getOperations(self):

        operations = ConfiguredDeviceManagerImplementation.getOperations()
        assert {"start", "stop"} == set(operations.keys())

    @pytest.mark.parametrize("status_result", [("myService is running", "", 0), ("myService is stopped", "", 3)])
    def test_status_regex(self, status_result, systemManagerClusterInfo, myServiceConfigProperties_regex):
        stdout, stderr, rc = status_result # @UnusedVariable
        router = Router("test")  # @UnusedVariable
        with mock.patch("c4.system.deviceManager.run", return_value=status_result):
            deviceManager = DeviceManager(systemManagerClusterInfo, "myservice", ConfiguredDeviceManagerImplementation, myServiceConfigProperties_regex)
            assert deviceManager.start()

            client = RouterClient("test/myservice")
            startResponse = client.sendRequest(LocalStartDeviceManager("test", "test/myservice"))

            status = client.sendRequest(Status("test/myservice"))
            stopResponse = client.sendRequest(LocalStopDeviceManager("test", "test/myservice"))

            assert deviceManager.stop()
            assert startResponse["state"] == States.RUNNING
            assert status.state == States.RUNNING
            if "running" in stdout:
                assert status.status == ConfiguredDeviceManagerStatus.OK
            else:
                assert status.status == ConfiguredDeviceManagerStatus.FAILED
            assert stopResponse["state"] == States.REGISTERED

    @pytest.mark.parametrize("status_result", [("myService is running", "", 0), ("myService is stopped", "", 3)])
    def test_status_rc(self, status_result, systemManagerClusterInfo, myServiceConfigProperties_rc):
        stdout, stderr, rc = status_result # @UnusedVariable
        router = Router("test")  # @UnusedVariable
        with mock.patch("c4.system.deviceManager.run", return_value=status_result):
            deviceManager = DeviceManager(systemManagerClusterInfo, "myservice", ConfiguredDeviceManagerImplementation, myServiceConfigProperties_rc)
            assert deviceManager.start()

            client = RouterClient("test/myservice")
            startResponse = client.sendRequest(LocalStartDeviceManager("test", "test/myservice"))

            status = client.sendRequest(Status("test/myservice"))
            stopResponse = client.sendRequest(LocalStopDeviceManager("test", "test/myservice"))

            assert deviceManager.stop()
            assert startResponse["state"] == States.RUNNING
            assert status.state == States.RUNNING
            if rc == 0:
                assert status.status == ConfiguredDeviceManagerStatus.OK
            else:
                assert status.status == ConfiguredDeviceManagerStatus.FAILED
            assert stopResponse["state"] == States.REGISTERED
