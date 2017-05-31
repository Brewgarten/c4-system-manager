import pytest

from c4.system.backend import Backend
from c4.system.deviceManager import DeviceManagerStatus
from c4.system.manager import SystemManagerStatus
from c4.utils.jsonutil import Datetime


class MockDeviceManagerStatus(DeviceManagerStatus):
    def __init__(self, value, timestampString):
        super(MockDeviceManagerStatus, self).__init__()
        self.test = value
        self.timestamp = timestampString

class MockSystemManagerStatus(SystemManagerStatus):
    def __init__(self, value, timestampString):
        super(MockSystemManagerStatus, self).__init__()
        self.test = value
        self.timestamp = timestampString

@pytest.fixture
def deviceHistory(backend):
    history = backend.deviceHistory
    history.add("node1", "device1", MockDeviceManagerStatus("one", Datetime(2000, 1, 1)))
    history.add("node1", "device1", MockDeviceManagerStatus("two", Datetime(2000, 1, 2)))
    history.add("node1", "device1", MockDeviceManagerStatus("three", Datetime(2000, 1, 3)))
    history.add("node1", "device2", MockDeviceManagerStatus("four", Datetime(2000, 1, 4)))
    history.add("node1", "device3", MockDeviceManagerStatus("five", Datetime(2000, 1, 5)))
    history.add("node2", "device1", MockDeviceManagerStatus("six", Datetime(2000, 1, 6)))
    history.add("node3", "device1", MockDeviceManagerStatus("seven", Datetime(2000, 1, 7)))
    return history

@pytest.fixture
def nodeHistory(backend):
    history = backend.nodeHistory
    history.add("node1", MockSystemManagerStatus("one", Datetime(2000, 1, 1)))
    history.add("node1", MockSystemManagerStatus("two", Datetime(2000, 1, 2)))
    history.add("node1", MockSystemManagerStatus("three", Datetime(2000, 1, 3)))
    history.add("node2", MockSystemManagerStatus("four", Datetime(2000, 1, 4)))
    history.add("node3", MockSystemManagerStatus("five", Datetime(2000, 1, 5)))
    return history

class TestDeviceHistory():

    def test_get(self, deviceHistory):

        history = Backend().deviceHistory

        # make sure all device entries are there and in order of recency
        entries = history.get("node1", "device1")
        assert [entry.status.test for entry in entries] == ["three", "two", "one"]

        # check that limiting works
        entries = history.get("node1", "device1", limit=1)
        assert len(entries) == 1
        entries = history.get("node1", "device1", limit=2)
        assert len(entries) == 2

        # make sure all entries are there and in order of recency
        entries = history.getAll()
        assert [entry.status.test for entry in entries] == ["seven", "six", "five", "four", "three", "two", "one"]

    def test_getLatest(self, deviceHistory):

        history = Backend().deviceHistory

        # make sure all entries are there
        assert history.getLatest("node1", "device1")
        assert history.getLatest("node2", "device1")
        assert history.getLatest("node3", "device1")

        # make sure the entry is the latest
        assert history.getLatest("node1", "device1").status.test == "three"

    def test_removeAllDevicesOnAllNodes(self, deviceHistory):

        history = Backend().deviceHistory
        # check removing all history
        history.remove()
        assert not history.getAll()

    def test_removeAllDevicesOnNode(self, deviceHistory):

        history = Backend().deviceHistory

        # check removing only history for node1
        history.remove(node="node1")
        assert not history.get("node1", "device1")
        assert not history.get("node1", "device2")
        assert not history.get("node1", "device3")
        assert history.get("node2", "device1")
        assert history.get("node3", "device1")

    def test_removeDeviceOnAllNodes(self, deviceHistory):

        history = Backend().deviceHistory

        # check removing only history for device1 on all nodes
        history.remove(name="device1")
        assert not history.get("node1", "device1")
        assert history.get("node1", "device2")
        assert history.get("node1", "device3")
        assert not history.get("node2", "device1")
        assert not history.get("node3", "device1")

    def test_removeDeviceOnNode(self, deviceHistory):

        history = Backend().deviceHistory

        # check removing only history for device1 on node1
        history.remove(node="node1", name="device1")
        assert not history.get("node1", "device1")
        assert history.get("node1", "device2")
        assert history.get("node1", "device3")
        assert history.get("node2", "device1")
        assert history.get("node3", "device1")

class TestNodeHistory():

    def test_get(self, nodeHistory):

        history = Backend().nodeHistory

        # make sure all node entries are there and in order of recency
        entries = history.get("node1")
        assert [entry.status.test for entry in entries] == ["three", "two", "one"]

        # check that limiting works
        entries = history.get("node1", limit=1)
        assert len(entries) == 1
        entries = history.get("node1", limit=2)
        assert len(entries) == 2

        # make sure all entries are there and in order of recency
        entries = history.getAll()
        assert [entry.status.test for entry in entries] == ["five", "four", "three", "two", "one"]

    def test_getLatest(self, nodeHistory):

        history = Backend().nodeHistory

        # make sure all entries are there
        assert history.getLatest("node1")
        assert history.getLatest("node2")
        assert history.getLatest("node3")

        # make sure the entry is the latest
        assert history.getLatest("node1").status.test == "three"

    def test_removeAllNodes(self, nodeHistory):

        history = Backend().nodeHistory

        # check removing all history
        history.remove()
        assert not history.getAll()

    def test_removeNode(self, nodeHistory):

        history = Backend().nodeHistory

        # check removing only history for node1
        history.remove(node="node1")
        assert not history.get("node1")
        assert history.get("node2")
        assert history.get("node3")
