import logging
import time

from c4.system.configuration import Configuration, States

log = logging.getLogger(__name__)

def test_system(system):

    logging.root.setLevel(logging.INFO)
    logging.getLogger("c4.messaging").setLevel(logging.INFO)
    logging.getLogger("c4.system.db").setLevel(logging.INFO)

    configuration = Configuration()

    for node in system.keys():
        nodeInfo = configuration.getNode(node, flatDeviceHierarchy=True)
        assert nodeInfo.state == States.DEPLOYED
        for deviceInfo in nodeInfo.devices.values():
            assert deviceInfo.state == States.REGISTERED

    system["rack1-master1"].start()
    waitForSystemManagerState("rack1-master1", States.RUNNING)

    nodeInfo = configuration.getNode("rack1-master1", flatDeviceHierarchy=True)
    assert nodeInfo.state == States.RUNNING
    for deviceInfo in nodeInfo.devices.values():
        if deviceInfo.type ==  "c4.system.devices.Unknown":
            assert deviceInfo.state == States.REGISTERED
        else:
            assert deviceInfo.state == States.RUNNING
    for node in ("rack1-master2", "rack1-master3"):
        nodeInfo = configuration.getNode(node, flatDeviceHierarchy=True)
        assert nodeInfo.state == States.DEPLOYED
        for deviceInfo in nodeInfo.devices.values():
            assert deviceInfo.state == States.REGISTERED


    system["rack1-master2"].start()
    system["rack1-master3"].start()
    waitForSystemManagerState("rack1-master2", States.RUNNING)
    waitForSystemManagerState("rack1-master3", States.RUNNING)

    for node in system.keys():
        nodeInfo = configuration.getNode(node, flatDeviceHierarchy=True)
        assert nodeInfo.state == States.RUNNING
        for deviceInfo in nodeInfo.devices.values():
            if deviceInfo.type ==  "c4.system.devices.Unknown":
                assert deviceInfo.state == States.REGISTERED
            else:
                assert deviceInfo.state == States.RUNNING

    system["rack1-master2"].stop(wait=True)
    system["rack1-master3"].stop(wait=True)

    nodeInfo = configuration.getNode("rack1-master1", flatDeviceHierarchy=True)
    assert nodeInfo.state == States.RUNNING
    for deviceInfo in nodeInfo.devices.values():
        if deviceInfo.type ==  "c4.system.devices.Unknown":
            assert deviceInfo.state == States.REGISTERED
        else:
            assert deviceInfo.state == States.RUNNING
    for node in ("rack1-master2", "rack1-master3"):
        nodeInfo = configuration.getNode(node, flatDeviceHierarchy=True)
        # TODO: should the node state be deployed?
        assert nodeInfo.state == States.REGISTERED
        for deviceInfo in nodeInfo.devices.values():
            assert deviceInfo.state == States.REGISTERED

    system["rack1-master1"].stop(wait=True)

    for node in system.keys():
        nodeInfo = configuration.getNode(node, flatDeviceHierarchy=True)
        # TODO: should the node state be deployed?
        assert nodeInfo.state == States.REGISTERED
        for deviceInfo in nodeInfo.devices.values():
            assert deviceInfo.state == States.REGISTERED

def waitForSystemManagerState(name, state, timeout=10):

    configuration = Configuration()
    end = time.time() + timeout
    while time.time() < end:
        if configuration.getNode(name, includeDevices=False).state == state:
            break
        else:
            time.sleep(1)
    else:
        raise RuntimeError("Waiting for '{name}' to reach '{state}' timed out (took more than '{timeout}' seconds".format(
                name=name,
                state=state,
                timeout=timeout)
            )

