import logging

from c4.system.configuration import Configuration, States

log = logging.getLogger(__name__)

def test_system(system):

    logging.root.setLevel(logging.INFO)
    logging.getLogger("c4.messaging").setLevel(logging.INFO)
    logging.getLogger("c4.system.db").setLevel(logging.INFO)
    logging.getLogger("c4.system.deviceManager").setLevel(logging.INFO)
    logging.getLogger("c4.system.manager.SystemManagerImplementation").setLevel(logging.DEBUG)

    configuration = Configuration()

    for node in system.keys():
        nodeInfo = configuration.getNode(node, flatDeviceHierarchy=True)
        assert nodeInfo.state == States.DEPLOYED
        for deviceInfo in nodeInfo.devices.values():
            assert deviceInfo.state == States.REGISTERED

    assert system["rack1-master1"].start()

    nodeInfo = configuration.getNode("rack1-master1", flatDeviceHierarchy=True)
    assert nodeInfo.state == States.RUNNING
    for deviceInfo in nodeInfo.devices.values():
        if deviceInfo.type ==  "c4.devices.Unknown":
            assert deviceInfo.state == States.REGISTERED
        else:
            assert deviceInfo.state == States.RUNNING
    for node in ("rack1-master2", "rack1-master3"):
        nodeInfo = configuration.getNode(node, flatDeviceHierarchy=True)
        assert nodeInfo.state == States.DEPLOYED
        for deviceInfo in nodeInfo.devices.values():
            assert deviceInfo.state == States.REGISTERED

    assert system["rack1-master2"].start()
    assert system["rack1-master3"].start()

    for node in system.keys():
        nodeInfo = configuration.getNode(node, flatDeviceHierarchy=True)
        assert nodeInfo.state == States.RUNNING
        for deviceInfo in nodeInfo.devices.values():
            if deviceInfo.type ==  "c4.devices.Unknown":
                assert deviceInfo.state == States.REGISTERED
            else:
                assert deviceInfo.state == States.RUNNING

    assert system["rack1-master2"].stop()
    assert system["rack1-master3"].stop()

    nodeInfo = configuration.getNode("rack1-master1", flatDeviceHierarchy=True)
    assert nodeInfo.state == States.RUNNING
    for deviceInfo in nodeInfo.devices.values():
        if deviceInfo.type ==  "c4.devices.Unknown":
            assert deviceInfo.state == States.REGISTERED
        else:
            assert deviceInfo.state == States.RUNNING
    for node in ("rack1-master2", "rack1-master3"):
        nodeInfo = configuration.getNode(node, flatDeviceHierarchy=True)
        assert nodeInfo.state == States.REGISTERED
        for deviceInfo in nodeInfo.devices.values():
            assert deviceInfo.state == States.REGISTERED

    assert system["rack1-master1"].stop()

    for node in system.keys():
        nodeInfo = configuration.getNode(node, flatDeviceHierarchy=True)
        assert nodeInfo.state == States.REGISTERED
        for deviceInfo in nodeInfo.devices.values():
            assert deviceInfo.state == States.REGISTERED
