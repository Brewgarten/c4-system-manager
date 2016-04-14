import logging

from c4.system.backend import Backend
from c4.system.client import Client


log = logging.getLogger(__name__)

class TestClient(object):

    def test_infoDeviceManager(self, system):

        assert system["rack1-master1"].start()

        client = Client("rack1-master1")

        # use configuration for verification
        configuration = Backend().configuration

        node1 = client.getNode("rack1-master1")
        assert node1
        assert node1.toJSON(pretty=True) == configuration.getNode("rack1-master1").toJSON(pretty=True)

        assert client.getNodeNames() == ["rack1-master1", "rack1-master2", "rack1-master3"]

        assert client.getSystemManagerNodeName() == "rack1-master1"

        assert system["rack1-master1"].stop()
