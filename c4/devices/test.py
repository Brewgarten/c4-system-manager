import logging
import random
import time

from c4.system.deviceManager import DeviceManagerImplementation, DeviceManagerStatus

log = logging.getLogger(__name__)

__version__ = "0.1.0.0"

class Test(DeviceManagerImplementation):
    """
    Test device manager
    """
    def __init__(self, clusterInfo, name, properties=None):
        super(Test, self).__init__(clusterInfo, name, properties=properties)

    def handleStatus(self, message):
        """
        The handler for an incoming Status message.
        """
        logging.debug("Received status request: %s" % message)
        time.sleep(self.properties.get("status_time", 0) / 1000)
        return TestStatus(self.properties .get("value", random.random()))

    def start(self):
        time.sleep(self.properties.get("start_time", 0) / 1000)

    def stop(self):
        time.sleep(self.properties.get("stop_time", 0) / 1000)

class TestStatus(DeviceManagerStatus):
    def __init__(self, value):
        super(TestStatus, self).__init__()
        self.value = value
