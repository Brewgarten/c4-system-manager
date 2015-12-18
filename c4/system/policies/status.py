"""
This library contains status related policy functionality

Examples
--------

.. code-block:: python

    device.status.ageInSeconds('rack1-master1', 'cpu') >= 10 -> system.requestStatus('rack1-master1', 'cpu')

Functionality
-------------
"""
import logging

from datetime import datetime

from c4.messaging import sendMessage
from c4.system.configuration import Configuration, Roles, States
from c4.system.db import DBManager
from c4.system.messages import Status
from c4.system.policyEngine import Action, Event, Policy
from c4.utils.logutil import ClassLogger

log = logging.getLogger(__name__)

@ClassLogger
class StatusAgeInSeconds(Event):
    """
    Status age of a device or system manager in seconds
    """
    id = "device.status.ageInSeconds"

    def evaluate(self, node, fullDeviceName=None):
        """
        Retrieve the age of the specified device or system manager in seconds

        :param node: node name
        :type node: str
        :param fullDeviceName: fully qualified device manager name
        :type fullDeviceName: device
        """
        # TODO: this kind of query should be in a separate History class
        database = DBManager()
        rows = database.query("""
            select history_date
            from t_sm_history
            where node = ? and name is ?
            order by history_date desc
            limit 1""",
            (node, fullDeviceName,))
        database.close()
        if rows:
            history_date = rows[0][0]
            timestamp = datetime.strptime(history_date, "%Y-%m-%d %H:%M:%S.%f")
            now = datetime.utcnow()
            timeDifference = now - timestamp
            return (timeDifference.days * 3600 * 24) + timeDifference.seconds
        return -1

@ClassLogger
class RequestStatus(Action):
    """
    Send status message to a device or system manager
    """
    id = "system.requestStatus"

    def perform(self, node, fullDeviceName=None):
        """
        Send status message to the specified device or system manager

        :param node: node name
        :type node: str
        :param fullDeviceName: fully qualified device manager name
        :type fullDeviceName: device
        """
        configuration = Configuration()
        systemManagerNode = configuration.getSystemManagerNodeName()
        systemManagerAddress = configuration.getAddress(systemManagerNode)

        # FIXME: need to convert fullDeviceName . to / for routing
        if fullDeviceName:
            self.log.debug("sending status request to %s/%s", node, fullDeviceName)
            sendMessage(systemManagerAddress, Status("{0}/{1}".format(node, fullDeviceName)))
        else:
            self.log.debug("sending status request to %s", node)
            sendMessage(systemManagerAddress, Status(node))

        return True

@ClassLogger
class DeviceManagerStatusRefreshPolicy(Policy):
    """
    Policy that sends out status request messages to device managers
    according to their specified status interval

    :param cache: cache
    :type cache: :class:`~c4.system.policyEngine.Cache`
    """
    id = "device.status.refresh"

    def __init__(self, cache):
        super(DeviceManagerStatusRefreshPolicy, self).__init__(cache)
        self.requestDeviceMap = {}
        self.statusInterval = {
            "c4.system.devices.cpu.Cpu": 10000,
            "c4.system.devices.disk.Disk": 5000,
        }

    def evaluateEvent(self):
        """
        Check status age for all running device managers
        """
        configuration = Configuration()

        statusAgeInSeconds = StatusAgeInSeconds()

        self.requestDeviceMap.clear()
        statusRequestNeeded = False
        for node in configuration.getNodeNames():

            nodeInfo = configuration.getNode(node, includeDevices=True, flatDeviceHierarchy=True)

            if nodeInfo.state == States.RUNNING:

                self.requestDeviceMap[node] = set()

                for fullDeviceName, deviceInfo in nodeInfo.devices.items():

                    age = statusAgeInSeconds.evaluate(node, fullDeviceName)
                    statusInterval = self.statusInterval.get(deviceInfo.type, 5000) / 1000

                    if age < 0 or age >= statusInterval:
                        self.log.debug("%s/%s: age %ss >= %ss", node, fullDeviceName, age, statusInterval)
                        self.requestDeviceMap[node].add(fullDeviceName)
                        statusRequestNeeded = True
                    else:
                        self.log.debug("%s/%s: age %ss < %ss", node, fullDeviceName, age, statusInterval)

        return statusRequestNeeded

    def performActions(self):
        """
        Send out status request messages to device managers
        that have been identified as having an old status
        """
        requestStatus = RequestStatus()
        for node, devices in self.requestDeviceMap.items():
            for fullDeviceName in devices:
                requestStatus.perform(node, fullDeviceName)

@ClassLogger
class SystemManagerStatusRefreshPolicy(Policy):
    """
    Policy that sends out status request messages to system managers
    according to their specified status interval

    :param cache: cache
    :type cache: :class:`~c4.system.policyEngine.Cache`
    """
    id = "node.status.refresh"

    def __init__(self, cache):
        super(SystemManagerStatusRefreshPolicy, self).__init__(cache)
        self.requestNodes = set()
        self.statusInterval = {
            Roles.ACTIVE : 5000,
            Roles.PASSIVE : 5000,
            Roles.THIN: 10000,
        }

    def evaluateEvent(self):
        """
        Check status age for all running system managers
        """
        configuration = Configuration()

        statusAgeInSeconds = StatusAgeInSeconds()

        self.requestNodes.clear()
        statusRequestNeeded = False
        for node in configuration.getNodeNames():

            nodeInfo = configuration.getNode(node, includeDevices=False)

            if nodeInfo.state == States.RUNNING:

                age = statusAgeInSeconds.evaluate(node)
                statusInterval = self.statusInterval.get(nodeInfo.role, 5000) / 1000

                if age < 0 or age >= statusInterval:
                    self.log.debug("%s: age %ss >= %ss", node, age, statusInterval)
                    self.requestNodes.add(node)
                    statusRequestNeeded = True
                else:
                    self.log.debug("%s: age %ss < %ss", node, age, statusInterval)

        return statusRequestNeeded

    def performActions(self):
        """
        Send out status request messages to system managers
        that have been identified as having an old status
        """
        requestStatus = RequestStatus()
        for node in self.requestNodes:
            requestStatus.perform(node)
