"""
This module provides a device manager interface to implement device managers.

In particular one should extend :py:class:`~dynamite.system.deviceManager.DeviceManagerImplementation`
to implement various message handlers. Note that by default the handlers follow this pattern:

    ``def handle<MessageType>(self, message)``
    or
    ``def handle<MessageType>(self, message, envelope)``

and return a ``dict`` which becomes message result.

An instance of a device manager can then be created using the
:py:class:`~dynamite.system.deviceManager.DeviceManager` class

Example
-------

The following creates a device manager that is able to deal with
:py:class:`~dynamite.system.messages.Status` messages.

.. code-block:: python

    import dynamite.system.deviceManager

    class MyDM(dynamite.system.deviceManager.DeviceManagerImplementation):

        def handleStatus(self, message):
            return {"healthy": True}

It can then be instantiated using

.. code-block:: python

    deviceManager = DeviceManager("localhost", "myDeviceManager")

"""

import datetime
import logging
import sys
import time

from c4.messaging import (DealerRouter,
                          RouterClient,
                          callMessageHandler)
from c4.system.configuration import States
from c4.utils.jsonutil import JSONSerializable
from c4.utils.logutil import ClassLogger


log = logging.getLogger(__name__)

@ClassLogger
class DeviceManager(DealerRouter):
    """
    Device Manager

    :param node: node name
    :type node: str
    :param name: name
    :type name: str
    :param implementation: implementation of handlers
    :type implementation: :class:`~dynamite.system.deviceManager.DeviceManagerImplementation`
    :param properties: optional properties
    :type properties: dict
    :raises MessagingException: if either parent device manager is not set up or device manager address is already in use
    """
    def __init__(self, node, name, implementation, properties=None):
        addressParts = name.split("/")
        addressParts.insert(0, node)
        routerAddress = "/".join(addressParts[:-1])
        address = "/".join(addressParts)
        super(DeviceManager, self).__init__(routerAddress, address, register=True, name="DM")

        # set up device manager implementation
        self.implementation = implementation(node, name, properties)
        # connect stop flag to allow implementation to stop device manager
        self.implementation.stopFlag = self.stopFlag

        self.addHandler(self.implementation.routeMessage)

    @property
    def node(self):
        """
        Get node name

        :returns: str
        """
        return self.implementation.node

    def stop(self, wait=False):
        """
        Stop the device manager

        :returns: :class:`~dynamite.system.deviceManager.DeviceManager`
        """
        log.debug("Stopping device manager '%s' on '%s'", self.name, self.node)
        self.stopFlag.set()
        if wait:
            while self.is_alive():
                time.sleep(1)
            log.debug("Stopped '%s'", self.name)
        return self

class DeviceManagerImplementation(object):
    """
    Device manager implementation which provides the handlers for messages.
    This implementation will be provided to each worker in the process pool.

    :param node: node name
    :type node: str
    :param name: name
    :type name: str
    :param properties: optional properties
    :type properties: dict
    """
    def __init__(self, node, name, properties=None):
        super(DeviceManagerImplementation, self).__init__()
        self.node = node
        self.name = name
        if properties is None:
            self.properties = {}
        else:
            self.properties = properties
        self.stopFlag = None

    def handleLocalStartDeviceManager(self, message, envelope):
        """
        Handle :class:`~dynamite.system.messages.LocalStartDeviceManager` messages

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~dynamite.system.messages.Envelope`
        """
        log.info("Received start request for %s", self.name)
        self.start()
        module = sys.modules[self.__class__.__module__]
        return {
                "state": States.RUNNING,
                "version": getattr(module, "__version__", "unknown")
                }

    def handleLocalStopDeviceManager(self, message, envelope):
        """
        Handle :class:`~dynamite.system.messages.LocalStopDeviceManager` messages

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~dynamite.system.messages.Envelope`
        """
        log.info("Received stop request for %s", self.name)
        self.stop()
        message["state"] = States.REGISTERED
        envelope.toResponse(message)
        envelope.From = "{0}/{1}".format(self.node, self.name)
        upstreamAddress = "/".join(envelope.From.split("/")[:-1])
        RouterClient(upstreamAddress).forwardMessage(envelope)

        self.stopFlag.set()

    def routeMessage(self, envelopeString, envelope):
        """
        Route message packaged in an WS-Addressing like envelope accordingly

        :param envelopeString: envelope JSON string
        :type envelopeString: str
        :param envelope: envelope
        :type envelope: :class:`~dynamite.messaging.Envelope`
        :returns: response
        """
        return callMessageHandler(self, envelope)

    def start(self):
        """
        Start device manager implementation
        """
        pass

    def stop(self):
        """
        Stop device manager implementation
        """
        pass

class DeviceManagerStatus(JSONSerializable):
    """
    Device manager status which can be extended to include additional details

    """
    def __init__(self):
        super(DeviceManagerStatus, self).__init__()
        utcTime = datetime.datetime.utcnow()
        self.timestamp = "{:%Y-%m-%d %H:%M:%S}.{:03d}".format(utcTime, utcTime.microsecond // 1000)
