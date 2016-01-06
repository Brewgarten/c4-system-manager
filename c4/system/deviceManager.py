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

import ctypes
import datetime
import multiprocessing
import sys
import time

from c4.messaging import (DealerRouter,
                          RouterClient,
                          callMessageHandler)
from c4.system.configuration import States
from c4.system.messages import LocalStopDeviceManager
from c4.utils.jsonutil import JSONSerializable
from c4.utils.logutil import ClassLogger


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
        self.addHandler(self.implementation.routeMessage)

    @property
    def node(self):
        """
        Get node name

        :returns: str
        """
        return self.implementation.node

    def start(self, timeout=60):
        """
        Start the device manager

        :param timeout: timeout in seconds
        :type timeout: int
        :returns: whether start was successful
        :rtype: bool
        """
        self.log.debug("starting device mananager '%s'", self.address)
        return super(DeviceManager, self).start(timeout=timeout)

    def stop(self, timeout=60):
        """
        Stop the device manager

        :param timeout: timeout in seconds
        :type timeout: int
        :returns: whether stop was successful
        :rtype: bool
        """
        self.log.debug("stopping device manager '%s' on '%s'", self.address, self.node)

        # stop child device managers, this is required, otherwise device manager processes won't stop
        if self.implementation.state == States.RUNNING:
            client = RouterClient(self.address)
            client.sendRequest(LocalStopDeviceManager(self.routerAddress, self.address))

            # give device managers and sub processes time to stop
            end = time.time() + 60
            while time.time() < end:
                if self.implementation.state != States.REGISTERED:
                    self.log.debug("waiting for device manager '%s' to return to '%s', current state is '%s'",
                                   self.address,
                                   repr(States.REGISTERED),
                                   repr(self.implementation.state)
                                   )
                    time.sleep(1)
                else:
                    break
            else:
                self.log.error("waiting for device manager '%s' to return to '%s' timed out",
                               self.address,
                               repr(States.REGISTERED)
                               )
                return False

        return super(DeviceManager, self).stop(timeout=timeout)

@ClassLogger
class DeviceManagerImplementation(object):
    """
    Device manager implementation which provides the handlers for messages.

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
        self._state = multiprocessing.Value(ctypes.c_char_p, States.REGISTERED.name)  # @UndefinedVariable

    def handleLocalStartDeviceManager(self, message, envelope):
        """
        Handle :class:`~dynamite.system.messages.LocalStartDeviceManager` messages

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~dynamite.system.messages.Envelope`
        """
        self.log.debug("received start request")
        self.state = States.RUNNING
        module = sys.modules[self.__class__.__module__]
        return {
            "state": self.state,
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
        self.log.debug("received stop request")
        self.state = States.REGISTERED
        return {
            "state": self.state
        }

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

    @property
    def state(self):
        """
        Device manager state
        """
        return States.valueOf(self._state.value)

    @state.setter
    def state(self, state):
        if isinstance(state, States):
            with self._state.get_lock():
                self._state.value = state.name
        else:
            self.log.error("'%s' does not match enum of type '%s'", state, States)

class DeviceManagerStatus(JSONSerializable):
    """
    Device manager status which can be extended to include additional details

    """
    def __init__(self):
        super(DeviceManagerStatus, self).__init__()
        utcTime = datetime.datetime.utcnow()
        self.timestamp = "{:%Y-%m-%d %H:%M:%S}.{:03d}".format(utcTime, utcTime.microsecond // 1000)
