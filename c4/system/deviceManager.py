"""
Copyright (c) IBM 2015-2017. All Rights Reserved.
Project name: c4-system-manager
This project is licensed under the MIT License, see LICENSE

This module provides a device manager interface to implement device managers.

In particular one should extend :py:class:`~c4.system.deviceManager.DeviceManagerImplementation`
to implement various message handlers. Note that by default the handlers follow this pattern:

    ``def handle<MessageType>(self, message)``
    or
    ``def handle<MessageType>(self, message, envelope)``

and return a ``dict`` which becomes message result.

An instance of a device manager can then be created using the
:py:class:`~c4.system.deviceManager.DeviceManager` class

Example
-------

The following creates a device manager that is able to deal with
:py:class:`~c4.system.messages.Status` messages.

.. code-block:: python

    import c4.system.deviceManager

    class MyDM(c4.system.deviceManager.DeviceManagerImplementation):

        def handleStatus(self, message):
            return {"healthy": True}

It can then be instantiated using

.. code-block:: python

    deviceManager = DeviceManager("localhost", "myDeviceManager")

"""

import ctypes
import inspect
import logging
import multiprocessing
import re
import sys
import time

from c4.messaging import (DealerRouter,
                          RouterClient,
                          callMessageHandler)
from c4.system.backend import Backend
from c4.system.configuration import States
from c4.system.messages import LocalStopDeviceManager
from c4.utils.command import run
from c4.utils.jsonutil import JSONSerializable, Datetime
from c4.utils.logutil import ClassLogger
from c4.utils.util import callWithVariableArguments, getVariableArguments
from c4.system.monitoring import ClassMonitor


log = logging.getLogger(__name__)

NOT_RUNNING_ACTIONS = set([
    "LocalStartDeviceManager",
    "LocalStopDeviceManager",
    "Status"
])

def operation(implementation):
    """
    Operation decorator to be used on methods of the device manager that
    should be exposed externally as operations

    :param implementation: a device manager method
    :returns: method decorated with additional operation information
    """
    handlerArgSpec = inspect.getargspec(implementation)
    if inspect.ismethod(implementation):
        handlerArguments = handlerArgSpec.args[1:]
    elif inspect.isfunction(implementation):
        handlerArguments = handlerArgSpec.args
    else:
        log.error("%s needs to be a method or function", implementation)
        return implementation

    if "self" in handlerArguments:
        handlerArguments.remove("self")

    if handlerArgSpec.defaults is None:
        handlerDefaults = []
    else:
        handlerDefaults = handlerArgSpec.defaults

    lastRequiredArgumentIndex = len(handlerArguments)-len(handlerDefaults)
    requiredHandlerArguments = handlerArguments[:lastRequiredArgumentIndex]
    optionalHandlerArguments = handlerArguments[lastRequiredArgumentIndex:]

    # add operation information to the implementation
    implementation.operation = {
        "name": implementation.__name__
    }
    if implementation.__doc__:
        descriptionLines = [
            line.strip()
            for line in implementation.__doc__.strip().splitlines()
            if line
        ]
        implementation.operation["description"] = "\n".join(descriptionLines)
    if requiredHandlerArguments:
        implementation.operation["required"] = requiredHandlerArguments
    if optionalHandlerArguments:
        implementation.operation["optional"] = optionalHandlerArguments
    return implementation

@ClassLogger
class DeviceManager(DealerRouter):
    """
    Device Manager

    :param clusterInfo: cluster information
    :type clusterInfo: :class:`~c4.system.configuration.ClusterInfo`
    :param name: name
    :type name: str
    :param implementation: implementation of handlers
    :type implementation: :class:`~c4.system.deviceManager.DeviceManagerImplementation`
    :param properties: optional properties
    :type properties: dict
    :raises MessagingException: if either parent device manager is not set up or device manager address is already in use
    """
    def __init__(self, clusterInfo, name, implementation, properties=None):
        addressParts = name.split("/")
        addressParts.insert(0, clusterInfo.node)
        routerAddress = "/".join(addressParts[:-1])
        address = "/".join(addressParts)
        if not properties:
            properties = {}
        super(DeviceManager, self).__init__(routerAddress, address, maxThreads=properties.get("maxThreads", 2), register=True, name="DM")
        self.clusterInfo = clusterInfo

        # set up device manager implementation
        self.implementation = implementation(self.clusterInfo, name, properties)
        self.addHandler(self.implementation.routeMessage)

    @property
    def node(self):
        """
        Node name

        :returns: str
        """
        return self.clusterInfo.node

    def run(self):
        """
        Override DealerRouter::run in order to introduce this hack
        """
        #FIXME: this is a hack to prevent etcd operations from hanging later on in this process.
        Backend().configuration.getAliases()
        super(DeviceManager, self).run()

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
            waitTime = Backend().configuration.getPlatform().settings.get("system.timeout", 60)
            end = time.time() + waitTime
            while time.time() < end:
                if self.implementation.state != States.REGISTERED:
                    self.log.debug("waiting for device manager '%s' to return to '%s', current state is '%s'",
                                   self.address,
                                   repr(States.REGISTERED),
                                   repr(self.implementation.state))
                    time.sleep(1)
                else:
                    break
            else:
                self.log.error("waiting for device manager '%s' to return to '%s' timed out",
                               self.address,
                               repr(States.REGISTERED))
                return False

        return super(DeviceManager, self).stop(timeout=timeout)

@ClassLogger
class DeviceManagerImplementation(object):
    """
    Device manager implementation which provides the handlers for messages.

    :param clusterInfo: cluster information
    :type clusterInfo: :class:`~c4.system.configuration.ClusterInfo`
    :param name: name
    :type name: str
    :param properties: optional properties
    :type properties: dict
    """
    def __init__(self, clusterInfo, name, properties=None):
        super(DeviceManagerImplementation, self).__init__()
        self.clusterInfo = clusterInfo
        self.name = name
        if properties is None:
            self.properties = {}
        else:
            self.properties = properties
        self._state = multiprocessing.Value(ctypes.c_char_p, States.REGISTERED.name)  # @UndefinedVariable

    @classmethod
    def getOperations(cls):
        """
        Get operations associated with this implementation

        :returns: operations map
        :rtype: dict
        """
        operations = {
            name: method.operation
            for name, method in inspect.getmembers(cls, inspect.ismethod)
            if hasattr(method, "operation")
        }
        return operations

    @property
    def node(self):
        """
        Node name

        :returns: str
        """
        return self.clusterInfo.node

    def handleLocalStartDeviceManager(self, message, envelope):
        """
        Handle :class:`~c4.system.messages.LocalStartDeviceManager` messages

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
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
        Handle :class:`~c4.system.messages.LocalStopDeviceManager` messages

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        self.log.debug("received stop request")
        self.state = States.REGISTERED
        return {
            "state": self.state
        }

    def handleOperation(self, message):
        """
        Handle :class:`~c4.system.messages.Operation` messages

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        operations = self.getOperations()
        if message.get("name", "unknown") in operations:

            operationImplementation = getattr(self, message["name"])
            arguments = message.get("arguments", [])
            keywordArguments = message.get("keywordArguments", {})

            # get information on the operation implementation
            handlerArgumentMap, leftOverArguments, leftOverKeywords = getVariableArguments(operationImplementation, *arguments, **keywordArguments)

            # check for missing required arguments
            missingArguments = [
                key
                for key, value in handlerArgumentMap.items()
                if value == "_notset_"
            ]
            if missingArguments:
                return {
                    "error": "'{0}' is missing required arguments '{1}'".format(
                        message["name"],
                        ",".join(missingArguments)
                        )
                }

            response = callWithVariableArguments(operationImplementation,
                                             *arguments,
                                             **keywordArguments)

            if response is not None:
                warning = []
                if leftOverArguments:
                    warning.append("'{0}' has left over arguments '{1}'".format(
                        message["name"],
                        ",".join(str(a) for a in leftOverArguments)
                        ))
                if leftOverKeywords:
                    warning.append("'{0}' has left over keyword arguments '{1}'".format(
                        message["name"],
                        ",".join(leftOverKeywords)
                        ))
                if warning:
                    response["warning"] = "\n".join(warning)

            return response
        else:
            return {"error": "unsupported operation '{0}'".format(message.get("name", message))}

    def routeMessage(self, envelopeString, envelope):
        """
        Route message packaged in an WS-Addressing like envelope accordingly

        :param envelopeString: envelope JSON string
        :type envelopeString: str
        :param envelope: envelope
        :type envelope: :class:`~c4.messaging.Envelope`
        :returns: response
        """
        if self.state == States.RUNNING or envelope.Action in NOT_RUNNING_ACTIONS:
            return callMessageHandler(self, envelope)
        else:
            warning = "message with action '{action}' will not be handled because it is not allowed when the device is not in 'RUNNING' state, currently '{state}'".format(
                action=envelope.Action,
                state=self.state.name)
            self.log.warning(warning)
            return {"warning": warning}

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

@ClassLogger
@ClassMonitor
class ConfiguredDeviceManagerImplementation(DeviceManagerImplementation):
    """
    Device manager implementation for services with a specified DeviceManagerConfiguration
    """
    def __init__(self, clusterInfo, name, properties=None):
        super(ConfiguredDeviceManagerImplementation, self).__init__(clusterInfo, name, properties=properties)
        if properties.has_key("configuration"):
            self.dmConfiguration = properties["configuration"]
        else:
            raise ValueError("No DeviceManagerConfiguration found in properties.")
        self.statusWarningIssued = False
        # Override the default event (class name) with the name of the managed device.
        self.monitor.event = name

    def handleLocalStartDeviceManager(self, message, envelope):
        """
        Handle :class:`~c4.system.messages.LocalStartDeviceManager` messages
        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        self.log.info("Starting %s device manager", self.name)
        self.start()
        return super(ConfiguredDeviceManagerImplementation, self).handleLocalStartDeviceManager(message, envelope)

    def handleLocalStopDeviceManager(self, message, envelope):
        """
        Handle :class:`~c4.system.messages.LocalStopDeviceManager` messages
        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        if not self.dmConfiguration.alwaysOn:
            self.stop()
        return super(ConfiguredDeviceManagerImplementation, self).handleLocalStopDeviceManager(message, envelope)


    def handleStatus(self):
        """
        The handler for an incoming Status message.
        """
        stdout, stderr, rc = run(self.dmConfiguration.statusCommand)
        if self.dmConfiguration.statusRegex:
            runningPattern = re.compile(self.dmConfiguration.statusRegex)
            match = runningPattern.search(stdout)
            status = ConfiguredDeviceManagerStatus.OK if match else ConfiguredDeviceManagerStatus.FAILED
        else:
            status = ConfiguredDeviceManagerStatus.OK if rc == self.dmConfiguration.rc else ConfiguredDeviceManagerStatus.FAILED

        if status == ConfiguredDeviceManagerStatus.FAILED:
            # Suppress the warning if 1) we aren't running or 2) we already issued the warning once
            if self.state == States.RUNNING and not self.statusWarningIssued:
                self.log.warning("Unexpected %s status: stdout: %s, stderr: %s, rc: %s", self.name, stdout, stderr, rc)
                self.statusWarningIssued = True
        else:
            self.statusWarningIssued = False
        return ConfiguredDeviceManagerStatus(self.state, status)

    @operation
    def start(self, isRecovery=False):
        """
        Start the configured service.
        """
        if self.state == States.STARTING:
            self.log.debug("%s received start request, but state is already STARTING.", self.name)
            return
        # Make sure we didn't already recover to OK status
        if self.handleStatus().status != ConfiguredDeviceManagerStatus.OK:
            self.state = States.STARTING
            stdout, stderr, rc = run(self.dmConfiguration.startCommand)
            if rc != 0:
                self.log.error("Error starting {0} service. stdout: %s, stderr: %s, rc: %s", self.name, stdout, stderr, rc)

            if isRecovery:
                self.monitor.report(self.monitor.SUCCESS if rc ==0 else self.monitor.FAILURE)

            self.state = States.RUNNING

    @operation
    def stop(self):
        """
        Stop the configured service.
        """
        stdout, stderr, rc = run(self.dmConfiguration.stopCommand)
        if rc != 0:
            self.log.error("Error stopping {0} service. stdout: %s, stderr: %s, rc: %s", self.name, stdout, stderr, rc)

class DeviceManagerStatus(JSONSerializable):
    """
    Device manager status which can be extended to include additional details

    """
    def __init__(self):
        super(DeviceManagerStatus, self).__init__()
        self.timestamp =  Datetime.utcnow()

class ConfiguredDeviceManagerStatus(DeviceManagerStatus):
    """
    Policy engine device manager status
    :param state: state
    :type state: :class:`~c4.system.configuration.States`
    """
    OK = "OK"
    FAILED = "FAILED"

    def __init__(self, state, status):
        super(ConfiguredDeviceManagerStatus, self).__init__()
        self.state = state
        self.status = status