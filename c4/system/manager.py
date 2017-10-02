#!/usr/bin/env python
"""
Copyright (c) IBM 2015-2017. All Rights Reserved.
Project name: c4-system-manager
This project is licensed under the MIT License, see LICENSE
"""
import argparse
import errno
import imp
import json
import logging.config
import multiprocessing
import os
import re
import signal
import socket
import sys
import time
import pkg_resources
from cloghandler import ConcurrentRotatingFileHandler

import c4.devices
from c4.messaging import (MessageTracker,
                          MessagingException,
                          PeerRouter,
                          RouterClient,
                          callMessageHandler,
                          DEFAULT_IPC_PATH)
from c4.system import egg
from c4.system.configuration import (ConfigurationInfo,
                                     NodeInfo,
                                     States,
                                     Roles,
                                     RoleInfo)
from c4.system.deviceManager import (DeviceManager,
                                     DeviceManagerImplementation,
                                     ConfiguredDeviceManagerImplementation)
from c4.system.messages import (DisableNode,
                                LocalStartDeviceManager,
                                LocalStopDeviceManager,
                                LocalStopNode,
                                RegistrationNotification,
                                RemoveDeviceManagers,
                                StartDeviceManagers,
                                StartNode,
                                StopNode)
from c4.utils.jsonutil import JSONSerializable, Datetime
from c4.utils.logutil import ClassLogger
from c4.utils.util import getFullModuleName, getModuleClasses
import platform as platformSpec
from c4.system.backend import Backend, BackendInfo

log = logging.getLogger(__name__)

DEFAULT_CONFIGURATION_PATH = "/etc/c4"

NOT_RUNNING_ACTIONS = set([
    "RegistrationNotification",
    "StartNode",
    "StopNode"
])

@ClassLogger
class SystemManager(PeerRouter):
    """
    System Manager

    :param clusterInfo: cluster information
    :type clusterInfo: :class:`~c4.system.configuration.ClusterInfo`
    :param name: name
    :type name: str
    :raises MessagingException: if either external or internal system manager address is already in use
    """
    def __init__(self, clusterInfo, name="SM"):
        super(SystemManager, self).__init__(clusterInfo.node, clusterInfo, name=name)

        # keep one message tracker for all implementations
        self.messageTracker = MessageTracker()

        # set up system manager implementation
        self.implementation = SystemManagerImplementation(self.clusterInfo, self.messageTracker)

        self.addHandler(self.implementation.routeMessage)

    def run(self):
        """
        Override PeerRouter::run in order to introduce this hack
        """
        #FIXME: this is a hack to prevent etcd operations from hanging later on in this process.
        Backend().configuration.getAliases()
        super(SystemManager, self).run()

    def start(self, timeout=60):
        """
        Start the system manager

        :param timeout: timeout in seconds
        :type timeout: int
        :returns: whether start was successful
        :rtype: bool
        """
        self.log.info("Starting system manager '%s'", self.address)
        started = super(SystemManager, self).start(timeout=timeout)
        if not started:
            return False

        self.clusterInfo.state = States.REGISTERING

        # send registration notification
        client = RouterClient(self.address)
        envelope =  RegistrationNotification(self.address)
        envelope.Message["node"] = NodeInfo(self.address,
                                            self.clusterInfo.getNodeAddress(self.address),
                                            self.clusterInfo.role,
                                            self.clusterInfo.state)
        envelope.Message["version"] = getattr(c4.system, "__version__", "unknown")

        # wait for state change to registered
        waitTime = Backend().configuration.getPlatform().settings.get("system.timeout", 60)
        end = time.time() + waitTime
        try:
            while time.time() < end:
                if self.clusterInfo.state == States.REGISTERING:
                    client.forwardMessage(envelope)
                    time.sleep(1)
                else:
                    break
            else:
                self.log.error("registering '%s' timed out", self.address)
                return False
        except KeyboardInterrupt:
            self.log.info("terminating registration for '%s'", self.address)
            return False

        self.log.info("System manager '%s' started", self.address)
        return True

    def stop(self, timeout=60):
        """
        Stop the system manager

        :param timeout: timeout in seconds
        :type timeout: int
        :returns: whether stop was successful
        :rtype: bool
        """
        self.log.info("Stopping system manager '%s'", self.address)

        # stop child device managers, this is required, otherwise device manager processes won't stop
        if self.clusterInfo.state == States.RUNNING:
            client = RouterClient(self.address)
            client.forwardMessage(StopNode(self.address))

            # give device managers and sub processes time to stop
            waitTime = Backend().configuration.getPlatform().settings.get("system.timeout", 60)
            end = time.time() + waitTime
            while time.time() < end:
                if self.clusterInfo.state != States.REGISTERED:
                    self.log.debug("waiting for system manager '%s' to return to '%s', current state is '%s'",
                                   self.address,
                                   repr(States.REGISTERED),
                                   repr(self.clusterInfo.state)
                                   )
                    devicesNotRegistered = self.getDevicesNotRegistered()
                    if len(devicesNotRegistered) > 0:
                        self.log.info("Waiting for devices to stop: %s", ", ".join(devicesNotRegistered))
                    time.sleep(5)
                else:
                    break
            else:
                self.log.error("Waiting for system manager '%s' to return to '%s' timed out. Forcing termination of child processes.",
                               self.address,
                               repr(States.REGISTERED)
                               )
                # Force termination
                client.forwardMessage(StopNode(self.address, terminate=True))

        return super(SystemManager, self).stop(timeout=timeout)

    def getDevicesNotRegistered(self):
        """
        Build a list of devices on this node that are not in registered state.
        :returns: list of device names
        """
        devices = Backend().configuration.getDevices(self.address, flatDeviceHierarchy=True)
        devicesNotRegistered = []
        for deviceInfo in devices.values():
            if deviceInfo.state != States.REGISTERED:
                devicesNotRegistered.append(deviceInfo.name)
        return devicesNotRegistered

@ClassLogger
class SystemManagerImplementation(object):
    """
    System manager implementation which provides the handlers for messages.

    :param clusterInfo: cluster information
    :type clusterInfo: :class:`~c4.system.configuration.ClusterInfo`
    :param messageTracker: message tracker
    :type messageTracker: :class:`~c4.messaging.MessageTracker`
    """
    def __init__(self, clusterInfo, messageTracker):
        super(SystemManagerImplementation, self).__init__()
        self.clusterInfo = clusterInfo
        self.messageTracker = messageTracker

        # reusable system manager router client
        self._client = None

    @property
    def client(self):
        """
        System manager router client
        """
        # TODO: determine if there is a better way to share the client, the problem is that during init the actual system manager router address is not set up
        if self._client is None:
            self._client = RouterClient(self.node)
        return self._client

    @property
    def deviceManagers(self):
        """
        Mapping of child device manager names to their processes
        """
        return {
            child.address: child
            for child in multiprocessing.active_children()
            if isinstance(child, DeviceManager)
        }

    def _handleDeployEgg(self, message, envelope):
        """
        Handle :class:`~c4.system.messages.DeployDeviceManager` messages.
        A message from the rest server to the active system manager to distribute and install the
        custom device manager egg.
        Or a message from the active system manager to the other system managers to install the
        custom device manager egg.

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        # e.g. "c4.devices.mydm.MyDM"
        egg_type = message["type"]

        # if from rest server to the active system manager
        (node_name, component_name) = self.parseFrom(envelope.From)
        if component_name == "rest":
            egg.sendDeployToOtherSystemManagers(self.node, self.clusterInfo.nodeNames,
                                                self.clusterInfo.getNodeAddress("system-manager"),
                                                message["type"], message["file_name"], message["data"])
        # all sysmgrs will now install the egg
        if not egg.deployEgg(egg_type, message["file_name"], message["data"]):
            return

        # import the new module
        dist = pkg_resources.get_distribution(egg_type)
        # remove the class from the type
        temp_mod_list = egg_type.split(".")
        temp_mod_list.pop()
        # e.g. "c4.devices.mydm"
        egg_mod = ".".join(temp_mod_list)
        # e.g. "/c4/system/devices/mydm.py"
        egg_path = "/" + "/".join(temp_mod_list) + ".py"
        # import this new module and
        # reload parent module, so that when we query for the list
        # of modules, this newly installed module will show up
        new_mod = imp.load_source(egg_mod, dist.location + egg_path)
        if egg_type.startswith("c4.devices"):
            reload(c4.devices)
        elif egg_type.startswith("c4.system.policies"):
            reload(c4.system.policies)
        elif egg_type.startswith("c4.system.actions"):
            reload(c4.system.actions)
        elif egg_type.startswith("c4.system.events"):
            reload(c4.system.events)
        else:
            self.log.error("Unsupported egg type: %s", egg_type)

    def _handleUndeployEgg(self, message, envelope):
        """
        Handle :class:`~c4.system.messages.UndeployDeviceManager` messages.
        A message from the rest server to the active system manager to delete the egg.  The system manager
        will also tell other system managers to delete the egg.
        Or a message from the active system manager to the other system managers to delete the egg.

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        self.log.debug("Undeploy device manager request from %s", envelope.From)
        # if from rest server to the active system manager
        (node_name, component_name) = self.parseFrom(envelope.From)
        if component_name == "rest":
            egg.sendUndeployToOtherSystemManagers(self.node, self.clusterInfo.nodeNames,
                                                self.clusterInfo.getNodeAddress("system-manager"),
                                                message["type"])
        # all sysmgrs will now uninstall the egg
        if not egg.undeployEgg(message["type"]):
            return

    def getDeviceManagerImplementations(self):
        # retrieve available device manager implementations
        unsortedImplementations = getModuleClasses(c4.devices, DeviceManagerImplementation)
        #Special case for generic services handled through configuration
        unsortedImplementations.append(ConfiguredDeviceManagerImplementation)
        deviceManagerImplementations = sorted(unsortedImplementations)

        if DeviceManagerImplementation in deviceManagerImplementations:
            deviceManagerImplementations.remove(DeviceManagerImplementation)
        return deviceManagerImplementations

    # TODO: documentation needs to be updated
    def getNodeDictFromDeviceList(self, device_list, node_list, configurationInfo):
        """
        Returns a dictionary where the keys are node names and the value is a
        dictionary of device names and deviceInfo (from configuration).
        The device names key are the full dotted device name.

        Transforms device wildcards.

        Throws exception on invalid device in device list.
        """
        node_dict = {}
        # * means all devices
        if "*" in device_list:
            def getAllDevices(parent_prefix, node_or_device, valid_devices_dict):
                for device_name, device_info in node_or_device.devices.iteritems():
                    full_device_name = parent_prefix + device_name
                    valid_devices_dict[full_device_name] = device_info
                    getAllDevices(full_device_name + ".", device_info, valid_devices_dict)

            # FIXME: all configuration dependent code should probably use the configuration directly
            for node_name, node in configurationInfo.nodes.iteritems():
                if node_name in node_list:
                    valid_devices_dict = {}
                    getAllDevices("", node, valid_devices_dict)
                    node_dict[node_name] = valid_devices_dict
            return node_dict

        # else
        # go through each provided device and validate
        # if valid device name, then add it
        # FIXME: all configuration dependent code should probably use the configuration directly
        for node_name, node in configurationInfo.nodes.iteritems():
            if node_name in node_list:
                all_devices_for_this_node = node.devices.keys()
                valid_devices_dict = {}
                # validate device name
                for device_name in device_list:
                    # check the top level device name against the node's devices
                    device_name_parts = device_name.split(".")
                    if device_name_parts[0] not in all_devices_for_this_node:
                        raise Exception("Invalid device %s for node %s" % (device_name, node_name))
                    device_info = node.devices[device_name_parts[0]]
                    # if the device is in a hierarchy, check each device name in the hierarchy
                    # and set device_info to the last device's info in the hierarchy
                    device_name_parts.pop(0)
                    for device_name_part in device_name_parts:
                        if device_name_part not in device_info.devices:
                            raise Exception("Invalid device %s for node %s" % (device_name, node_name))
                        device_info = device_info.devices[device_name_part]
                    valid_devices_dict[device_name] = device_info
                node_dict[node_name] = valid_devices_dict
        return node_dict

    def handleDeployDeviceManager(self, message, envelope):
        """
        Handle :class:`~c4.system.messages.DeployDeviceManager` messages.
        A message from the rest server to the active system manager to distribute and install the
        custom device manager egg.
        Or a message from the active system manager to the other system managers to install the
        custom device manager egg.

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        self.log.debug("Deploy device manager request from %s", envelope.From)
        self._handleDeployEgg(message, envelope)

    def handleDisableDeviceManager(self, message, envelope):
        """
        Handle :class:`~c4.system.messages.DisableDeviceManager` messages.
        A message from the active system manager to a system manager to
        disable one or more device managers

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        self.log.debug("Disable device manager request from %s for %s",
                    envelope.From, message)

        if "devices" not in message:
            self.log.error("Unable to disable devices because no devices were supplied.")
            return

        response = {'status': States.MAINTENANCE}
        response["devices"] = message["devices"]
        return response

    def handleDisableDeviceManagerResponse(self, message, envelope):
        """
        Handle :class:`~c4.system.messages.DisableDeviceManager` messages.
        A message from a system manager to an active system manager to
        acknowlege disabling one or more device managers.

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        self.log.debug("Disable device manager response from %s for %s",
                    envelope.From, message)

        # validate role
        if self.clusterInfo.role != Roles.ACTIVE:
            self.log.error("Received disable device manager response from '%s' but current role is '%s', should be '%s'",
                           envelope.From, self.clusterInfo.role, Roles.ACTIVE)
            return

        (node_name, from_device_name) = self.parseFrom(envelope.From)

        if "devices" not in message:
            self.log.error("Received disable device manager response, but no devices were provided.")
            return

        for device_name in message["devices"]:
            self.log.debug("Changing %s/%s state from REGISTERED to MAINTENANCE.", node_name, device_name)
            Backend().configuration.changeState(node_name, device_name, States.MAINTENANCE)

    def handleDisableNode(self, message, envelope):
        """
        Handle :class:`~c4.system.messages.Disable` messages.
        A message from the active system manager to a system manager to
        disable itself

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        self.log.debug("Disable node request from %s for %s",
                    envelope.From, message)

        response = {'status': States.MAINTENANCE}
        return response

    def handleDisableNodeResponse(self, message, envelope):
        """
        Handle :class:`~c4.system.messages.DisableNode` messages.
        A message from a system manager to an active system manager to
        acknowlege disabling one or more device managers.

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        self.log.debug("Disable node response from %s for %s",
                    envelope.From, message)

        # validate role
        if self.clusterInfo.role != Roles.ACTIVE:
            self.log.error("Received disable node response from '%s' but current role is '%s', should be '%s'",
                           envelope.From, self.clusterInfo.role, Roles.ACTIVE)
            return

        (node_name, from_device_name) = self.parseFrom(envelope.From)

        # Received request to change the state of a node
        Backend().configuration.changeState(node_name, None, States.MAINTENANCE)

    def handleEnableDeviceManager(self, message, envelope):
        """
        Handle :class:`~c4.system.messages.Enable` messages.
        A message from the active system manager to a system manager to
        enable one or more device managers

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        self.log.debug("Enable device manager request from %s for %s",
                    envelope.From, message)

        if "devices" not in message:
            self.log.error("Unable to enable devices because no devices were supplied.")
            return

        response = {'status': States.REGISTERED}
        response["devices"] = message["devices"]
        return response

    def handleEnableDeviceManagerResponse(self, message, envelope):
        """
        Handle :class:`~c4.system.messages.EnableDeviceManager` messages.
        A message from a system manager to an active system manager to
        acknowlege enabling one or more device managers.

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        self.log.debug("Enable device manager response from %s for %s",
                    envelope.From, message)

        # validate role
        if self.clusterInfo.role != Roles.ACTIVE:
            self.log.error("Received enable device manager response from '%s' but current role is '%s', should be '%s'",
                           envelope.From, self.clusterInfo.role, Roles.ACTIVE)
            return

        if "devices" not in message:
            self.log.error("Unable to enable devices because no devices were supplied.")
            return

        (node_name, from_device_name) = self.parseFrom(envelope.From)

        configuration = Backend().configuration
        # change device state from MAINTENANCE to REGISTERED
        for device_name in message["devices"]:
            self.log.debug("Changing %s/%s state from MAINTENANCE to REGISTERED.", node_name, device_name)
            configuration.changeState(node_name, device_name, States.REGISTERED)

        if self.messageTracker.isARelatedMessage(envelope.RelatesTo):
            original_message_id = self.messageTracker.removeRelatedMessage(envelope.RelatesTo)
            if not self.messageTracker.hasMoreRelatedMessages(original_message_id):
                original_message = self.messageTracker.remove(original_message_id)
                # the devices are now all registered, so now start them
                # the original_message came from handleStartdevicemanager
                if type(original_message) == StartDeviceManagers:
                    self.log.debug("Sending original start message: %s to %s", original_message.Message, original_message.To)
                    self.client.forwardMessage(original_message)

    def handleEnableNode(self, message, envelope):
        """
        Handle :class:`~c4.system.messages.EnableNode` messages.
        A message from the active system manager to a system manager to
        enable itself

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        self.log.debug("Enable node request from %s for %s",
                    envelope.From, message)

        response = {'status': States.REGISTERED}
        return response

    def handleEnableNodeResponse(self, message, envelope):
        """
        Handle :class:`~c4.system.messages.EnableNode` messages.
        A message from a system manager to an active system manager to
        acknowlege enabling itself.

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        self.log.debug("Enable node response from %s for %s",
                    envelope.From, message)

        # validate role
        if self.clusterInfo.role != Roles.ACTIVE:
            self.log.error("Received enable node response from '%s' but current role is '%s', should be '%s'",
                           envelope.From, self.clusterInfo.role, Roles.ACTIVE)
            return

        (node_name, from_device_name) = self.parseFrom(envelope.From)

        # Received request to change the state of a node
        Backend().configuration.changeState(node_name, None, States.REGISTERED)

    def handleLocalStartDeviceManagerResponse(self, message, envelope):
        """
        Handle :class:`~c4.system.messages.LocalStartDeviceManagerResponse` response messages

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        self.log.debug("Received start acknowledgement from '%s'", envelope.From)
        (node, device_name) = self.parseFrom(envelope.From)

        configuration = Backend().configuration
        if message.get("state", None) and isinstance(message.get("state", None), States):
            configuration.changeState(node, device_name, States.RUNNING)

        # remove response from the related messages list
        storedMessageId = self.messageTracker.removeRelatedMessage(envelope.RelatesTo)

        # store response message in stored message
        self.messageTracker.updateMessageContent(storedMessageId, {"devices" : {device_name : message}})

        # check if there are more related messages
        if not self.messageTracker.hasMoreRelatedMessages(storedMessageId):
            # last related message, so pick up the stored message and send it out
            storedMessageEnvelope = self.messageTracker.remove(storedMessageId)
            if storedMessageEnvelope is not None:
                self.client.forwardMessage(storedMessageEnvelope)

    def handleLocalStopDeviceManagerResponse(self, message, envelope):
        """
        Handle :class:`~c4.system.messages.LocalStopDeviceManager` response messages

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        self.log.debug("Received stop acknowledgement from '%s'", envelope.From)
        (node, device_name) = self.parseFrom(envelope.From)

        # get device manager sub process
        deviceManager = self.deviceManagers.get(envelope.From)
        deviceManager.stop()

        configuration = Backend().configuration
        if message.get("state", None) and isinstance(message.get("state", None), States):
            configuration.changeState(node, device_name, States.REGISTERED)

        # remove response from the related messages list
        originalMessageId = self.messageTracker.removeRelatedMessage(envelope.RelatesTo)

        # store response message in original message
        self.messageTracker.acquire()
        originalMessageEnvelope = self.messageTracker.messages[originalMessageId]
        if "devices" not in originalMessageEnvelope.Message:
            originalMessageEnvelope.Message["devices"] = {}
        originalMessageEnvelope.Message["devices"][device_name] = message
        self.messageTracker.messages[originalMessageId] = originalMessageEnvelope
        self.messageTracker.release()

        # check if there are more related messages
        if not self.messageTracker.hasMoreRelatedMessages(originalMessageId):
            # last related message, so pick up the original message and respond to it
            originalMessageEnvelope = self.messageTracker.remove(originalMessageId)
            if originalMessageEnvelope is not None:
                self.client.forwardMessage(originalMessageEnvelope.toResponse(originalMessageEnvelope.Message))

    def handleLocalStopNode(self, message, envelope):
        """
        Handle :class:`~c4.system.messages.LocalStopNode` messages

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        # send stop to all device managers if we have any running
        self.log.debug("received local stop node message")
        deviceManagers = self.deviceManagers
        if deviceManagers:
            self.messageTracker.add(envelope)
            for deviceManagerAddress in deviceManagers.keys():
                self.log.debug("sending LocalStopDeviceManager to '%s'", deviceManagerAddress)
                stopEnvelope = LocalStopDeviceManager(self.node, deviceManagerAddress)
                self.messageTracker.addRelatedMessage(envelope.MessageID, stopEnvelope.MessageID)
                self.client.forwardMessage(stopEnvelope)

        else:
            self.clusterInfo.state = States.REGISTERED
            return {"state" : States.REGISTERED}

    def handleLocalStopNodeResponse(self, message, envelope):
        """
        Handle :class:`~c4.system.messages.LocalStopNodeResponse` response messages

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        # remove response from the related messages list
        originalMessageId = self.messageTracker.removeRelatedMessage(envelope.RelatesTo)

        # check if there are more related messages
        if not self.messageTracker.hasMoreRelatedMessages(originalMessageId):
            # last related message, so pick up the original message and respond to it
            originalMessageEnvelope = self.messageTracker.remove(originalMessageId)
            if originalMessageEnvelope is not None:

                self.clusterInfo.state = States.REGISTERED

                responseEnvelope = originalMessageEnvelope.toResponse(originalMessageEnvelope.Message)
                responseEnvelope.Message["state"] = States.REGISTERED
                if "devices" in message:
                    responseEnvelope.Message["devices"] = envelope.Message["devices"]

                self.client.forwardMessage(responseEnvelope)

        else:
            self.log.debug("%s does not have a Stop message originating from %s", message, self.node)

    def handleLocalUnregisterDeviceManagers(self, message, envelope):
        """
        Handle :class:`~c4.system.messages.LocalUnregisterDeviceManagers` messages

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        self.log.debug("Unregistering '%s' on '%s'", message['devices'], message['node'])

        node = message['node']
        configuration = Backend().configuration
        for d in message['devices']:
            try:
                configuration.removeDevice(node, d)
            except Exception as e:
                self.log.error("Could not remove '%s' from '%s' :%s", d, node, e)
        self.client.forwardMessage(envelope.toResponse(message))

    def handleLocalUnregisterDeviceManagersResponse(self, message, envelope):
        """
        Handle :class:`~c4.system.messages.LocalUnregisterDeviceManagers` response

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        # remove response from the related messages list
        originalMessageId = self.messageTracker.removeRelatedMessage(envelope.RelatesTo)

        # check if there are more related messages
        if not self.messageTracker.hasMoreRelatedMessages(originalMessageId):
            # last related message, so pick up the original message and respond to it
            self.messageTracker.remove(originalMessageId)

    def handleRegisterDeviceManager(self, message, envelope):
        """
        Handle :class:`~c4.system.messages.RegisterDeviceManager` messages.
        A message from the active system manager to a thin system manager to add one or more device managers.

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        self.log.debug("Register device manager request (local) from %s for %s on %s with type %s",
                     envelope.From, message["devices"], self.node, message["type"])
        # the thin system manager does nothing to add a device
        # the work is all done in the active system manager to update the configuration
        # see handleRegisterDeviceManagerResponse
        return {"devices": message["devices"], "type": message["type"]}

    def handleRegisterDeviceManagerResponse(self, message, envelope):
        """
        A message from the thin system manager to the active system manager to add one or more device managers
        to the configuration.

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        self.log.debug("Register device manager response from %s for %s on %s with type %s",
                     envelope.From, message["devices"], envelope.From, message["type"])

        # validate role
        if self.clusterInfo.role != Roles.ACTIVE:
            self.log.error("Received register device manager response from '%s' but current role is '%s', should be '%s'",
                           envelope.From, self.clusterInfo.role, Roles.ACTIVE)
            return

        configuration = Backend().configuration
        # add each given device name to node name
        for device_name in message["devices"]:
            try:
                # make sure device name is not already associated with each given node
                # handle hierarchical device name - if parent doesn't exist error
                node_name = envelope.From
                configuration.addDevice(node_name, device_name, message["type"])
                self.log.debug("Added device %s to node %s.", device_name, node_name)
            except:
                # addDevice will already log the error
                pass

    def handleRegistrationNotification(self, message, envelope):
        """
        Handle :class:`~c4.system.messages.RegisterNode` messages

        The active system manager receives this message.

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        self.log.debug("Registration message from %s", envelope.From)

        # Is node in configuration?
        configuration = Backend().configuration
        node = envelope.From
        if node not in self.clusterInfo.nodeNames:
            self.log.debug("Adding %s", node)
            configuration.addNode(envelope.Message["node"])
            self.log.debug("%s added to the configuration",node)

        state = configuration.getState(node)
        if state == States.REGISTERING:
            configuration.changeState(node, None, States.REGISTERED)
            response = {
                "role": configuration.getRole(node),
                "state": States.REGISTERED
                }
        else:
            response = {
                "error": "Received node registration message, but current state is {}.".format(state)
                }
        # start node
        self.client.forwardMessage(StartNode(node))

        # send registration response
        return response

    def handleRegistrationNotificationResponse(self, message):
        """
        Handle :class:`~c4.system.messages.RegisterNode` response messages

        :param message: message
        :type message: dict
        """
        if message.get("state", None) == States.REGISTERED:
            self.clusterInfo.role = message.get("role", Roles.THIN)
            self.log.debug("'%s' is now registered with role '%s'", self.node, self.clusterInfo.role.name)
        else:
            self.log.error(message["error"])

    def handleRefreshDevices(self, message, envelope):
        """
        Handle :class:`~c4.system.messages.RefreshDevices` messages

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        self.log.debug("*****************************************************************************************")
        self.log.debug("Received refresh devices message")
        self.log.debug("*****************************************************************************************")
        if self.clusterInfo.state == States.RUNNING:
            extraDevices = {}
            self.log.debug("Check devices based on assigned role...")
            # remove all devices from node, and add devices based on role
            configuration = Backend().configuration
            nodeInfo = configuration.getNode(node=self.node, includeDevices=True)
            nodeRole = nodeInfo.role
            self.log.debug("Node '%s' has role: '%s'", self.node, nodeRole.name)
            roleInfo = configuration.getRoleInfo(role=nodeRole)
            if not roleInfo:
                self.log.error("The nodes assigned role '%s' not found in configuration", nodeRole)

            else:
                # remove any devices attached to node that are not part of the role
                for device in nodeInfo.devices.values():
                    self.log.debug("Checking device '%s' ...", device.name)
                    if device not in roleInfo.devices.values():
                        #TODO: add a platform setting to make this check configurable
                        if device.type == 'c4.devices.policyengine.PolicyEngineManager':
                            self.log.debug("Ignoring policy engine")
                        else:
                            self.log.debug("Removing device: %s from node %s", device.name, self.node)
                            extraDevices[device.name]=device

                self.log.debug("Extra devices: %s", str(extraDevices))
                if extraDevices:
                    self.messageTracker.add(envelope)
                    removeEnvelope = RemoveDeviceManagers(envelope.To)
                    removeEnvelope.Message["devices"] = extraDevices
                    self.messageTracker.addRelatedMessage(envelope.MessageID, removeEnvelope.MessageID)
                    self.client.forwardMessage(removeEnvelope)

                def loadMessageWithRegisteredDevices(node_name, message, parent_prefix, node_or_device):
                    for name, deviceInfo in node_or_device.devices.iteritems():
                        # full_name is the full dotted name, but without the host
                        # e.g. db2.instance1.mln1
                        full_name = parent_prefix + name
                        if deviceInfo.state == States.REGISTERED:
                            configuration.changeState(node_name, full_name, States.STARTING)
                            message["devices"][full_name] = deviceInfo
                        loadMessageWithRegisteredDevices(node_name, message, full_name + ".", deviceInfo)

                nodeInfo = configuration.getNode(node=self.node, includeDevices=True)
                # add any devices to the node that were part of the role but missing from node
                for device in roleInfo.devices.values():
                    self.log.debug("Found device in role '%s' ...", device.name)
                    if device not in nodeInfo.devices.values():
                        #TODO: add a platform setting to make this check configurable
                        if device.type == 'c4.devices.policyengine.PolicyEngineManager':
                            self.log.debug("Ignoring policy engine")
                        else:
                            self.log.debug("Adding device: %s to node %s", device.name, self.node)
                            configuration.addDevice(node=self.node, fullDeviceName=device.name, device=device)

                startEnvelope = StartDeviceManagers(envelope.To)
                configurationInfo = configuration.toInfo()
                for node_name, a_node in configurationInfo.nodes.iteritems():
                    if envelope.To == node_name:
                        loadMessageWithRegisteredDevices(node_name, startEnvelope.Message, "", a_node)
                self.log.debug("Sending message to %s to start all REGISTERED devices: %s", envelope.To, startEnvelope.Message)
                self.client.forwardMessage(startEnvelope)
        else:
            self.log.error("Received refresh devices message but current state is '{0}'".format(self.clusterInfo.state))

    def handleRemoveDeviceManagers(self, message, envelope):
        """
        Handle :class:`~c4.system.messages.RemoveDeviceManagers` messages

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        self.log.debug("Received remove device managers request for %s from %s", message, envelope.From)

        self.messageTracker.add(envelope)
        for deviceManager in message["devices"].keys():
            self.log.debug("Sending stop to %s/%s", self.node, deviceManager)
            stopEnvelope = LocalStopDeviceManager(self.node, "{0}/{1}".format(self.node, deviceManager))
            self.messageTracker.addRelatedMessage(envelope.MessageID, stopEnvelope.MessageID)
            self.client.forwardMessage(stopEnvelope)

    def handleRemoveDeviceManagersResponse(self, message, envelope):
        """
        Handle :class:`~c4.system.messages.RemoveDeviceManager` response messages

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        self.log.debug("Received remove device manager acknowledgement from '%s'", envelope.From)
        (node, _) = self.parseFrom(envelope.From)
        configuration = Backend().configuration
        for device in message["devices"].keys():
            self.log.debug("Removing device '%s' from node '%s'", device, node)
            configuration.removeDevice(node, fullDeviceName=device)

        original_message_id = self.messageTracker.removeRelatedMessage(envelope.RelatesTo)

        if not self.messageTracker.hasMoreRelatedMessages(original_message_id):
            original_message = self.messageTracker.remove(original_message_id)
            if original_message is not None:
                self.client.forwardMessage(original_message)

    def handleStartDeviceManagers(self, message, envelope):
        """
        Handle :class:`~c4.system.messages.StartDeviceManagers` messages

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        self.log.debug("Received start device manager message %s", message)
        response = {}
        if self.clusterInfo.state == States.RUNNING:

            # convert message to response
            envelope.toResponse()
            self.messageTracker.add(envelope)

            # retrieve available device manager implementations
            deviceManagerImplementations = self.getDeviceManagerImplementations()

            # create a type map for the device manager implementations
            deviceManagerImplementationMap = {
                  getFullModuleName(implementation) + "." +  implementation.__name__ : implementation
                  for implementation in deviceManagerImplementations}

            self.startDevices(message["devices"], deviceManagerImplementationMap, envelope.MessageID)

            # check if there are more related messages, this happens when none of the device could be started
            if not self.messageTracker.hasMoreRelatedMessages(envelope.MessageID):
                # last related message, so pick up the stored message and send it out
                storedMessageEnvelope = self.messageTracker.remove(envelope.MessageID)
                if storedMessageEnvelope is not None:
                    self.client.forwardMessage(storedMessageEnvelope)
        else:
            response["error"] = "Received start message but current state is '{0}'".format(self.clusterInfo.state)
            self.log.error(response["error"])
            return response

    def handleStartDeviceManagersResponse(self, message, envelope):
        """
        Handle :class:`~c4.system.messages.StartDeviceManagersResponse` response messages

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        if self.clusterInfo.role == Roles.ACTIVE:
            self.log.debug("Received start acknowlegdment from '%s': %s", envelope.From, message)
            (node_name, _) = self.parseFrom(envelope.From)
            if "devices" in message:

                configuration = Backend().configuration
                # change state of the device manager to running for remaining devices not
                # updated and registered in the error case
                for name, info in message["devices"].items():
                    if "state" in info and isinstance(info["state"], States):
                        configuration.changeState(node_name, name, States.RUNNING)
                    elif "error" in info:
                        configuration.changeState(node_name, name, States.REGISTERED)
                        self.log.error(info["error"])
            else:

                self.log.error("Unsupported start acknowlegdment for %s", message)
        else:
            self.log.error("Received start acknowlegdment from '%s' but current role is '%s'",
                           envelope.From, self.clusterInfo.role)

    def handleStartNode(self):
        """
        Handle :class:`~c4.system.messages.StartNode` messages

        """
        self.log.debug("Received start node message")
        response = {}
        nodeState = self.clusterInfo.state
        if nodeState == States.REGISTERED or nodeState == States.REGISTERING:
            self.clusterInfo.state = States.RUNNING
            response["state"] = States.RUNNING

            # TODO: check and revise if necessary
            # get version for this node's DEPLOYED devices and system-manager
            # versionDict - key is device type and value is the version
            versionDict = {}
            deviceManagerImplementations = self.getDeviceManagerImplementations()
            for dmClass in deviceManagerImplementations:
                dmModule = sys.modules[dmClass.__module__]
                dmType = dmModule.__name__ + "." + dmClass.__name__
                versionDict[dmType] = getattr(dmModule, "__version__", "unknown")
            versionDict["c4.system.manager.SystemManager"] = getattr(c4.system, "__version__", "unknown")
            response["version_dict"] = versionDict

        else:
            response["error"] = "Received start message but current state is '{0}'".format(nodeState)
            self.log.error(response["error"])
        return response

    def handleStartNodeResponse(self, message, envelope):
        """
        Handle :class:`~c4.system.messages.StartNodeResponse` response messages

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        configuration = Backend().configuration
        # recursively load the message with the full device name and deviceInfo from configuration
        # of all devices that are REGISTERED
        def loadMessageWithRegisteredDevices(node_name, message, parent_prefix, node_or_device):
            for name, deviceInfo in node_or_device.devices.iteritems():
                # full_name is the full dotted name, but without the host
                # e.g. db2.instance1.mln1
                full_name = parent_prefix + name
                if deviceInfo.state == States.REGISTERED:
                    configuration.changeState(node_name, full_name, States.STARTING)
                    message["devices"][full_name] = deviceInfo
                loadMessageWithRegisteredDevices(node_name, message, full_name + ".", deviceInfo)

        if self.clusterInfo.role == Roles.ACTIVE:
            self.log.debug("Received start node acknowlegdment from '%s'", envelope.From)
            if "state" in message and isinstance(message["state"], States):

                # change state of the node in the configuration to running
                configuration.changeState(envelope.From, None, States.RUNNING)

                # version
                configurationInfo = configuration.toInfo()

                # start all devices that are in REGISTERED state for the node that just started
                startEnvelope = StartDeviceManagers(envelope.From)
                for node_name, a_node in configurationInfo.nodes.iteritems():
                    if envelope.From == node_name:
                        loadMessageWithRegisteredDevices(node_name, startEnvelope.Message, "", a_node)
                logging.debug("Sending message to %s to start all REGISTERED devices: %s", envelope.From, startEnvelope.Message)
                self.client.forwardMessage(startEnvelope)

            elif "error" in message:
                self.log.error(message["error"])
            else:
                self.log.error("Unsupported start acknowlegdment for %s", message)
        else:
            self.log.error("Received start acknowlegdment from '%s' but current role is '%s'",
                           envelope.From, self.clusterInfo.role)

    def handleStatus(self, message, envelope):
        """
        Handle :class:`~c4.system.messages.Status` messages.

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        # TODO: what about status requests to the specific system manager nodes, include more details?
        self.log.debug("Received status request")
        status = SystemManagerStatus()
        status.state = self.clusterInfo.state
        return status

    def handleStatusResponse(self, message, envelope):
        """
        Handle :class:`~c4.system.messages.Status` response messages.

        When the status is received from the device managers, the
        system manager will append a record to the history table and
        update a record from the latest table.

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        if self.clusterInfo.role == Roles.ACTIVE:
            (node, name) = self.parseFrom(envelope.From)
            self.log.debug("Received status from %s", envelope.From)
            if name:
                history = Backend().deviceHistory
                history.add(node, name, message)
            else:
                history = Backend().nodeHistory
                history.add(node, message)

        else:
            self.log.error("Received status response from '%s' but current role is '%s'",
                           envelope.From, self.clusterInfo.role)

    def handleStopDeviceManagers(self, message, envelope):
        """
        Handle :class:`~c4.system.messages.StopDeviceManagers` messages

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        self.log.debug("Received stop request for %s from %s", message, envelope.From)

        self.messageTracker.add(envelope)
        for deviceManager in message["devices"]:
            self.log.debug("Sending stop to %s/%s", self.node, deviceManager)
            stopEnvelope = LocalStopDeviceManager(self.node, "{0}/{1}".format(self.node, deviceManager))
            self.messageTracker.addRelatedMessage(envelope.MessageID, stopEnvelope.MessageID)
            self.client.forwardMessage(stopEnvelope)

    def handleStopDeviceManagersResponse(self, message, envelope):
        """
        Handle :class:`~c4.system.messages.StopDeviceManager` response messages

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        self.log.debug("Received stop acknowledgement from '%s'", envelope.From)
        original_message_id = self.messageTracker.removeRelatedMessage(envelope.RelatesTo)

        if not self.messageTracker.hasMoreRelatedMessages(original_message_id):
            original_message = self.messageTracker.remove(original_message_id)
            if original_message is not None:
                self.client.forwardMessage(original_message)

    def handleStopNode(self, message, envelope):
        """
        Handle :class:`~c4.system.messages.StopNode` messages

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        self.log.info("Received stop node message. terminate: %s", envelope.terminate)

        if envelope.terminate:
            for process in self.deviceManagers.values():
                process.terminate()
            self.clusterInfo.state = States.REGISTERED
        else:
            # send stop to all device managers if we have any running
            if self.deviceManagers:

                localStopNodeMessage = LocalStopNode(self.node)
                self.messageTracker.add(envelope)
                self.messageTracker.addRelatedMessage(envelope.MessageID, localStopNodeMessage.MessageID)
                self.client.forwardMessage(localStopNodeMessage)

            else:
                self.clusterInfo.state = States.REGISTERED
                return {"state" : States.REGISTERED}

    def handleStopNodeResponse(self, message, envelope):
        """
        Handle :class:`~c4.system.messages.StopNodeResponse` response messages

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        if self.clusterInfo.role == Roles.ACTIVE:

            self.log.debug("Received stop node acknowlegdment from '%s'", envelope.From)

            if "state" in message and isinstance(message["state"], States):

                configuration = Backend().configuration
                allDevicesStopped = True
                for deviceName, deviceInfo in message.get("devices", {}).items():

                    if "state" in deviceInfo and isinstance(deviceInfo["state"], States):
                        configuration.changeState(envelope.From, deviceName, States.REGISTERED)
                    elif "error" in deviceInfo:
                        self.log.error(deviceInfo["error"])
                        allDevicesStopped = False
                    else:
                        self.log.error("Unsupported stop node acknowlegdment for %s: %s", deviceName, deviceInfo)

                if allDevicesStopped:
                    configuration.changeState(envelope.From, None, States.REGISTERED)

            originalMessageId = self.messageTracker.removeRelatedMessage(envelope.RelatesTo)
            if not self.messageTracker.hasMoreRelatedMessages(originalMessageId):
                # last related message, so pick up the original message and respond to it
                originalMessageEnvelope = self.messageTracker.remove(originalMessageId)
                if originalMessageEnvelope is not None:
                    if self.messageTracker.isARelatedMessage(originalMessageEnvelope.MessageID):
                        self.client.forwardMessage(originalMessageEnvelope)
                    elif self.messageTracker.isInMessages(originalMessageEnvelope.MessageID):
                        responseEnvelope = originalMessageEnvelope.toResponse(originalMessageEnvelope.Message)
                        responseEnvelope.Message["state"] = States.REGISTERED
                        if "devices" in message:
                            responseEnvelope.Message["devices"] = envelope.Message["devices"]
                        self.client.forwardMessage(responseEnvelope)
                    elif type(originalMessageEnvelope) == DisableNode:
                        self.client.forwardMessage(originalMessageEnvelope)

            elif "error" in message:
                self.log.error(message["error"])
            else:
                self.log.error("Unsupported stop node acknowlegdment for %s", message)

        else:
            self.log.error("Received stop node acknowlegdment from '%s' but current role is '%s'",
                           envelope.From, self.clusterInfo.role)

    def handleSystemManagerUpdate(self, message, envelope):
        """
        Handle :class:`~c4.system.messages.SystemManagerUpdate` messages

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        self.log.debug("Received an system manager update message")
        self.clusterInfo.refresh()
        self.log.debug(self.clusterInfo.aliases)

    def handleUndeployDeviceManager(self, message, envelope):
        """
        Handle :class:`~c4.system.messages.UndeployDeviceManager` messages.
        A message from the rest server to the active system manager to delete the egg.  The system manager
        will also tell other system managers to delete the egg.
        Or a message from the active system manager to the other system managers to delete the egg.

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        self.log.debug("Undeploy device manager request from %s", envelope.From)
        self._handleUndeployEgg(message, envelope)

    def handleUnregisterNode(self, message, envelope):
        """
        Handle :class:`~c4.system.messages.Unregister` messages

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        self.log.debug("Received an unregister message from the active system manager")
        if self.clusterInfo.state == States.REGISTERED:
            self.clusterInfo.state = States.DEPLOYED
            self.client.forwardMessage(envelope.toResponse(message))
            if self.clusterInfo.role != Roles.ACTIVE:
                # FIXME: determine what to do here since we are not stopping the actual process
                pass

    def handleUnregisterNodeResponse(self, message, envelope):
        """
        Handle :class:`~c4.system.messages.UnregisterNode` response

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        original_message_id = self.messageTracker.removeRelatedMessage(envelope.RelatesTo)

        # This envelope should only ever be related to an UnregisterNodeRequest, so we don't do anything with that message
        if not self.messageTracker.hasMoreRelatedMessages(original_message_id):
            original_message = self.messageTracker.remove(original_message_id)
        self.log.debug("Unregistering: {0}".format(envelope.From))

        # FIXME: The rest server is not receiving the response when the active system manager is unregistered
        configuration = Backend().configuration
        configuration.removeNode(envelope.From)
        if envelope.From == self.node:
            # FIXME: determine what to do here since we are not stopping the actual process
            pass

    @property
    def node(self):
        """
        Node name

        :returns: str
        """
        return self.clusterInfo.node

    @classmethod
    def parseFrom(cls, from_str):
        """
        Parses the From attribute of a message in the form node/componentN

        If the component is nested, then it will remain nested and be returned
        as a single item.

        :returns: node_name and component_name as a tuple
        """
        parts = str(from_str).split("/")
        # if no slash, then assume no component
        if len(parts) == 1:
            node_name = parts[0]
            component_name = ""
        else:
            node_name = parts[0]
            component_name = "/".join(parts[1:])
        return (node_name, component_name)

    def routeMessage(self, envelopeString, envelope):
        """
        Route message packaged in an WS-Addressing like envelope accordingly

        :param envelopeString: envelope JSON string
        :type envelopeString: str
        :param envelope: envelope
        :type envelope: :class:`~c4.messaging.Envelope`
        :returns: response
        """
        if self.clusterInfo.state == States.RUNNING or envelope.Action in NOT_RUNNING_ACTIONS:
            return callMessageHandler(self, envelope)
        else:
            error = "message with action '{action}' will not be handled because it is not allowed when the node is not in 'RUNNING' state, currently '{state}'".format(
                action=envelope.Action,
                state=self.clusterInfo.state.name)
            self.log.error(error)
            return {"error": error}

    def startDevices(self, devices, deviceManagerImplementationMap, messageId):
        """
        Given a devices dict with key equals the full dotted device name (without the the host)
        and a value of DeviceInfo (see configuration), start each device

        :param devices: devices map
        :type devices: dict
        :param deviceManagerImplementationMap: devices type to implementation map
        :type deviceManagerImplementationMap: dict
        :param messageId: message id individual start device messages are related to
        :type messageId: str
        """
        localStartDeviceManagerMessages = []
        for fullDeviceName, deviceInfo in devices.iteritems():
            if deviceInfo.type in deviceManagerImplementationMap:
                try:
                    # create a new device manager instance
                    self.log.debug("starting device: '%s' on '%s'", fullDeviceName, self.node)
                    deviceManager = DeviceManager(self.clusterInfo,
                                                  fullDeviceName,
                                                  deviceManagerImplementationMap[deviceInfo.type],
                                                  deviceInfo.properties)
                    deviceManager.start()

                    # send start message to device manager
                    localStartDeviceManager = LocalStartDeviceManager(self.node, "{0}/{1}".format(self.node, fullDeviceName))
                    localStartDeviceManagerMessages.append(localStartDeviceManager)
                    self.messageTracker.addRelatedMessage(messageId, localStartDeviceManager.MessageID)
                    # TODO: make sure the device manager's message server is up and running, check for start timeout

                except MessagingException as e:

                    # store response message in stored message
                    content = {"devices" : {
                                    fullDeviceName : {
                                        "error": "Unable to start unknown device manager '{0}' of type '{1}' on '{2}': {3}".format(
                                            fullDeviceName, deviceInfo.type, self.node, e)}
                    }}
                    self.messageTracker.updateMessageContent(messageId, content)

            else:
                # store response message in stored message
                content = {"devices" : {
                                fullDeviceName : {
                                    "error": "Unable to start unknown device manager '{0}' of type '{1}' on '{2}'".format(
                                        fullDeviceName, deviceInfo.type, self.node)}
                }}
                self.messageTracker.updateMessageContent(messageId, content)

        for message in localStartDeviceManagerMessages:
            self.client.forwardMessage(message)

    def validateNodeListAndTransformWildcards(self, node_list):
        """
        Throws exception on invalid node in node list.
        """
        # * means all nodes
        if "*" in node_list:
            return self.clusterInfo.nodeNames
        for node_name in node_list:
            # make sure node exists
            if node_name not in self.clusterInfo.nodeNames:
                raise Exception("Invalid node: %s" % node_name)
        return node_list

class SystemManagerStatus(JSONSerializable):
    """
    System manager status which can be extended to include additional details

    """
    def __init__(self):
        super(SystemManagerStatus, self).__init__()
        self.timestamp = Datetime.utcnow()

def main():
    """
    Main function of the system manager
    """
    currentNodeName = platformSpec.node().split(".")[0]

    # parse command line arguments
    parser = argparse.ArgumentParser(description="C4 system manager")

    parentParser = argparse.ArgumentParser(add_help=False)
    parentParser.add_argument("-b", "--backend", action="store", default=os.path.join(DEFAULT_CONFIGURATION_PATH, "backend.json"), help="Backend configuration file")
    parentParser.add_argument("-i", "--pid-file", action="store", default=os.path.join(DEFAULT_CONFIGURATION_PATH, "c4.pid"), help="PID file")
    parentParser.add_argument("-l", "--logging-config", action="store", default=pkg_resources.resource_filename("c4.data", "config/c4_logging_config.json"), help="Backend configuration file")
    parentParser.add_argument("-n", "--node", action="store", default=currentNodeName, help="Node name for this system manager")
    parentParser.add_argument("-p", "--port", action="store", dest="node_port", type=int, default=5000, help="Port for this system manager")
    parentParser.add_argument("-v", "--verbose", action="store_true", default=0, help="Displays more log information")

    commandParser = parser.add_subparsers(dest="command")

    runParser = commandParser.add_parser(
        "run",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        help="Start this system manager as an active node and continue to run in the foreground",
        parents=[parentParser]
    )
    runParser.add_argument("-c", "--config", action="store", help="Configuration file name")
    runParser.add_argument("-f", "--force", action="store_true", help="Loads from the configuration file, regardless of whether the configuration tables are empty or not.")

    joinParser = commandParser.add_parser(
        "join",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        help="Join an existing cluster of system managers by starting this system manager as a thin node that connects to the active system manager",
        parents=[parentParser]
    )
    joinParser.add_argument("address", action="store", help="Address of the active system manager")
    args = parser.parse_args()

    # Setup logging
    with open(args.logging_config, 'r') as logConfigFile:
        configDict = json.load(logConfigFile)
        logging.config.dictConfig(configDict)

    if args.verbose:
        for handler in logging.root.handlers:
            if handler.get_name() == "file":
                handler.setLevel(logging.DEBUG)
                break
    # check for backend
    exampleProperties = {
        "path.database": "/dev/shm",
        "path.backup": "/tmp"
    }
    exampleInfo = BackendInfo("c4.backends.sharedSQLite.SharedSqliteDBBackend",
                              properties=exampleProperties)
    example = exampleInfo.toJSON(includeClassInfo=True, pretty=True)

    if not os.path.exists(args.backend):
        log.error("could not find backend configuration file at '%s', e.g.:\n%s", args.backend, example)
        return 1
    try:
        backendInfo = BackendInfo.fromJSONFile(args.backend)

        # get class info
        info = backendInfo.backend.split(".")
        className = info.pop()
        moduleName = ".".join(info)

        # load class from module
        log.info("loading backend implementation '%s' from module '%s'", className, moduleName)
        module = __import__(moduleName, fromlist=[className])
        clazz = getattr(module, className)

        # create instance based off constructor and set the backend
        backendImplementation = clazz(backendInfo)
        backend = Backend(implementation=backendImplementation)

    except Exception as e:
        log.error("could not load backend configuration file from '%s' because '%s', e.g.:\n%s", args.backend, e, example)
        log.exception(e)
        return 1

    if args.command == "join":

        # if active system manager address is supplied then we start as a thin system manager
        log.info("System manager information provided. Continuing as a thin node")
        if re.search(r"tcp://(?P<ip>(\d+\.\d+\.\d+\.\d+)):(?P<port>\d+)", args.address):
            clusterInfo = backend.ClusterInfo(args.node,
                                              "tcp://{0}:{1}".format(socket.gethostbyname(socket.gethostname()), args.node_port),
                                              args.address,
                                              Roles.THIN,
                                              States.DEPLOYED)
        else:
            log.error("'%s' is an invalid address for a system manager, expected 'tcp://ip:port'", args.address)
            return 1

    elif args.command == "run":
        log.info("*****************************************************************************************")
        log.info("*****************************************************************************************")
        log.info("*********                  STARTING SYSTEM MANAGER                          *************")
        log.info("*****************************************************************************************")
        log.info("*****************************************************************************************")

        try:
            configuration = Backend().configuration
            backend = Backend()
            backend.deviceHistory.remove(node=args.node)
            backend.nodeHistory.remove(node=args.node)
            if not configuration.getNodeNames() or args.force:
                # if database is empty, then load from config file
                if not args.config:
                    log.error("configuration database is empty but no config file is specified")
                    return 1
                if not args.force:
                    log.warn("Configuration database is empty. Loading from config file '%s'", args.config)
                configInfo = ConfigurationInfo.fromJSONFile(args.config)
                configuration.clear()
                configuration.loadFromInfo(configInfo)
            else:
                # else database not empty


                # set all devices to REGISTERED unless their states are MAINTENTANCE or UNDEPLOYED
                configuration.resetDeviceStates()

            # validate node (hostname) matches configuration
            if args.node not in configuration.getNodeNames():
                log.error("Current node name '%s' not found in configuration", args.node)
                return 1

            # make sure devices match assigned role
            else:
                log.info("Check devices based on assigned role...")
                # remove all devices from node, and add devices based on role
                nodeInfo = configuration.getNode(node=args.node, includeDevices=True)
                nodeRole = nodeInfo.role
                log.info("Node '%s' has role: '%s'", args.node, nodeRole.name)
                roleInfo = configuration.getRoleInfo(role=nodeRole)
                if not roleInfo:
                    log.error("The nodes assigned role '%s' not found in configuration", nodeRole)

                else:
                    # remove any devices attached to node that are not part of the role
                    for device in nodeInfo.devices.values():
                        log.info("Checking device '%s' ...", device.name)
                        if device not in roleInfo.devices.values():
                            log.info("Removing device: %s from node %s", device.name, args.node)
                            configuration.removeDevice(node=args.node, fullDeviceName=device.name)

                    nodeInfo = configuration.getNode(node=args.node, includeDevices=True)
                    # add any devices to the node that were part of the role but missing from node
                    for device in roleInfo.devices.values():
                        log.info("Found device in role '%s' ...", device.name)
                        if device not in nodeInfo.devices.values():
                            log.info("Adding device: %s to node %s", device.name, args.node)
                            configuration.addDevice(node=args.node, fullDeviceName=device.name, device=device)

            clusterInfo = backend.ClusterInfo(args.node,
                                              configuration.getAddress(args.node),
                                              configuration.getAddress("system-manager"),
                                              configuration.getNode(args.node, includeDevices=False).role,
                                              States.DEPLOYED)

        except Exception as e:
            log.error("Error in '%s'", args.config)
            log.exception(e)
            return 1

    # Check for pidfile. If it exists and the process is still running, exit.
    # If not, create it and continue starting up.
    if os.path.exists(args.pid_file):
        with open(args.pid_file, "r") as pidfile:
            pid = int(pidfile.read())
            try:
                os.kill(pid, 0)
                log.warning("System Manager is already running with process id %s.", pid)
                return 1
            except OSError as err:
                # EPERM indicates process is running but we didn't have permission to run kill against it
                if err.errno == errno.EPERM:
                    log.warning("System Manager is already running with process id %s.", pid)
                    return 1
                pass

    with open(args.pid_file, "w") as pidfile:
        pidfile.write(str(os.getpid()))

    # Clean up any remaining .ipc files
    dirFiles = os.listdir(DEFAULT_IPC_PATH)
    for dirFile in dirFiles:
        if dirFile.endswith(".ipc"):
            os.remove(os.path.join(DEFAULT_IPC_PATH, dirFile))

    try:
        systemManager = SystemManager(clusterInfo)
    except MessagingException as e:
        log.error("Could not set up system manager '%s'", e)
        return 1

    stopFlag = multiprocessing.Event()

    # adjust interrupt signal handler such that we can shut down cleanly
    # this means ignoring all sigints except when in the system manager
    originalInterruptHandler = signal.getsignal(signal.SIGINT)
    def interruptHandler(signum, frame):
        if "systemManager" in frame.f_locals:
            # we are a running system manager
            log.info("system manager received interrupt signal")
            # restore original interrupt signal handler
            signal.signal(signal.SIGINT, originalInterruptHandler)
            # stop system manager
            stopFlag.set()
        elif isinstance(frame.f_locals.get("self", None), SystemManager):
            # we are still starting the system manager
            log.info("system manager received interrupt signal during start")
            # restore original interrupt signal handler
            signal.signal(signal.SIGINT, originalInterruptHandler)
            # stop system manager
            stopFlag.set()
            # propagate interrupt back up to start method
            raise KeyboardInterrupt
        else:
            # ignore interrupt signal
            signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGINT, interruptHandler)

    role = configuration.getRole(args.node)
    started = False
    if role != Roles.DISABLED:
        started = systemManager.start()

    tickCounter = 0
    if started:
        # note that we need to loop here because wait will not allow us to catch the interrupt
        while not stopFlag.is_set():
            # Poll the role every 30 seconds and auto-shutdown if role moves to inactive
            if tickCounter >= 30:
                tickCounter = 0
                role = configuration.getRole(args.node)
                if role == Roles.DISABLED:
                    log.info("Detected %s went inactive - Shutting Down!", args.node)
                    stopFlag.set()
            time.sleep(1)
            tickCounter += 1
    systemManager.stop()
    os.remove(args.pid_file)
    return 0

if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s [%(levelname)s] <%(processName)s:%(process)s> [%(name)s(%(filename)s:%(lineno)d)] - %(message)s', level=logging.INFO)
    sys.exit(main())
