#!/usr/bin/env python

import argparse
import datetime
import imp
import logging
from multiprocessing.managers import SyncManager
import socket
import sys
import time

import pkg_resources

from c4.messaging import (MessageTracker, MessagingException, PeerRouter,
                          callMessageHandler, sendMessage, sendMessageToRouter)
from c4.system import db, egg
from c4.system.configuration import (DBClusterInfo, Configuration, NodeInfo,
                                     States, Roles, ConfigurationInfo)
from c4.system.deviceManager import DeviceManager
import c4.system.devices
from c4.system.messages import (StartDeviceManagers, LocalStartDeviceManager, LocalStopDeviceManager,
                                RegistrationNotification, DisableNode,
                                StartNode, StopNode, LocalStopNode)
from c4.system.version import Version
from c4.utils.jsonutil import JSONSerializable
from c4.utils.logutil import ClassLogger
from c4.utils.util import (Timer, disableInterruptSignal,
                           getFullModuleName, getModuleClasses)

# TODO: check what this is used for
import platform as platformSpec


log = logging.getLogger(__name__)

@ClassLogger
class SystemManager(PeerRouter):
    """
    System Manager

    :param node: node name
    :type node: str
    :param clusterInfo: cluster information
    :type clusterInfo: :class:`~c4.system.configuration.DBClusterInfo`
    :param name: name
    :type name: str
    :raises MessagingException: if either external or internal system manager address is already in use
    """
    def __init__(self, node, clusterInfo, name="SM"):
        super(SystemManager, self).__init__(node, clusterInfo, "ipc://{}.ipc".format(node), name=name)

        # FIXME: check which ones need/may to be moved to implementation
        # FIXME: which ones need to stay shared?
        # enable shared objects
        self.manager = SyncManager()
        self.manager.start(disableInterruptSignal)
        self.sharedLock = self.manager.Lock()
        self.sharedDict = self.manager.dict()
        self.messageTracker = MessageTracker()

        # set up system manager implementation
        self.implementation = SystemManagerImplementation(node, self.clusterInfo, self.sharedLock, self.sharedDict, self.messageTracker)
        # connect stop flag to allow implementation to stop device manager
        self.implementation.stopFlag = self.stopFlag

        self.addHandler(self.implementation.routeMessage)

    def run(self):
        """
        Run the system manager
        """
        self.implementation.start()
        super(SystemManager, self).run()
        self.implementation.stop()

    def stop(self, wait=False):
        """
        Stop the system manager

        :returns: :class:`~c4.system.manager.SystemManager`
        """
        self.log.debug("Stopping '%s'", self.name)

        # stop local device managers, this is required, otherwise device manager processes won't stop
        if self.clusterInfo.state == States.RUNNING:
            sendMessageToRouter("ipc://{0}.ipc".format(self.implementation.node), StopNode(self.implementation.node))

            # give device managers and sub processes time to stop
            while self.clusterInfo.state == States.RUNNING:
                self.log.debug("Waiting for system manager to stop, current state: %s", self.clusterInfo.state.name)
                time.sleep(1)

        self.stopFlag.set()
        if wait:
            while self.is_alive():
                time.sleep(1)
            self.log.debug("Stopped '%s'", self.name)
        return self

@ClassLogger
class SystemManagerImplementation(object):
    """
    System manager implementation which provides the handlers for messages.
    This implementation will be provided to each worker in the process pool.

    :param node: node name
    :type node: str
    :param sharedLock: shared lock
    :type sharedLock: :class:`~multiprocessing.managers.Lock`
    :param sharedDict: shared dictionary
    :type sharedDict: :class:`~multiprocessing.managers.dict`
    :param messageTracker: message tracker
    :type messageTracker: :class:`~c4.messaging.MessageTracker`
    """
    def __init__(self, node, clusterInfo, sharedLock, sharedDict, messageTracker):
        super(SystemManagerImplementation, self).__init__()
        self.node = node
        self.clusterInfo = clusterInfo
        self.sharedLock = sharedLock
        self.sharedDict = sharedDict
        self.sharedLock.acquire()
        if "deviceManagers" not in self.sharedDict:
            self.sharedDict["deviceManagers"] = set()
        self.sharedLock.release()
        self.messageTracker = messageTracker
        self.stopFlag = None

        self.processes ={}

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
        # e.g. "c4.system.devices.mydm.MyDM"
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
        # e.g. "c4.system.devices.mydm"
        egg_mod = ".".join(temp_mod_list)
        # e.g. "/c4/system/devices/mydm.py"
        egg_path = "/" + "/".join(temp_mod_list) + ".py"
        # import this new module and
        # reload parent module, so that when we query for the list
        # of modules, this newly installed module will show up
        new_mod = imp.load_source(egg_mod, dist.location + egg_path)
        if egg_type.startswith("c4.system.devices"):
            reload(c4.system.devices)
        elif egg_type.startswith("c4.system.policies"):
            reload(c4.system.policies)
        elif egg_type.startswith("c4.system.actions"):
            reload(c4.system.actions)
        elif egg_type.startswith("c4.system.events"):
            reload(c4.system.events)
        else:
            log.error("Unsupported egg type: %s", egg_type)

        # save version on active master only
        if self.clusterInfo.role == Roles.ACTIVE:
            version = getattr(new_mod, "__version__", "unknown")
            Version.saveVersion(self.node, "", egg_type, version)

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
        log.info("Undeploy device manager request from %s", envelope.From)
        # if from rest server to the active system manager
        (node_name, component_name) = self.parseFrom(envelope.From)
        if component_name == "rest":
            egg.sendUndeployToOtherSystemManagers(self.node, self.clusterInfo.nodeNames,
                                                self.clusterInfo.getNodeAddress("system-manager"),
                                                message["type"])
        # all sysmgrs will now uninstall the egg
        if not egg.undeployEgg(message["type"]):
            return

        # delete version on active master only
        if self.clusterInfo.role == Roles.ACTIVE:
            Version.deleteVersion(self.node, "", message["type"])

    def areMyDeviceManagersStopped(self,devices):
        """
        Check if devices are running
        """
        for deviceInfo in devices.values():
            if deviceInfo.state == States.RUNNING:
                return False
            if not self.areMyDeviceManagersStopped(deviceInfo.devices):
                return False
        return True

    def executeNodeRegistration(self):
        """
        Execute node registration
        """
        if self.clusterInfo.state == States.DEPLOYED or self.clusterInfo.state == States.REGISTERING:
            self.clusterInfo.state = States.REGISTERING
            env =  RegistrationNotification(self.node)
            node_info = NodeInfo(self.node, self.clusterInfo.getNodeAddress(self.node),
                                 self.clusterInfo.role, self.clusterInfo.state)
            # TODO: have it automatically serialize
            env.Message['NodeInfo'] = node_info.toJSONSerializable(includeClassInfo=True)
            env.Message['version'] = c4.system.__version__
            sendMessageToRouter("ipc://{0}.ipc".format(self.node), env)

    def getDeviceManagerImplementations(self):
        # retrieve available device manager implementations
        deviceManagerImplementations = sorted(getModuleClasses(c4.system.devices, c4.system.deviceManager.DeviceManagerImplementation))
        if c4.system.deviceManager.DeviceManagerImplementation in deviceManagerImplementations:
            deviceManagerImplementations.remove(c4.system.deviceManager.DeviceManagerImplementation)
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
        log.info("Deploy device manager request from %s", envelope.From)
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
        log.info("Disable device manager request from %s for %s",
                    envelope.From, message)

        if "devices" not in message:
            log.error("Unable to disable devices because no devices were supplied.")
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
        log.info("Disable device manager response from %s for %s",
                    envelope.From, message)

        # validate role
        if self.clusterInfo.role != Roles.ACTIVE:
            log.error("Received disable device manager response from '%s' but current role is '%s', should be '%s'",
                           envelope.From, self.clusterInfo.role, Roles.ACTIVE)
            return

        (node_name, from_device_name) = self.parseFrom(envelope.From)

        if "devices" not in message:
            log.error("Received disable device manager response, but no devices were provided.")
            return

        for device_name in message["devices"]:
            log.info("Changing %s/%s state from REGISTERED to MAINTENANCE.", node_name, device_name)
            Configuration().changeState(node_name, device_name, States.MAINTENANCE)

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
        log.info("Disable node request from %s for %s",
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
        log.info("Disable node response from %s for %s",
                    envelope.From, message)

        # validate role
        if self.clusterInfo.role != Roles.ACTIVE:
            log.error("Received disable node response from '%s' but current role is '%s', should be '%s'",
                           envelope.From, self.clusterInfo.role, Roles.ACTIVE)
            return

        (node_name, from_device_name) = self.parseFrom(envelope.From)

        # Received request to change the state of a node
        Configuration().changeState(node_name, None, States.MAINTENANCE)

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
        log.info("Enable device manager request from %s for %s",
                    envelope.From, message)

        if "devices" not in message:
            log.error("Unable to enable devices because no devices were supplied.")
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
        log.info("Enable device manager response from %s for %s",
                    envelope.From, message)

        # validate role
        if self.clusterInfo.role != Roles.ACTIVE:
            log.error("Received enable device manager response from '%s' but current role is '%s', should be '%s'",
                           envelope.From, self.clusterInfo.role, Roles.ACTIVE)
            return

        if "devices" not in message:
            log.error("Unable to enable devices because no devices were supplied.")
            return

        (node_name, from_device_name) = self.parseFrom(envelope.From)

        configuration = Configuration()
        # change device state from MAINTENANCE to REGISTERED
        for device_name in message["devices"]:
            log.info("Changing %s/%s state from MAINTENANCE to REGISTERED.", node_name, device_name)
            configuration.changeState(node_name, device_name, States.REGISTERED)

        if self.messageTracker.isARelatedMessage(envelope.RelatesTo):
            original_message_id = self.messageTracker.removeRelatedMessage(envelope.RelatesTo)
            if not self.messageTracker.hasMoreRelatedMessages(original_message_id):
                original_message = self.messageTracker.remove(original_message_id)
                # the devices are now all registered, so now start them
                # the original_message came from handleStartdevicemanager
                if type(original_message) == StartDeviceManagers:
                    log.debug("Sending original start message: %s to %s", original_message.Message, original_message.To)
                    sendMessage(self.clusterInfo.getNodeAddress(envelope.From), original_message)

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
        log.info("Enable node request from %s for %s",
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
        log.info("Enable node response from %s for %s",
                    envelope.From, message)

        # validate role
        if self.clusterInfo.role != Roles.ACTIVE:
            log.error("Received enable node response from '%s' but current role is '%s', should be '%s'",
                           envelope.From, self.clusterInfo.role, Roles.ACTIVE)
            return

        (node_name, from_device_name) = self.parseFrom(envelope.From)

        # Received request to change the state of a node
        Configuration().changeState(node_name, None, States.REGISTERED)

    def handleLocalStartDeviceManagerResponse(self, message, envelope):
        """
        Handle :class:`~c4.system.messages.LocalStartDeviceManagerResponse` response messages

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        log.info("Received start acknowledgement from '%s'", envelope.From)
        (node, device_name) = self.parseFrom(envelope.From)

        # remove response from the related messages list
        storedMessageId = self.messageTracker.removeRelatedMessage(envelope.RelatesTo)

        # store response message in stored message
        self.messageTracker.updateMessageContent(storedMessageId, {"devices" : {device_name : message}})

        # check if there are more related messages
        if not self.messageTracker.hasMoreRelatedMessages(storedMessageId):
            # last related message, so pick up the stored message and send it out
            storedMessageEnvelope = self.messageTracker.remove(storedMessageId)
            if storedMessageEnvelope is not None:
                sendMessageToRouter("ipc://{0}.ipc".format(self.node), storedMessageEnvelope)

    def handleLocalStopDeviceManagerResponse(self, message, envelope):
        """
        Handle :class:`~c4.system.messages.LocalStopDeviceManager` response messages

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        log.info("Received stop acknowledgement from '%s'", envelope.From)
        (node, device_name) = self.parseFrom(envelope.From)

        # remove device manager from list
        self.sharedLock.acquire()
        deviceManagers = self.sharedDict["deviceManagers"]
        deviceManagers.remove(device_name)
        self.sharedDict["deviceManagers"] = deviceManagers
        self.sharedLock.release()

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
                sendMessageToRouter("ipc://{0}.ipc".format(self.node),
                                    originalMessageEnvelope.toResponse(originalMessageEnvelope.Message))

    def handleLocalStopNode(self, message, envelope):
        """
        Handle :class:`~c4.system.messages.LocalStopNode` messages

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        # send stop to all device managers if we have any running
        if self.sharedDict['deviceManagers']:
            self.messageTracker.add(envelope)
            deviceManagers = self.sharedDict['deviceManagers']
            for deviceManager in deviceManagers:

                stopEnvelope = LocalStopDeviceManager(self.node, "{0}/{1}".format(self.node, deviceManager))
                self.messageTracker.addRelatedMessage(envelope.MessageID, stopEnvelope.MessageID)
                sendMessageToRouter("ipc://{0}.ipc".format(self.node), stopEnvelope)

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

                sendMessageToRouter("ipc://{0}.ipc".format(self.node), responseEnvelope)

        else:
            log.debug("%s does not have a Stop message originating from %s", message, self.node)

    def handleLocalUnregisterDeviceManagers(self, message, envelope):
        """
        Handle :class:`~c4.system.messages.LocalUnregisterDeviceManagers` messages

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        log.debug("Unregistering '%s' on '%s'", message['devices'], message['node'])

        node = message['node']
        configuration = Configuration()
        for d in message['devices']:
            try:
                configuration.removeDevice(node, d)
            except Exception as e:
                log.error("Could not remove '%s' from '%s' :%s", d, node, e)
        sendMessageToRouter("ipc://{0}.ipc".format(self.node), envelope.toResponse(message))

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
        log.info("Register device manager request (local) from %s for %s on %s with type %s",
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
        log.info("Register device manager response from %s for %s on %s with type %s",
                     envelope.From, message["devices"], envelope.From, message["type"])

        # validate role
        if self.clusterInfo.role != Roles.ACTIVE:
            log.error("Received register device manager response from '%s' but current role is '%s', should be '%s'",
                           envelope.From, self.clusterInfo.role, Roles.ACTIVE)
            return

        configuration = Configuration()
        # add each given device name to node name
        for device_name in message["devices"]:
            try:
                # make sure device name is not already associated with each given node
                # handle hierarchical device name - if parent doesn't exist error
                node_name = envelope.From
                configuration.addDevice(node_name, device_name, message["type"])
                log.debug("Added device %s to node %s.", device_name, node_name)
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
        log.info("Registration message from %s", envelope.From)

        # Is node in configuration?
        configuration = Configuration()
        node = envelope.From
        if node not in configuration.getNodeNames():
            log.info("Adding %s", node)
            configuration.addNode(envelope.Message['NodeInfo'])
            log.info("%s added to the configuration",node)

        # change directly to starting state since we are going to start the node
        configuration.changeState(node, None, States.STARTING)

        # start node
        sendMessageToRouter("ipc://{0}.ipc".format(self.node), StartNode(node))

        # send registration response
        return {"state": States.REGISTERED}

    def handleRegistrationNotificationResponse(self):
        """
        Handle :class:`~c4.system.messages.RegisterNode` response messages
        """
        if self.clusterInfo.state == States.REGISTERING:
            self.clusterInfo.state = States.REGISTERED
            log.info("'%s' is now registered", self.node)
        elif self.clusterInfo.state == States.STARTING or self.clusterInfo.state == States.RUNNING:
            # we implicitly know that the active system manager must have registered because it sent
            # a start message
            pass
        else:
            # TODO: skip if we are already started/starting
            log.error("Received register node acknowlegdment but current state is '%s'", self.clusterInfo.state)

    def handleStartDeviceManagers(self, message, envelope):
        """
        Handle :class:`~c4.system.messages.StartDeviceManagers` messages

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        log.info("Received start device manager message %s", message)
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
                    sendMessageToRouter("ipc://{0}.ipc".format(self.node), storedMessageEnvelope)
        else:
            response["error"] = "Received start message but current role is '{0}'".format(self.clusterInfo.role)
            log.error(response["error"])
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
            log.debug("Received start acknowlegdment from '%s'", envelope.From)
            (node_name, component_name) = self.parseFrom(envelope.From)
            if "devices" in message:

                configuration = Configuration()
                # change state of the device manager in the configuration to running
                for name, info in message["devices"].items():
                    if "state" in info and isinstance(info["state"], States):
                        configuration.changeState(node_name, name, States.RUNNING)
                    elif "error" in info:
                        configuration.changeState(node_name, name, States.REGISTERED)
                        log.error(info["error"])
            else:
                log.error("Unsupported start acknowlegdment for %s", message)
        else:
            log.error("Received start acknowlegdment from '%s' but current role is '%s'",
                           envelope.From, self.clusterInfo.role)

    def handleStartNode(self):
        """
        Handle :class:`~c4.system.messages.StartNode` messages

        """
        log.info("Received start node message")
        response = {}

        if self.clusterInfo.state == States.REGISTERED or self.clusterInfo.state == States.REGISTERING:
            self.clusterInfo.state = States.RUNNING
            response["state"] = States.RUNNING

            # remove register timer
            registerTimer = self.processes.pop("registerTimer")
            registerTimer.terminate()
            registerTimer.join()

            # TODO: check and revise if necessary
            # get version for this node's DEPLOYED devices and system-manager
            # versionDict - key is device type and value is the version
            versionDict = {}
            deviceManagerImplementations = self.getDeviceManagerImplementations()
            for dmClass in deviceManagerImplementations:
                dmModule = sys.modules[dmClass.__module__]
                dmType = dmModule.__name__ + "." + dmClass.__name__
                versionDict[dmType] = getattr(dmModule, "__version__", "unknown")
            versionDict["c4.system.manager.SystemManager"] = c4.system.__version__
            response["version_dict"] = versionDict

        else:
            response["error"] = "Received start message but current state is '{0}'".format(self.clusterInfo.state)
            log.error(response["error"])
        return response

    def handleStartNodeResponse(self, message, envelope):
        """
        Handle :class:`~c4.system.messages.StartNodeResponse` response messages

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        configuration = Configuration()
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

        # for each device type, save version information by name
        # if no name currently exists, then name will be blank
        def saveVersionByType(deviceType, version, configurationInfo):
            def _saveVersionByType(parentPrefix, nodeOrDevice, deviceType, version):
                for deviceInfo in nodeOrDevice.devices.itervalues():
                    deviceName = parentPrefix + deviceInfo.name
                    if deviceInfo.type == deviceType:
                        Version.saveVersion(envelope.From, deviceName, deviceType, version)
                    # check hierarchy children
                    _saveVersionByType(deviceName + ".", deviceInfo, deviceType, version)

            # handle sysmgr version
            if deviceType == "c4.system.manager.SystemManager":
                Version.saveVersion(envelope.From, "system-manager", deviceType, version)
                return

            # handle device managers version

            # make sure there is at least one row in the version table
            # in case there is no name (i.e. device not in configuration) for the given type
            Version.saveVersion(envelope.From, "", deviceType, version)
            # save version for each device manager name, if any, for this node
            if envelope.From in configurationInfo.nodes:
                nodeInfo = configurationInfo.nodes[envelope.From]
                _saveVersionByType("", nodeInfo, deviceType, version)

        if self.clusterInfo.role == Roles.ACTIVE:
            log.debug("Received start node acknowlegdment from '%s'", envelope.From)
            if "state" in message and isinstance(message["state"], States):

                # change state of the node in the configuration to running
                configuration.changeState(envelope.From, None, States.RUNNING)

                # version
                configurationInfo = configuration.toInfo()
                # set version for all of the device names for each device type, if any
                # also sets version for sysmgr
                for deviceType, version in message["version_dict"].iteritems():
                    saveVersionByType(deviceType, version, configurationInfo)

                # start all devices that are in REGISTERED state for the node that just started
                startEnvelope = StartDeviceManagers(envelope.From)
                for node_name, a_node in configurationInfo.nodes.iteritems():
                    if envelope.From == node_name:
                        loadMessageWithRegisteredDevices(node_name, startEnvelope.Message, "", a_node)
                logging.debug("Sending message to %s to start all REGISTERED devices: %s", envelope.From, startEnvelope.Message)
                sendMessageToRouter("ipc://{0}.ipc".format(self.node), startEnvelope)

            elif "error" in message:
                log.error(message["error"])
            else:
                log.error("Unsupported start acknowlegdment for %s", message)
        else:
            log.error("Received start acknowlegdment from '%s' but current role is '%s'",
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
        log.debug("Received status request")
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
            try:
                statusJSON = message.toJSON(True)
                # TODO: we should think if showing the status message is necessary as it produces a lot of output
                # log.debug("Status from %s:\n%s", envelope.From, message.toJSON(pretty=True))
                log.debug("Received status from %s", envelope.From)

                if name:
                    configuration = Configuration()
                    senderType = configuration.getDevice(node, name).type
                else:
                    name = None
                    senderType = getFullModuleName(SystemManager) + ".SystemManager"

                # TODO: this kind of database functionality should be in a separate History class
                dbm = db.DBManager()
                dbm.write("begin")
                # t_sm_history grows
                # (until a limit is reached and a db trigger removes old rows)
                dbm.write("""
                    insert into t_sm_history (history_date, node, name, type, details)
                    values (?, ?, ?, ?, ?)""",
                    (message.timestamp, node, name, senderType, statusJSON))
                # t_sm_latest holds the latest status
                # the table never grows more than the total number of components
                # perform a provisional update and check if rows were affected
                updated = dbm.write("""
                    update t_sm_latest set details = ?
                    where node is ? and name is ?""",
                    (statusJSON, node, name))
                if updated < 1:
                    dbm.write("""
                        insert into t_sm_latest (node, name, type, details)
                        values (?, ?, ?, ?)""",
                        (node, name, senderType, statusJSON))
                dbm.write("commit")
                dbm.close()
            except Exception as e:
                log.error("%s needs to be a status message. Error:%s", message, e)

        else:
            log.error("Received status response from '%s' but current role is '%s'",
                           envelope.From, self.clusterInfo.role)

    def handleStopDeviceManagers(self, message, envelope):
        """
        Handle :class:`~c4.system.messages.StopDeviceManagers` messages

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        log.info("Received stop request for %s from %s", message, envelope.From)

        self.messageTracker.add(envelope)
        for deviceManager in message["devices"]:

            log.debug("Sending stop to %s/%s", self.node, deviceManager)
            stopEnvelope = LocalStopDeviceManager(self.node, "{0}/{1}".format(self.node, deviceManager))
            self.messageTracker.addRelatedMessage(envelope.MessageID, stopEnvelope.MessageID)
            sendMessageToRouter("ipc://{0}.ipc".format(self.node), stopEnvelope)

    def handleStopDeviceManagersResponse(self, message, envelope):
        """
        Handle :class:`~c4.system.messages.StopDeviceManager` response messages

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        log.info("Received stop acknowledgement from '%s'", envelope.From)
        (node, device_name) = self.parseFrom(envelope.From)
        configuration = Configuration()
        for device in message["devices"]:
            log.info("Changing %s/%s state from RUNNING to REGISTERED.", node, device)
            configuration.changeState(node, device, States.REGISTERED)

        original_message_id = self.messageTracker.removeRelatedMessage(envelope.RelatesTo)

        if not self.messageTracker.hasMoreRelatedMessages(original_message_id):
            original_message = self.messageTracker.remove(original_message_id)
            if original_message is not None:
                sendMessageToRouter("ipc://{0}.ipc".format(self.node), original_message)

    def handleStopNode(self, message, envelope):
        """
        Handle :class:`~c4.system.messages.StopNode` messages

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        log.info("Received stop node message")

        # send stop to all device managers if we have any running
        if self.sharedDict['deviceManagers']:

            localStopNodeMessage = LocalStopNode(self.node)
            self.messageTracker.add(envelope)
            self.messageTracker.addRelatedMessage(envelope.MessageID, localStopNodeMessage.MessageID)
            sendMessageToRouter("ipc://{0}.ipc".format(self.node), localStopNodeMessage)

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

            log.info("Received stop node acknowlegdment from '%s'", envelope.From)

            if "state" in message and isinstance(message["state"], States):

                configuration = Configuration()
                allDevicesStopped = True
                for deviceName, deviceInfo in message.get("devices", {}).items():

                    if "state" in deviceInfo and isinstance(deviceInfo["state"], States):
                        configuration.changeState(envelope.From, deviceName, States.REGISTERED)
                    elif "error" in deviceInfo:
                        log.error(deviceInfo["error"])
                        allDevicesStopped = False
                    else:
                        log.error("Unsupported stop node acknowlegdment for %s: %s", deviceName, deviceInfo)

                if allDevicesStopped:
                    configuration.changeState(envelope.From, None, States.REGISTERED)

            originalMessageId = self.messageTracker.removeRelatedMessage(envelope.RelatesTo)
            if not self.messageTracker.hasMoreRelatedMessages(originalMessageId):
                # last related message, so pick up the original message and respond to it
                originalMessageEnvelope = self.messageTracker.remove(originalMessageId)
                if originalMessageEnvelope is not None:
                    if self.messageTracker.isARelatedMessage(originalMessageEnvelope.MessageID):
                        sendMessageToRouter("ipc://{0}.ipc".format(self.node), originalMessageEnvelope)
                    elif self.messageTracker.isInMessages(originalMessageEnvelope.MessageID):
                        responseEnvelope = originalMessageEnvelope.toResponse(originalMessageEnvelope.Message)
                        responseEnvelope.Message["state"] = States.REGISTERED
                        if "devices" in message:
                            responseEnvelope.Message["devices"] = envelope.Message["devices"]
                        sendMessageToRouter("ipc://{0}.ipc".format(self.node), responseEnvelope)
                    elif type(originalMessageEnvelope) == DisableNode:
                        sendMessage(self.clusterInfo.getNodeAddress(originalMessageEnvelope.From), originalMessageEnvelope)

            elif "error" in message:
                log.error(message["error"])
            else:
                log.error("Unsupported stop node acknowlegdment for %s", message)

        else:
            log.error("Received stop node acknowlegdment from '%s' but current role is '%s'",
                           envelope.From, self.clusterInfo.role)

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
        log.info("Undeploy device manager request from %s", envelope.From)
        self._handleUndeployEgg(message, envelope)

    def handleUnregisterNode(self, message, envelope):
        """
        Handle :class:`~c4.system.messages.Unregister` messages

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        log.debug("Received an unregister message from the active system manager")
        if self.clusterInfo.state == States.REGISTERED:
            self.clusterInfo.state = States.DEPLOYED
            sendMessageToRouter("ipc://{0}.ipc".format(self.node), envelope.toResponse(message))
            if self.clusterInfo.role != Roles.ACTIVE:
                self.stopFlag.set()

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
        log.debug("Unregistering: {0}".format(envelope.From))

        # FIXME: The rest server is not receiving the response when the active system manager is unregistered
        configuration = Configuration()
        configuration.removeNode(envelope.From)
        if envelope.From == self.node:
            self.stopFlag.set()

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
        return callMessageHandler(self, envelope)

    def start(self):
        """
        Start system manager implementation
        """
        # setup and start register timer
        self.processes["registerTimer"] = Timer("{}-RegisterTimer".format(self.node),
                                                self.executeNodeRegistration, 1, -1, 1)
        self.processes["registerTimer"].start()

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
        for fullDeviceName, deviceInfo in devices.iteritems():
            if deviceInfo.type in deviceManagerImplementationMap:
                try:
                    # create a new device manager instance
                    deviceManager = DeviceManager(self.node,
                                                  fullDeviceName,
                                                  deviceManagerImplementationMap[deviceInfo.type],
                                                  deviceInfo.properties)
                    deviceManager.start()

                    # add device manager to the local device manager list
                    self.sharedLock.acquire()
                    deviceManagers = self.sharedDict["deviceManagers"]
                    deviceManagers.add(fullDeviceName)
                    self.sharedDict["deviceManagers"] = deviceManagers
                    self.sharedLock.release()

                    # send start message to device manager
                    localStartDeviceManager = LocalStartDeviceManager(self.node, "{0}/{1}".format(self.node, fullDeviceName))
                    self.messageTracker.addRelatedMessage(messageId, localStartDeviceManager.MessageID)
                    # TODO: make sure the device manager's message server is up and running
                    sendMessageToRouter(deviceManager.downstreamAddress, localStartDeviceManager)
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

    def stop(self):
        """
        Stop system manager implementation
        """
        for name, process in self.processes.items():
            self.log.debug("stopping %s", name)
            process.terminate()
            process.join()
            self.log.info("stopped %s", name)

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
        utcTime = datetime.datetime.utcnow()
        self.timestamp = "{:%Y-%m-%d %H:%M:%S}.{:03d}".format(utcTime, utcTime.microsecond // 1000)

def main():
    """
    Main function of the system manager
    """
    logging.basicConfig(format='%(asctime)s [%(levelname)s] <%(processName)s:%(process)s> [%(name)s(%(filename)s:%(lineno)d)] - %(message)s', level=logging.INFO)

    # parse command line arguments
    parser = argparse.ArgumentParser(description="Dynamite system manager")
    parser.add_argument("-s", "--sm-addr", action="store", dest="sm_addr", help="Address of the active system manager.  The system manager will start up as a thin node.")
    parser.add_argument("-v", "--verbose", action="count", help="Displays more log information")
    parser.add_argument("-n", "--node", action="store", help="Node name for this system manager.  Must match a node in the bill of materials (i.e. bom.json).")
    parser.add_argument("-p", "--port", action="store", help="The port to use for routing of messages")
    parser.add_argument("-b", "--bom", action="store", help="BOM file name")
    parser.add_argument("-f", "--force", action="store_true", help="Loads from the configuration file, regardless of whether the configuration tables are empty or not.")
    parser.set_defaults(node=platformSpec.node(),
                        port="5000",
                        bom="bom.json")

    args = parser.parse_args()

    # TODO: we need to have more distinguished levels for this
    if args.verbose >= 1:
        logging.root.setLevel(logging.DEBUG)
        log.setLevel(logging.DEBUG)

    # if active system manager address is supplied,
    # then we are a thin system manager
    if args.sm_addr:
        log.info("System manager information provided. Continuing as a thin node")
        clusterInfo = DBClusterInfo(args.node,
                        "tcp://{0}:{1}".format(socket.gethostbyname(socket.gethostname()), args.port),
                        args.sm_addr, Roles.THIN)
    # load configuration from the database
    # if empty, then load from bom.json
    elif args.bom:
        try:
            configuration = Configuration()
            # if database is empty, then load from bom.json
            if len(configuration.getNodeNames()) == 0 or args.force:
                if not args.force:
                    log.debug("Configuration database is empty.  Loading from config file %s", args.bom)
                configuration.clear()
                configuration.loadFromInfo(ConfigurationInfo.fromJSONFile(args.bom))
            # else database not empty
            # set all devices to REGISTERED unless their states are MAINTENTANCE or UNDEPLOYED
            else:
                configuration.resetDeviceStates()

            # validate node (hostname) matches configuration
            if args.node not in configuration.getNodeNames():
                log.error("Current node name (%s) not found in configuration" % args.node)
                return 1

            clusterInfo = DBClusterInfo(args.node,
                                        configuration.getAddress(args.node),
                                        configuration.getAddress("system-manager"),
                                        configuration.getNode(args.node, includeDevices=False).role)

        except Exception, e:
            log.error("Error in %s", args.bom)
            log.error(e)
            return 1
    # will never get here
    else:
        log.error("Please provide either a configuration file or the address of the active system manager")
        return 1

    # start system manager
    try:
        systemManager = SystemManager(args.node, clusterInfo)
        systemManager.run()
        return 0
    except MessagingException as e:
        log.error("Could not set up System Manager: %s", e)
        return 1

if __name__ == '__main__':
    sys.exit(main())
