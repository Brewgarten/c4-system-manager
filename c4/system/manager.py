#!/usr/bin/env python

import argparse
import datetime
import imp
import logging
import os
import pkg_resources
import platform as platformSpec
import socket
import sys
import time

from multiprocessing.managers import SyncManager

import c4.system.actions
import c4.system.devices
import c4.system.events
import c4.system.policies
import gpfs

from c4.messaging import MessageTracker, MessagingException, PeerRouter, callMessageHandler, sendMessage,\
    sendMessageToRouter
from c4.system import db, policyEngine, egg, alertManager
from c4.system.configuration import DBClusterInfo, Configuration, NodeInfo, States, Roles, GPFSClusterInfo,\
    ConfigurationInfo
from c4.system.deviceManager import DeviceManager
from c4.system.messages import Envelope, StartDeviceManagers, Status, StopDeviceManagers, LocalStartDeviceManager, LocalStopDeviceManager, \
    RegistrationNotification, LocalUnregisterDeviceManagers, UnregisterNode, DisableDeviceManager, DisableNode, EnableDeviceManager, \
    EnableNode, StartNode, StopNode, LocalStopNode, RegisterDeviceManager
from c4.utils.util import Timer, disableInterruptSignal, getFullModuleName, getModuleClasses, exclusiveWrite
from c4.system.rest import RestServer
from c4.system.version import Version
from c4.utils.jsonutil import JSONSerializable
from c4.utils.logutil import ClassLogger

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
        super(SystemManager, self).__init__(node, clusterInfo, "ipc://{}.ipc".format(node), name=name, setParentName=True)

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

        self.policyEngine = None
        self.alertManager = None

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

    def executePolicyEngine(self):
        """
        Execute the policy engine
        """
        log.debug("Executing policy engine")
        self.policyEngine.run()

    def executeAlertManager(self):
        """
        Execute the WTi alert manager
        """
        log.debug("Executing alert manager")
        self.alertManager.run()

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

    @classmethod
    def getSSLCertificateFile(cls, platform):
        """
        Gets the path of the SSL certificate file.

        If the SSL certificate file is just a filename, then assume it resides in the data directory.
        """
        ssl_certificate_file = platform.settings.get("ssl_certificate_file", "ssl.crt")
        # if there is no path, then assume the file is in the data dir
        if os.path.dirname(ssl_certificate_file) == "":
            ssl_certificate_file = pkg_resources.resource_filename("c4.data", "ssl/" + ssl_certificate_file)  # @UndefinedVariable
        return ssl_certificate_file

    @classmethod
    def getSSLKeyFile(cls, platform):
        """
        Gets the path of the SSL key file.

        If the SSL key file is just a filename, then assume it resides in the data directory.
        """
        ssl_key_file = platform.settings.get("ssl_key_file", "ssl.key")
        # if there is no path, then assume the file is in the data dir
        if os.path.dirname(ssl_key_file) == "":
            ssl_key_file = pkg_resources.resource_filename("c4.data", "ssl/" + ssl_key_file)  # @UndefinedVariable
        return ssl_key_file

    def handleDeploydevicemanager(self, message, envelope):
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

    def handleDeploypolicy(self, message, envelope):
        """
        Handle :class:`~c4.system.messages.DeployPolicy` messages.
        A message from the rest server to the active system manager to distribute and install the
        custom policy egg.
        Or a message from the active system manager to the other system managers to install the
        custom policy egg.

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        log.info("Deploy policy request from %s", envelope.From)
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

    def handleDisableDeviceManagerRequest(self, message, envelope):
        """
        Handle :class:`~c4.system.messages.DisableDeviceManagerRequest` messages.
        A message from the rest server to the active system manager to disable one or more device managers.
        Changes device state from RUNNING to MAINTENANCE or REGISTERED to MAINTENANCE.

        The message contains an attribute called names which is a list of device manager names to disable.

        The message contains an attribute called nodes which is a list of node names.

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        log.info("Disable device manager request from %s for %s on %s",
                     envelope.From, message["names"], message["nodes"])
        # validate role
        if self.clusterInfo.role != Roles.ACTIVE:
            log.error("Received disable device manager request from '%s' but current role is '%s', should be '%s'",
                           envelope.From, self.clusterInfo.role, Roles.ACTIVE)
            return

        try:
            node_list = self.validateNodeListAndTransformWildcards(message["nodes"])
        except Exception, e:
            return {"error": str(e)}

        configurationInfo = Configuration().toInfo()
        try:
            node_dict = self.getNodeDictFromDeviceList(message["names"], node_list,
                                                       configurationInfo)
        except Exception, e:
            return {"error": str(e)}

        # validate state
        stop_dict = {}
        for node_name, device_dict in node_dict.iteritems():
            for device_name in device_dict.keys():
                device_state = device_dict[device_name].state
                # REGISTERED to MAINTENANCE
                # already MAINTENANCE, then do nothing
                if device_state == States.MAINTENANCE:
                    # done with this device
                    # remove device from node dict
                    del device_dict[device_name]
                    log.warn("Ignoring request to disable %s/%s because it is already disabled.",
                                 node_name, device_name)
                elif device_state == States.RUNNING:
                    # add to stop list
                    if node_name in stop_dict:
                        device_dict2 = stop_dict[node_name]
                    else:
                        device_dict2 = {}
                        stop_dict[node_name] = device_dict2
                    device_dict2[device_name] = device_dict[device_name]
                    # remove device from node dict
                    del device_dict[device_name]
                # else invalid state
                elif device_state != States.REGISTERED:
                    # remove device from node dict
                    del device_dict[device_name]
                    log.warn("Ignoring request to disable %s/%s because it has an invalid state of %s.  State should be %s or %s.",
                                 node_name, device_name, device_state.value, States.REGISTERED.value, States.RUNNING.value) # @UndefinedVariable

        # stop all RUNNING devices and then set them to MAINTENANCE later
        for node_name, device_dict in stop_dict.iteritems():
            disable_message = DisableDeviceManager(node_name)
            disable_message.Message["devices"] = device_dict
            self.messageTracker.add(disable_message)

            # send stop to each node with a list of devices to stop
            stop_message = StopDeviceManagers(node_name)
            self.messageTracker.addRelatedMessage(disable_message.MessageID, stop_message.MessageID)
            stop_message.Message["devices"] = device_dict
            sendMessage(self.clusterInfo.getNodeAddress(node_name), stop_message)

        # disable devices that are REGISTERED
        for node_name, device_dict in node_dict.iteritems():
            # if no devices for this node, then skip to next node
            if len(device_dict) == 0:
                continue
            # send disable to each node with a list of devices to disable
            disable_message = DisableDeviceManager(node_name)
            disable_message.Message["devices"] = device_dict
            sendMessage(self.clusterInfo.getNodeAddress(node_name), disable_message)

        return {"response": "sent"}

    def handleDisableNodeRequest(self, message, envelope):
        """
        Handle :class:`~c4.system.messages.DisableNodeRequest` messages.
        A message from the rest server to the active system manager to disable one or more nodes.
        Changes state from REGISTERED to MAINTENANCE

        The message contains an attribute called nodes which is a list of node names.

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        log.info("Disable node request from %s for %s", envelope.From, message["nodes"])
        # validate role
        if self.clusterInfo.role != Roles.ACTIVE:
            log.error("Received disable node request from '%s' but current role is '%s', should be '%s'",
                           envelope.From, self.clusterInfo.role, Roles.ACTIVE)
            return

        try:
            node_list = self.validateNodeListAndTransformWildcards(message["nodes"])
        except Exception, e:
            return {"error": str(e)}

        configuration = Configuration()
        # validate state
        for node in node_list:
            disable_message = DisableNode(node)
            nodeState = configuration.getState(node)
            if nodeState == States.MAINTENANCE:
                log.debug("{0} is already disabled".format(node))
            elif nodeState == States.RUNNING:
                log.debug("{0} needs to be put into REGISTERED state first".format(node))
                self.messageTracker.add(disable_message)
                stop_env = StopNode(node)
                self.messageTracker.add(stop_env)
                self.messageTracker.addRelatedMessage(disable_message.MessageID, stop_env.MessageID)
                sendMessage(self.clusterInfo.getNodeAddress(node), stop_env)
            else:
                # disable nodes that are REGISTERED
                sendMessage(self.clusterInfo.getNodeAddress(node), disable_message)

        return {"response": "sent"}

    def handleDisablePolicyRequest(self, message, envelope):
        """
        Handle :class:`~c4.system.messages.DisablePolicyRequest` messages.
        A message from the REST server to the active system manager to
        disable one or more policies

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        log.info("Disable policy request from %s for %s",
                    envelope.From, message)
        # validate role
        if self.clusterInfo.role != Roles.ACTIVE:
            log.error("Received disable policy request from '%s' but current role is '%s', should be '%s'",
                           envelope.From, self.clusterInfo.role, Roles.ACTIVE)
            return
        if self.policyEngine is None:
            log.error("Cannot disable policy because the policy engine has not been initialized.")
        else:
            # stop the timer because we need to modify the policy engine
            # and it is running in another process
            self.stopPolicyEngineTimer()

            # "*" is wildcard for all policies
            all_policies = "*" in message["policies"]
            # go through all policies and if id matches a policy in the given list, then disable it
            for policy in self.policyEngine.policies.itervalues():
                if all_policies or policy.id in message["policies"]:
                    self.policyEngine.disablePolicy(policy)

            self.startPolicyEngineTimer()

        return {"response": "sent"}

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

    def handleEnableDeviceManagerRequest(self, message, envelope):
        """
        Handle :class:`~c4.system.messages.EnableDeviceManagerRequest` messages.
        A message from the rest server to the active system manager to enable one or more device managers.
        Changes state from MAINTENANCE to REGISTERED.

        The message contains an attribute called names which is a list of device manager names to enable.

        The message contains an attribute called nodes which is a list of node names.

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        log.info("Enable device manager request from %s for %s on %s",
                     envelope.From, message["names"], message["nodes"])
        # validate role
        if self.clusterInfo.role != Roles.ACTIVE:
            log.error("Received enable device manager request from '%s' but current role is '%s', should be '%s'",
                           envelope.From, self.clusterInfo.role, Roles.ACTIVE)
            return

        try:
            node_list = self.validateNodeListAndTransformWildcards(message["nodes"])
        except Exception, e:
            return {"error": str(e)}

        configurationInfo = Configuration().toInfo()
        try:
            node_dict = self.getNodeDictFromDeviceList(message["names"], node_list,
                                                       configurationInfo)
        except Exception, e:
            return {"error": str(e)}

        # validate state
        for node_name, device_dict in node_dict.iteritems():
            for device_name in device_dict.keys():
                device_state = device_dict[device_name].state
                # already MAINTENANCE, then do nothing
                if device_state == States.REGISTERED:
                    # done with this device
                    # remove device from node dict
                    del device_dict[device_name]
                    log.warn("Ignoring request to enable %s/%s because it is already enabled.",
                                 node_name, device_name)
                # else invalid state
                elif device_state != States.MAINTENANCE:
                    # remove device from node dict
                    del device_dict[device_name]
                    log.warn("Ignoring request to enable %s/%s because it has an invalid state of %s.  State should be %s.",
                                 node_name, device_name, device_state.value, States.MAINTENANCE.value) # @UndefinedVariable

        # enable devices that are MAINTENANCE
        for node_name, device_dict in node_dict.iteritems():
            # if no devices for this node, then skip to next node
            if len(device_dict) == 0:
                continue
            # send enable to each node with a list of devices to enable
            enable_message = EnableDeviceManager(node_name)
            enable_message.Message["devices"] = device_dict
            sendMessage(self.clusterInfo.getNodeAddress(node_name), enable_message)

        return {"response": "sent"}

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

    def handleEnableNodeRequest(self, message, envelope):
        """
        Handle :class:`~c4.system.messages.EnableNodeRequest` messages.
        A message from the rest server to the active system manager to enable one or more nodes.
        Changes state from MAINTENANCE to REGISTERED.

        The message contains an attribute called nodes which is a list of node names.

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        log.info("Enable node request from %s for %s", envelope.From, message["nodes"])
        # validate role
        if self.clusterInfo.role != Roles.ACTIVE:
            log.error("Received enable node request from '%s' but current role is '%s', should be '%s'",
                           envelope.From, self.clusterInfo.role, Roles.ACTIVE)
            return

        try:
            node_list = self.validateNodeListAndTransformWildcards(message["nodes"])
        except Exception, e:
            return {"error": str(e)}

        configuration = Configuration()
        # validate state
        for node in node_list:
            if configuration.getState(node) != States.MAINTENANCE:
                log.debug("{0} is already enabled".format(node))
            else:
                # enable nodes that are MAINTENANCE
                enable_message = EnableNode(node)
                sendMessage(self.clusterInfo.getNodeAddress(node), enable_message)

        return {"response": "sent"}

    def handleEnablePolicyRequest(self, message, envelope):
        """
        Handle :class:`~c4.system.messages.EnablePolicyRequest` messages.
        A message from the REST server to the active system manager to
        enable one or more policies

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        log.info("Enable policy request from %s for %s",
                    envelope.From, message)
        # validate role
        if self.clusterInfo.role != Roles.ACTIVE:
            log.error("Received enable policy request from '%s' but current role is '%s', should be '%s'",
                           envelope.From, self.clusterInfo.role, Roles.ACTIVE)
            return
        if self.policyEngine is None:
            log.error("Cannot enable policy because the policy engine has not been initialized.")
        else:
            # stop the timer because we need to modify the policy engine
            # and it is running in another process
            self.stopPolicyEngineTimer()

            # "*" is wildcard for all policies
            all_policies = "*" in message["policies"]
            # go through all policies and if id matches a policy in the given list, then enable it
            for policy in self.policyEngine.policies.itervalues():
                if all_policies or policy.id in message["policies"]:
                    self.policyEngine.enablePolicy(policy)

            self.startPolicyEngineTimer()

        return {"response": "sent"}

    def handleGPFSClusterManagerTakeOverNotification(self, message, envelope):
        """
        Handle :class:`~c4.system.messages.GPFSClusterManagerTakeOverNotification` messages

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        # This notification is special in that it will go to every local system manager
        # so that the sys mgr can update its cluster information
        log.critical("Received {0} notification: {1}".format(message["eventName"], message))

        # Step 1: Update the cluster info
        port = '5000'
        try:
            with open('/tmp/port_for_zeromq', 'r') as f:
                port = f.readline().strip()
        except:
            pass
        sysmgr_ip = self.clusterInfo.getNodeAddress(message['clusterManager'])

        configuration = Configuration()
        if self.clusterInfo.role == Roles.ACTIVE or self.clusterInfo.role == Roles.PASSIVE:
            configuration.changeAlias('system-manager', message['clusterManager'])
            if self.clusterInfo.role == Roles.ACTIVE:
                configuration.changeRole(self.node, Roles.PASSIVE)
                self.clusterInfo.role = Roles.PASSIVE
        self.clusterInfo.systemManagerAddress = sysmgr_ip

    def handleGPFSDiskFailureNotification(self, message, envelope):
        """
        Handle :class:`~c4.system.messages.GPFSDiskFailureNotification` messages

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        log.critical("Received {0} notification: {1}".format(message["eventName"], message))

    def handleGPFSFilesetLimitExceededNotification(self, message, envelope):
        """
        Handle :class:`~c4.system.messages.GPFSFilesetLimitExceededNotification` messages

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        log.critical("Received {0} notification: {1}".format(message["eventName"], message))

    def handleGPFSLowDiskSpaceNotification(self, message, envelope):
        """
        Handle :class:`~c4.system.messages.GPFSPreLowDiskSpaceNotification` messages

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        log.critical("Received {0} notification: {1}".format(message["eventName"], message))

    def handleGPFSMountNotification(self, message, envelope):
        """
        Handle :class:`~c4.system.messages.GPFSMountNotification` messages

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        log.critical("Received {0} notification: {1}".format(message["eventName"], message))

    def handleGPFSNodeJoinNotification(self, message, envelope):
        """
        Handle :class:`~c4.system.messages.GPFSNodeJoinNotification` messages

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        log.critical("Received {0} notification: {1}".format(message["eventName"], message))

    def handleGPFSNodeLeaveNotification(self, message, envelope):
        """
        Handle :class:`~c4.system.messages.GPFSNodeLeaveNotification` messages

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        log.critical("Received {0} notification: {1}".format(message["eventName"], message))


    def handleGPFSNoDiskSpaceNotification(self, message, envelope):
        """
        Handle :class:`~c4.system.messages.GPFSPreNoDiskSpaceNotification` messages

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        log.critical("Received {0} notification: {1}".format(message["eventName"], message))

    def handleGPFSNotification(self, message, envelope):
        """
        Handle :class:`~c4.system.messages.Notification` messages

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        log.critical("Received notification: %s", message)
#         global_events = ["nodeJoin","nodeLeave",
#              "quorumLoss", "quorumReached","quorumNodeLoss",
#              "quorumNodeJoin","quorumNodeLeave"]
#         if message["eventName"] in global_events:
#             if self.node != envelope.From:
#                 logging.debug("Discarding global event notification from %s",envelope.From)
#                 return
#         if message["eventName"] == "clusterManagerTakeOver":
#             logging.info("Received clusterManagerTakeOver event from %s", envelope.From)
#             old_cm = self.clusterInfo.aliases['system-manager']
#             self.clusterInfo.aliases['system-manager'] = message['clusterManager']
#             if self.node != message['clusterManager']:
#                 logging.debug("Not the active system manager")
#                 return
#             if old_cm != message['clusterManager']:
#                 # Stop services on old sysmgr, restart services on this node
#                 # Ejecting the node from the cluster should provide most of what we need
#                 # in order to prevent split-brain
#                 try:
#                     # Try to stop DB2 on the old CM
#                     stopEnvelope = Stop('{}/DB21'.format(old_cm))
#                     sendMessage(self.clusterInfo.getNodeAddress(old_cm), stopEnvelope)
#                     gpfs.shutdown([old_cm])
#
#                     # Start DB2 on the new sysmgr
# #                     startEnvelope = Start('system-manager/DB21')
# #                     sendMessage(self.clusterInfo.getNodeAddress('system-manager'), startEnvelope)
#
#                     # Should we try to startup the old sysmgr?
#                     gpfs.startup([old_cm])
#                     # Other measures that may need to be taken
#                 except:
#                     pass
#         self.logGPFSNotifications(message)
#         logging.debug("Received notification %s", message)

    def handleGPFSPreMountNotification(self, message, envelope):
        """
        Handle :class:`~c4.system.messages.GPFSPreMountNotification` messages

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        log.critical("Received {0} notification: {1}".format(message["eventName"], message))

    def handleGPFSPreShutdownNotification(self, message, envelope):
        """
        Handle :class:`~c4.system.messages.GPFSPreShutdownNotification` messages

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        log.critical("Received {0} notification: {1}".format(message["eventName"], message))

    def handleGPFSPreStartupNotification(self, message, envelope):
        """
        Handle :class:`~c4.system.messages.GPFSPreStartupNotification` messages

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        log.critical("Received {0} notification: {1}".format(message["eventName"], message))

    def handleGPFSPreUnmountNotification(self, message, envelope):
        """
        Handle :class:`~c4.system.messages.GPFSPreUnmountNotification` messages

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        log.critical("Received {0} notification: {1}".format(message["eventName"], message))

    def handleGPFSQuorumLossNotification(self, message, envelope):
        """
        Handle :class:`~c4.system.messages.GPFSQuorumLossNotification` messages

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        log.critical("Received {0} notification: {1}".format(message["eventName"], message))

    def handleGPFSQuorumNodeJoinNotification(self, message, envelope):
        """
        Handle :class:`~c4.system.messages.GPFSQuorumNodeJoinNotification` messages

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        log.critical("Received {0} notification: {1}".format(message["eventName"], message))

    def handleGPFSQuorumNodeLeaveNotification(self, message, envelope):
        """
        Handle :class:`~c4.system.messages.GPFSQuorumNodeLeaveNotification` messages

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        log.critical("Received {0} notification: {1}".format(message["eventName"], message))

    def handleGPFSQuorumReachedNotification(self, message, envelope):
        """
        Handle :class:`~c4.system.messages.GPFSQuorumReachedNotification` messages

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        log.critical("Received {0} notification: {1}".format(message["eventName"], message))

    def handleGPFSShutdownNotification(self, message, envelope):
        """
        Handle :class:`~c4.system.messages.GPFSShutdownNotification` messages

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        log.critical("Received {0} notification: {1}".format(message["eventName"], message))

    def handleGPFSStartupNotification(self, message, envelope):
        """
        Handle :class:`~c4.system.messages.GPFSStartupNotification` messages

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        log.critical("Received {0} notification: {1}".format(message["eventName"], message))

    def handleGPFSUnmountNotification(self, message, envelope):
        """
        Handle :class:`~c4.system.messages.GPFSUnmountNotification` messages

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        log.critical("Received {0} notification: {1}".format(message["eventName"], message))

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

    def handleRegisterDeviceManagerRequest(self, message, envelope):
        """
        Handle :class:`~c4.system.messages.RegisterDeviceManagerRequest` messages.
        A message from the rest server to the active system manager to add one or more device managers.

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        log.info("Register device manager request from %s for %s on %s with type %s",
                     envelope.From, message["devices"], message["nodes"], message["type"])

        # validate role
        if self.clusterInfo.role != Roles.ACTIVE:
            log.error("Received register device manager request from '%s' but current role is '%s', should be '%s'",
                           envelope.From, self.clusterInfo.role, Roles.ACTIVE)
            return

        try:
            node_list = self.validateNodeListAndTransformWildcards(message["nodes"])
        except Exception, e:
            return {"error": str(e)}

        # validate type
        # assume that if the device manager exists on the active node, it exists on the thin nodes as well
        type_found = False
        deviceManagerImplementations = self.getDeviceManagerImplementations()
        for implementation in deviceManagerImplementations:
            existing_type = getFullModuleName(implementation) + "." +  implementation.__name__
            if message["type"] == existing_type:
                type_found = True
                break
        if not type_found:
            return {"error": "Type " + message["type"] + " does not exist."}

        # don't allow wildcard for device name
        if "*" in message["devices"] or "" in message["devices"]:
            return {"error": "Wildcard not allowed for device name when registering."}

        # send register devices to each node with a list of devices to register
        for node_name in node_list:
            register_message = RegisterDeviceManager(node_name, message["devices"], message["type"])
            sendMessage(self.clusterInfo.getNodeAddress(node_name), register_message)

        return {"response": "sent"}

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

    def handleStartDeviceManagersRequest(self, message, envelope):
        """
        Handle :class:`~c4.system.messages.StartDeviceManagersRequest` messages.
        A message from the rest server to the active system manager to start one or more device managers.
        Changes device state from REGISTERED to RUNNING.

        The message contains an attribute called names which is a list of device manager names to start.

        The message contains an attribute called nodes which is a list of node names.

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        :returns: Returns either a response sent message or an error message back to the REST server
        """
        log.info("Start device manager request from %s for %s on %s",
                     envelope.From, message["names"], message["nodes"])
        # validate role
        if self.clusterInfo.role != Roles.ACTIVE:
            log.error("Received start device manager request from '%s' but current role is '%s', should be '%s'",
                           envelope.From, self.clusterInfo.role, Roles.ACTIVE)
            return

        # FIXME: create separate StartNodeRequest
        try:
            node_list = self.validateNodeListAndTransformWildcards(message["nodes"])
        except Exception, e:
            return {"error": str(e)}

        configurationInfo = Configuration().toInfo()
        try:
            node_dict = self.getNodeDictFromDeviceList(message["names"], node_list,
                                                       configurationInfo)
        except Exception, e:
            return {"error": str(e)}

        # validate state
        start_dict = {}
        for node_name, device_dict in node_dict.iteritems():
            for device_name in device_dict.keys():
                device_state = device_dict[device_name].state
                # already RUNNING, then do nothing
                if device_state == States.RUNNING:
                    # done with this device
                    # remove device from node dict
                    del device_dict[device_name]
                    log.warn("Ignoring request to start %s/%s because it is already started.",
                                 node_name, device_name)
                # convenience step - MAINTENANCE to RUNNING
                elif device_state == States.MAINTENANCE:
                    # add to start list
                    if node_name in start_dict:
                        device_dict2 = start_dict[node_name]
                    else:
                        device_dict2 = {}
                        start_dict[node_name] = device_dict2
                    device_dict2[device_name] = device_dict[device_name]
                    # remove device from node dict
                    del device_dict[device_name]
                # else invalid state
                elif device_state != States.REGISTERED:
                    # remove device from node dict
                    del device_dict[device_name]
                    log.warn("Ignoring request to start %s/%s because it has an invalid state of %s.  State should be %s.",
                                 node_name, device_name, device_state.value, States.REGISTERED.value) # @UndefinedVariable

        # enable all devices in MAINTENANCE and then set them to RUNNING later
        for node_name, devices_dict in start_dict.iteritems():
            # start later
            start_message = StartDeviceManagers(node_name)
            start_message.Message["devices"] = devices_dict
            self.messageTracker.add(start_message)

            # enable devices first
            enable_message = EnableDeviceManager(node_name)
            self.messageTracker.addRelatedMessage(start_message.MessageID, enable_message.MessageID)
            enable_message.Message["devices"] = devices_dict
            sendMessage(self.clusterInfo.getNodeAddress(node_name), enable_message)

        # start all listed REGISTERED devices
        for node_name, devices_dict in node_dict.iteritems():
            # send start to each system manager
            # with the DeviceInfo subset dict
            start_message = StartDeviceManagers(node_name)
            start_message.Message["devices"] = devices_dict
            sendMessage(self.clusterInfo.getNodeAddress(node_name), start_message)

        return {"response": "sent"}

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

            # TODO: enable start/stop of policy engine, REST server, etc. based on role changes
            if self.clusterInfo.role == Roles.ACTIVE:

                # TODO: why is this necessary?
                # So that when the user queries for version through the REST interface
                # he/she will get only the version for those devmgr that are actually installed
                Version.clearVersion()

                configuration = Configuration()
                platform = configuration.getPlatform()

                # policy engine
                if platform.settings.get("policy_engine_enabled", True):
                    self.policyEngine = policyEngine.PolicyEngine()
                    self.startPolicyEngineTimer()

                # WTi alert manager
                if platform.settings.get("alert_manager_enabled", False):
                    self.alertManager = alertManager.AlertManager(
                        port=platform.settings.get("alert_manager_wti_port", "11081"))
                    self.startAlertManagerTimer()

                # get SSL options from configuration
                ssl_enabled = platform.settings.get("ssl_enabled", True)
                ssl_certificate_file = self.getSSLCertificateFile(platform)
                ssl_key_file = self.getSSLKeyFile(platform)

                # use rest listen address to limit requests to a particular network, e.g. 127.0.0.1 for localhost only
                rest_listen_address = platform.settings.get("rest_listen_address", "")

                # REST server
                rest_port = platform.settings.get("rest_port", 8443)
                self.processes["restServer"] = RestServer(self.node, self.clusterInfo.getNodeAddress(self.node),
                                                          rest_port, ssl_enabled, ssl_certificate_file, ssl_key_file,
                                                          rest_listen_address)
                # TODO: why daemon?
                # So that the restServer will exit when the sysmgr exits
                self.processes["restServer"].daemon = True
                self.processes["restServer"].start()
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

    def handleStopDeviceManagersRequest(self, message, envelope):
        """
        Handle :class:`~c4.system.messages.StopDeviceManagersRequest` messages.
        A message from the rest server to the active system manager to stop one or more device managers.
        Changes device state from RUNNING to REGISTERED.

        The message contains an attribute called names which is a list of device manager names to stop.

        The message contains an attribute called nodes which is a list of node names.

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        :param disable: Defaults to False.  If True, sets device state from REGISTERED to DISABLED
        :type disable: bool
        :returns: Returns either a response sent message or an error message back to the REST server
        """
        log.info("Stop device manager request from %s for %s on %s",
                     envelope.From, message["names"], message["nodes"])
        # validate role
        if self.clusterInfo.role != Roles.ACTIVE:
            log.error("Received stop device manager request from '%s' but current role is '%s', should be '%s'",
                           envelope.From, self.clusterInfo.role, Roles.ACTIVE)
            return

        try:
            node_list = self.validateNodeListAndTransformWildcards(message["nodes"])
        except Exception, e:
            return {"error": str(e)}

        configurationInfo = Configuration().toInfo()
        try:
            node_dict = self.getNodeDictFromDeviceList(message["names"], node_list,
                                                       configurationInfo)
        except Exception, e:
            return {"error": str(e)}

        # validate state
        for node_name, device_dict in node_dict.iteritems():
            for device_name in device_dict.keys():
                device_state = device_dict[device_name].state
                # device is already REGISTERED, so its done.
                # remove it from the list of devices to stop (i.e. node_dict)
                if device_state == States.REGISTERED:
                    del device_dict[device_name]
                    log.warn("Ignoring request to stop %s/%s because it is already stopped.",
                                 node_name, device_name)
                # this device has an invalid state
                # remove it from the list of devices to stop (i.e. node_dict)
                elif device_state != States.RUNNING:
                    del device_dict[device_name]
                    log.warn("Ignoring request to stop %s/%s because it has an invalid state of %s.  State should be %s.",
                                 node_name, device_name, device_state.value, States.RUNNING.value) # @UndefinedVariable

        for node_name, device_dict in node_dict.iteritems():
            # if no devices for this node, then skip to next node
            if len(device_dict) == 0:
                continue
            # send stop to each node with a list of devices to stop
            stop_message = StopDeviceManagers(node_name)
            stop_message.Message["devices"] = device_dict
            sendMessage(self.clusterInfo.getNodeAddress(node_name), stop_message)

        return {"response": "sent"}

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

    def handleUndeploydevicemanager(self, message, envelope):
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

    def handleUndeploypolicy(self, message, envelope):
        """
        Handle :class:`~c4.system.messages.UndeployPolicy` messages.
        A message from the rest server to the active system manager to delete the egg.  The system manager
        will also tell other system managers to delete the egg.
        Or a message from the active system manager to the other system managers to delete the egg.

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        log.info("Undeploy policy request from %s", envelope.From)
        self._handleUndeployEgg(message, envelope)

    def handleUnregisterDeviceManagersRequest(self, message, envelope):
        """
        Handle :class:`~c4.system.messages.UnregisterDeviceManagersRequest` messages

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        log.debug("Received unregister device request from %s for %s on node(s) %s",
                 envelope.From, message['devices'], message['nodes'])
        try:
            nodes_list = self.validateNodeListAndTransformWildcards(message['nodes'])
        except Exception as e:
            return {'error': 'Bad node name in request: {0}'.format(e)}

        configurationInfo = Configuration().toInfo()
        try:
            node_dict = self.getNodeDictFromDeviceList(message["devices"], nodes_list, configurationInfo)
        except Exception, e:
            return {"error": str(e)}

        self.messageTracker.add(envelope)
        for node_name, device_dict in node_dict.iteritems():
            devices_to_stop = dict((key, value) for (key, value) in device_dict.iteritems() if value == States.RUNNING)
            devices_to_unregister = dict((key, value) for (key, value) in device_dict.iteritems() if value != States.RUNNING)

            if devices_to_stop:
                unregister_after_stop = LocalUnregisterDeviceManagers(node_name, devices_to_stop.keys())
                self.messageTracker.addRelatedMessage(envelope.MessageID, unregister_after_stop.MessageID)
                self.messageTracker.add(unregister_after_stop)
                stop_env = StopDeviceManagers(node_name)
                stop_env.Message['devices'] = devices_to_stop
                self.messageTracker.addRelatedMessage(unregister_after_stop.MessageID, stop_env.MessageID)
                sendMessageToRouter("ipc://{0}.ipc".format(self.node), stop_env)
            if devices_to_unregister:
                unregister_message = LocalUnregisterDeviceManagers(node_name, devices_to_unregister.keys())
                self.messageTracker.addRelatedMessage(envelope.MessageID, unregister_message.MessageID)
                sendMessageToRouter("ipc://{0}.ipc".format(self.node), unregister_message)

        return {'response': 'sent'}

    def handleUnregisterNodeRequest(self, message, envelope):
        """
        Handle :class:`~c4.system.messages.UnregisterNodeRequest` messages

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        log.debug("Received unregister node request from %s for %s",
                 envelope.From, message['nodes'])
        try:
            nodes_list = self.validateNodeListAndTransformWildcards(message['nodes'])
        except Exception as e:
            return {'error': 'Bad node name in request: {0}'.format(e)}

        configuration = Configuration()
        self.messageTracker.add(envelope)
        for n in nodes_list:
            unregister_message = UnregisterNode(n)
            self.messageTracker.addRelatedMessage(envelope.MessageID, unregister_message.MessageID)
            configuration.changeTargetState(n, None, States.DEPLOYED)
            state = configuration.getState(n)
            if state == States.REGISTERED:
                configuration.changeState(n, None, States.UNREGISTERING)
                sendMessageToRouter("ipc://{0}.ipc".format(self.node), unregister_message)
            elif state == States.MAINTENANCE:
                configuration.changeState(n, None, States.ENABLING)
                enable_env = EnableNode(n)
                self.messageTracker.add(unregister_message)
                self.messageTracker.addRelatedMessage(unregister_message.MessageID, enable_env.MessageID)
                sendMessageToRouter("ipc://{0}.ipc".format(self.node), enable_env)
            elif state == States.RUNNING:
                configuration.changeState(n, None, States.STOPPING)
                stop_env = StopNode(n)
                self.messageTracker.add(unregister_message)
                self.messageTracker.addRelatedMessage(unregister_message.MessageID, stop_env.MessageID)
                sendMessageToRouter("ipc://{0}.ipc".format(self.node), stop_env)

        return {'response': 'sent'}

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
        Handle :class:`~c4.system.messages.Unregister` response

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

    def logGPFSNotifications(self, message):
        """
        Log GPFS Notifications

        :param message: message
        :type message: dict
        """

        components = ['diskName','fsName','filesetName','storagePool','eventNode','clusterManager']
        info = ['reason','filesetSize','quorumNodes','filesetLimit','filesetQuota', "myNode"]
        log_time = message["time"]
        event = message["eventName"]
        eventComponent = ""
        msg = ""
        for comp in components:
            if comp in message:
                if eventComponent == "":
                    eventComponent += comp + ":" + message[comp]
                else:
                    eventComponent += ' ' + comp + ":" + message[comp]
        for i in info:
            if i in message:
                if msg == "":
                    msg += i + ":" + message[i]
                else:
                    msg += ' ' + i + ":" + message[i]
        text = log_time + ' [' + event + '] ' + eventComponent
        if msg:
            text += " " + msg
        try:
            exclusiveWrite("/head/logs/callback_logs", text+"\n")
        except:
            logging.debug("Can't write to callback log, probably no longer the active master")

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

    def startPolicyEngineTimer(self):
        """
        Start the policy engine timer.

        For example, initial system manager startup.

        For example, after the policy engine is modified like enable/disable.
        """
        configuration = Configuration()
        self.processes["policyEngineTimer"] = Timer("{}-PolicyEngineTimer".format(self.node),
                                 self.executePolicyEngine, 5, -1,
                                 configuration.getPlatform().settings.get("policy_timer_interval", 5000)/1000)
        self.processes["policyEngineTimer"].start()

    def startAlertManagerTimer(self):
        """
        Start the WTi alert manager timer.
        """
        configuration = Configuration()
        self.processes["alertManagerTimer"] = Timer("{}-AlertManagerTimer".format(self.node),
                                 self.executeAlertManager, 5, -1,
                                 configuration.getPlatform().settings.get("alert_manager_timer_interval", 10000)/1000)
        self.processes["alertManagerTimer"].start()

    def stop(self):
        """
        Stop system manager implementation
        """
        for name, process in self.processes.items():
            self.log.debug("stopping %s", name)
            process.terminate()
            process.join()
            self.log.info("stopped %s", name)

    def stopPolicyEngineTimer(self):
        """
        Stop the policy engine timer

        For example, after the policy engine is modified like enable/disable.
        """
        if "policyEngineTimer" in self.processes:
            self.processes["policyEngineTimer"].terminate()
            self.processes["policyEngineTimer"].join()

    def stopAlertManagerTimer(self):
        """
        Stop the WTi alert manager.
        """
        if "alertManagerTimer" in self.processes:
            self.processes["alertManagerTimer"].terminate()
            self.processes["alertManagerTimer"].join()

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
    parser.add_argument("-g","--gpfs", dest="gpfs", action="store_true", help="Specifies that GPFS is being used")
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
        if args.gpfs:
            clusterInfo = GPFSClusterInfo(args.node,
                                        "tcp://{0}:{1}".format(socket.gethostbyname(socket.gethostname()), args.port),
                                        args.sm_addr, Roles.THIN)
        else:
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
                sys.exit(1)

            if args.gpfs:
                clusterInfo = GPFSClusterInfo(args.node,
                                              configuration.getAddress(args.node),
                                              configuration.getAddress("system-manager"),
                                              configuration.getNode(args.node, includeDevices=False).role)
            else:
                clusterInfo = DBClusterInfo(args.node,
                                            configuration.getAddress(args.node),
                                            configuration.getAddress("system-manager"),
                                            configuration.getNode(args.node, includeDevices=False).role)

            # verify ssl certificate and key file exists
            platform = configuration.getPlatform()
            ssl_enabled = platform.settings.get("ssl_enabled", True)
            if ssl_enabled:
                ssl_certificate_file = SystemManagerImplementation.getSSLCertificateFile(platform)
                if not os.path.isfile(ssl_certificate_file):
                    log.error("SSL certificate file does not exist: %s", ssl_certificate_file)
                    sys.exit(1)
                ssl_key_file = SystemManagerImplementation.getSSLKeyFile(platform)
                if not os.path.isfile(ssl_key_file):
                    log.error("SSL key file does not exist: %s", ssl_key_file)
                    sys.exit(1)

        except Exception, e:
            log.error("Error in %s", args.bom)
            log.error(e)
            sys.exit(1)
    # will never get here
    else:
        log.error("Please provide either a configuration file or the address of the active system manager")
        sys.exit(1)

    # Note the port we're using for zmq messages
    if args.gpfs:
        try:
            exclusiveWrite("/tmp/port_for_zeromq", args.port, append=False)
        except Exception as e:
            log.error("Could not write port to /tmp/port_for_zeromq")
            log.error(e)
            sys.exit(1)
    # start system manager
    try:
        systemManager = SystemManager(args.node, clusterInfo)
    except MessagingException as e:
        log.error("Could not set up System Manager: %s", e)
    else:
        systemManager.run()

if __name__ == '__main__':
    main()
