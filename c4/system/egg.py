"""
Copyright (c) IBM 2015-2017. All Rights Reserved.
Project name: c4-system-manager
This project is licensed under the MIT License, see LICENSE
"""
import logging
import base64
import os
import subprocess

from c4.messaging import RouterClient
from c4.system.messages import DeployDeviceManager, UndeployDeviceManager

def sendDeployToOtherSystemManagers(node_name, node_names, sysmgr_address, type, file_name, data):
    """
    Handles a message from the rest server to the active system manager to distribute an egg
    to other system managers.

    :param node_name: The name of this node
    :type node_name: str
    :param node_names: The list of nodes in the system, so that the active system manager can send a deploy egg message to all other system managers.
    :type node_names: list
    :param sysmgr_address: The address of the sysmgr, so that a message can be sent to it.
    :type sysmgr_address: str
    :param type: Type is the dotted module names for the custom device manager class.  e.g. c4.devices.mydm.MyDM
    :type type: str
    :param file_name: The file name of the egg e.g. MyCustomDM-0.1-py2.7.egg
    :type file_name: str
    :param data: The base 64 encoded egg file
    :type data: str
    """
    client = RouterClient(node_name)
    # distribute to other system managers, if any
    for a_node_name in node_names:
        # but skip ourself
        if a_node_name != node_name:
            # send to node
            logging.debug("Sending deploy device manager request to %s", a_node_name)
            new_message = DeployDeviceManager("system-manager", a_node_name)
            new_message.Message["type"] = type
            new_message.Message["data"] = data
            new_message.Message["file_name"] = file_name
            client.forwardMessage(new_message)


def deployEgg(type, file_name, data):
    """
    Deploys the egg to the system.  Assumes system manager is run as root.

    :param type: Type is the dotted module names for the custom device manager class.  e.g. c4.devices.mydm.MyDM
    :type type: str
    :param file_name: The file name of the egg e.g. MyCustomDM-0.1-py2.7.egg
    :type file_name: str
    :param data: The base 64 encoded egg file
    :type data: str
    """

    # write egg to a temp file
    # using the type as the file name
    # and appending the version and the Python version from the original egg file
    org_egg_file_name = file_name
    org_egg_file_name_parts = org_egg_file_name.split("-")
    parts_len = len(org_egg_file_name_parts)
    if parts_len < 3:
        logging.error("Unable to deploy device manager egg.  File name does not contain correct version and Python version: %s", org_egg_file_name)
        return False
    egg_file_path = "/tmp/" + type + "-" + org_egg_file_name_parts[parts_len - 2] + "-" + org_egg_file_name_parts[parts_len - 1]
    with open(egg_file_path, "wb") as egg:
        egg.write(base64.b64decode(data))
    # install egg
    # assuming we, the system manager, is running as root
    return_code = subprocess.call(["/opt/ibm/c4/python/bin/easy_install " + egg_file_path], shell=True)
    if return_code != 0:
        logging.error("Unable to install egg %s", type)
    else:
        logging.debug("Installed egg %s", type)
    # delete the temporary egg file
    os.remove(egg_file_path)
    return return_code == 0

def sendUndeployToOtherSystemManagers(node_name, node_names, sysmgr_address, type):
    """
    Handles a message from the rest server to the active system manager to distribute an egg
    to other system managers.

    :param node_name: The name of this node
    :type node_name: str
    :param node_names: The list of nodes in the system, so that the active system manager can send a deploy egg message to all other system managers.
    :type node_names: list
    :param sysmgr_address: The address of the sysmgr, so that a message can be sent to it.
    :type sysmgr_address: str
    :param type: Type is the dotted module names for the custom device manager class.  e.g. c4.devices.mydm.MyDM
    :type type: str
    """
    client = RouterClient(node_name)
    # distribute to other system managers, if any
    for a_node_name in node_names:
        # but skip ourself
        if a_node_name != node_name:
            # send to node
            logging.debug("Sending undeploy device manager request to %s", a_node_name)
            new_message = UndeployDeviceManager("system-manager", a_node_name)
            new_message.Message["type"] = type
            client.forwardMessage(new_message)

def undeployEgg(type):
    """
    Undeploys the egg from the system.  Assumes system manager is run as root.

    :param type: Type is the dotted module names for the custom device manager class.  e.g. c4.devices.mydm.MyDM
    :type type: str
    """
    # uninstall egg
    # assuming we, the system manager, is running as root
    return_code = subprocess.call(["/opt/ibm/c4/python/bin/pip uninstall -y " + type], shell=True)
    if return_code != 0:
        logging.error("Unable to uninstall egg %s", type)
        return False
    else:
        logging.debug("Uninstalled egg %s", type)
        return True
