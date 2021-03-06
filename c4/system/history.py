"""
Copyright (c) IBM 2015-2017. All Rights Reserved.
Project name: c4-system-manager
This project is licensed under the MIT License, see LICENSE
"""
from abc import ABCMeta, abstractmethod


class DeviceHistory(object):
    """
    Device manager history
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def add(self, node, name, status, ttl=None):
        """
        Add status for device manager with specified name on specified node

        :param node: node name
        :type node: str
        :param name: device manager name
        :type name: str
        :param status: status
        :type status: :class:`DeviceManagerStatus`
        :param ttl: time to live (in seconds), infinite by default
        :type ttl: int
        """

    @abstractmethod
    def get(self, node, name, limit=None):
        """
        Get status history for device manager with specified name on specified node

        :param node: node name
        :type node: str
        :param name: device manager name
        :type name: str
        :param limit: number of statuses to return
        :type limit: int
        :returns: list of history entries
        :rtype: [:class:`Entry`]
        """

    @abstractmethod
    def getAll(self):
        """
        Get status history for all device managers on all nodes

        :returns: list of history entries
        :rtype: [:class:`Entry`]
        """

    @abstractmethod
    def getLatest(self, node, name):
        """
        Get latest status for device manager with specified name on specified node

        :param node: node name
        :type node: str
        :param name: device manager name
        :type name: str
        :returns: history entry
        :rtype: :class:`Entry`
        """

    @abstractmethod
    def remove(self, node=None, name=None):
        """
        Remove status history for device managers with specified names on specified nodes.

        node and name:
            remove history for specific device on a specific node

        node and no name
            remove history for all devices on a specific node

        no node and name
            remove history for specific device on all nodes

        no node and no name
            remove history for all devices on all nodes

        :param node: node name
        :type node: str
        :param name: device manager name
        :type name: str
        """

class Entry(object):
    """
    History entry with timestamp and status information

    :param timestamp: datetime instance
    :type timestamp: :class:`Datetime`
    :param status: status
    :type status: :class:`SystemManagerStatus` or :class:`DeviceManagerStatus`
    """
    def __init__(self, timestamp, status):
        self.timestamp = timestamp
        self.status = status

class NodeHistory(object):
    """
    System manager history
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def add(self, node, status, ttl=None):
        """
        Add status for system manager with on specified node

        :param node: node name
        :type node: str
        :param status: status
        :type status: :class:`SystemManagerStatus`
        :param ttl: time to live (in seconds), infinite by default
        :type ttl: int
        """

    @abstractmethod
    def get(self, node, limit=None):
        """
        Get status history for system manager on specified node

        :param node: node name
        :type node: str
        :param limit: number of statuses to return
        :type limit: int
        :returns: list of history entries
        :rtype: [:class:`Entry`]
        """

    @abstractmethod
    def getAll(self):
        """
        Get status history for all system managers on all nodes

        :returns: list of history entries
        :rtype: [:class:`Entry`]
        """

    @abstractmethod
    def getLatest(self, node):
        """
        Get latest status for system manager on specified node

        :param node: node name
        :type node: str
        :returns: history entry
        :rtype: :class:`Entry`
        """

    @abstractmethod
    def remove(self, node=None):
        """
        Remove status history for system managers on specified nodes.

        node:
            remove history for specific node

        no node
            remove history for all nodes

        :param node: node name
        :type node: str
        """
