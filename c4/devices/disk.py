"""
Copyright (c) IBM 2015-2017. All Rights Reserved.
Project name: c4-system-manager
This project is licensed under the MIT License, see LICENSE
"""
import logging
import os

from c4.system.deviceManager import DeviceManagerImplementation, DeviceManagerStatus

log = logging.getLogger(__name__)

__version__ = "0.1.0.0"

class Disk(DeviceManagerImplementation):
    """
    Disk devmgr

    Retrieves a list of paths in properties.
    Calculates disk usage for each path.
    """

    def __init__(self, clusterInfo, name, properties=None):
        super(Disk, self).__init__(clusterInfo, name, properties=properties)
        if properties is not None and "paths" in properties:
            self.paths = properties["paths"]
        else:
            self.paths = ["/"]

    def calculateDiskUsage(self, path):
        """
        Calculates disk usage given a path (e.g. /)

        :returns: path, total (k), used (k), used_percent
        """
        try:
            stats = os.statvfs(path)
            free = (stats.f_bavail * stats.f_frsize) / 1024
            total = (stats.f_blocks * stats.f_frsize) / 1024
            used = total - free
            usage = used / float(total) * 100
            return (path, total, used, usage)
        except Exception, e:
            return (0, 0, 0, 0)

    def handleStatus(self, message):
        """
        The handler for an incoming Status message.
        """
        log.debug("Received status request: %s" % message)
        all_status = StatusForAllDisks()
        for path in self.paths:
            (path, total, used, usage) = self.calculateDiskUsage(path)
            disk_status = DiskStatus(path, total, used, usage)
            all_status.append(path, disk_status)
        return all_status

class StatusForAllDisks(DeviceManagerStatus):
    def __init__(self):
        super(StatusForAllDisks, self).__init__()
        self.disks = {}

    def append(self, name, disk_status):
        """
        :param name: The name of the disk
        :type name: str
        :param disk_status: The disk status object
        :type disk_status: :class:`~dynamite.devices.disk.DiskStatus`
        """
        self.disks[name] = disk_status

class DiskStatus(DeviceManagerStatus):
    def __init__(self, path, total, used, usage):
        super(DiskStatus, self).__init__()
        self.path = path
        self.total = total
        self.used = used
        self.usage = usage
