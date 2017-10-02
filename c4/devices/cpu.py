"""
Copyright (c) IBM 2015-2017. All Rights Reserved.
Project name: c4-system-manager
This project is licensed under the MIT License, see LICENSE
"""
import logging

from c4.system.deviceManager import DeviceManagerImplementation, DeviceManagerStatus

log = logging.getLogger(__name__)

__version__ = "0.1.0.0"

class Cpu(DeviceManagerImplementation):
    """
    CPU devmgr
    """
    IDLE_INDEX = 3  # index of "idle cpu" from /proc/stat
    WAIT_INDEX = 4  # index of "wait cpu" from /proc/stat

    def __init__(self, clusterInfo, name, properties=None):
        super(Cpu, self).__init__(clusterInfo, name, properties)
        self.prev_cpu_idle = 0.0
        self.prev_cpu_wait = 0.0
        self.prev_cpu_total = 0.0

    def calculateCPUUsage(self):
        """
        Calculates CPU usage and IO Wait
        Note: the first time this function is called, the cpu
        usage will be the average since boot.  Subsequent calls
        will return the usage since last time function was called.

        :returns: overall cpu usage as a float (percent)
            and IO wait as a float (percent)
        """

        with open("/proc/stat") as f:
            # Example line from /proc/stat
            # cpu  1023890566 36368 356430133 21668748589 16353131 3017 6033012 0 633076568
            for line in f:
                if line.startswith("cpu "):
                    cpu_times = line.split()
                    # remove the cpu header
                    del cpu_times[0]
                    # keep first five fields
                    # user, nice, system, idle, and wait
                    # so that the output is similar to the "top" command
                    del cpu_times[5:]

                    # calculate total cpu
                    total = sum(map(float, cpu_times))

                    # extract idle
                    idle = float(cpu_times[self.IDLE_INDEX])
                    # extract wait
                    wait = float(cpu_times[self.WAIT_INDEX])

                    # calculate difference since last call
                    diff_idle = idle - self.prev_cpu_idle
                    diff_wait = wait - self.prev_cpu_wait
                    diff_total = total - self.prev_cpu_total

                    # calculate usage
                    usage = 100 * (diff_total - diff_idle) / diff_total
                    # calculate io wait
                    io_wait = 100 * (diff_wait / diff_total)

                    # save values for next time
                    self.prev_cpu_idle = idle
                    self.prev_cpu_wait = wait
                    self.prev_cpu_total = total
                    return (usage, io_wait)

        # should never get here
        return 0.0

    def handleStatus(self, message):
        """
        The handler for an incoming Status message.
        """
        log.debug("Received status request: %s" % message)
        (usage, iowait) = self.calculateCPUUsage()
        return CPUStatus(usage, iowait)

class CPUStatus(DeviceManagerStatus):
    def __init__(self, usage, iowait):
        super(CPUStatus, self).__init__()
        self.usage = usage
        self.iowait = iowait
