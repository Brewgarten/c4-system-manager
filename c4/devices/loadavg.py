import logging

from c4.system.deviceManager import DeviceManagerImplementation, DeviceManagerStatus

log = logging.getLogger(__name__)

__version__ = "0.1.0.0"

class LoadAvg(DeviceManagerImplementation):
    """
    LoadAvg devmgr
    """
    def __init__(self, clusterInfo, name, properties=None):
        super(LoadAvg, self).__init__(clusterInfo, name, properties=properties)
        # get number of CPUs and total number of cores
        self.numCPUs = 0
        self.totalCores = 0
        with open("/proc/cpuinfo") as f:
            # In file /proc/cpuinfo
            for line in f:
                # count number of occurrences of "model name"
                if line.startswith("model name"):
                    self.numCPUs = self.numCPUs + 1
                # keep a running total of cores
                if line.startswith("cpu cores"):
                    core_info = line.split(":")
                    self.totalCores = self.totalCores + int(core_info[1])


    def calculateLoadAvg(self):
        """
        Calculates load average for the last 1, 5, and 15 minutes

        :returns: load1, load5, load15
        """
        with open("/proc/loadavg") as f:
            # Example line from /proc/loadavg
            # 2.80 1.59 1.18 2/1704 29054
            for line in f:
                averages = line.split()
                load1 = float(averages[0])
                load5 = float(averages[1])
                load15 = float(averages[2])
                break
        return (load1, load5, load15)

    def handleStatus(self, message):
        """
        The handler for an incoming Status message.
        """
        log.debug("Received status request: %s" % message)
        (load1, load5, load15) = self.calculateLoadAvg()
        return LoadAvgStatus(load1, load5, load15, self.numCPUs)

class LoadAvgStatus(DeviceManagerStatus):
    def __init__(self, load1, load5, load15, num_cpus):
        super(LoadAvgStatus, self).__init__()
        self.load1 = load1
        self.load5 = load5
        self.load15 = load15
        self.num_cpus = num_cpus
