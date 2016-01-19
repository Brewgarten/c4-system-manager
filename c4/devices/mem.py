import logging

from c4.system.deviceManager import DeviceManagerImplementation, DeviceManagerStatus

log = logging.getLogger(__name__)

__version__ = "0.1.0.0"

class Memory(DeviceManagerImplementation):
    """
    Memory devmgr
    """
    def __init__(self, host, name, properties=None):
        super(Memory, self).__init__(host, name, properties=properties)

    def calculateMemoryInfo(self):
        """
        Calculates memory info

        Extracts total memory, free memory, buffer memory, and cached memory
        from /proc/meminfo.  Values are in kB.

        Also calculates used memory by subtracting free, buffer, and cached
        memory from total.

        Also calculates used memory as a percent

        :returns: memory status
        :rtype: :class:`~MemoryStatus`
        """

        # Example lines from /proc/meminfo
        # MemTotal:       198335704 kB
        # MemFree:        49995828 kB
        # Buffers:          902040 kB
        # Cached:         92300176 kB
        mem_info = {}

        with open("/proc/meminfo") as f:
            for line in f:
                if line.startswith("MemTotal"):
                    tokens = line.split()
                    mem_info["total_memory"] = int(tokens[1])

                elif line.startswith("MemFree"):
                    tokens = line.split()
                    mem_info["free_memory"] = int(tokens[1])

                elif line.startswith("Buffers"):
                    tokens = line.split()
                    mem_info["buffer_memory"] = int(tokens[1])

                elif line.startswith("Cached"):
                    tokens = line.split()
                    mem_info["cached_memory"] = int(tokens[1])

        mem_info["used_memory"] = mem_info["total_memory"] - \
                                    mem_info["free_memory"] - \
                                    mem_info["buffer_memory"] - \
                                    mem_info["cached_memory"]

        mem_info["usage"] = mem_info["used_memory"] / float(mem_info["total_memory"]) * 100
        return MemoryStatus(mem_info["buffer_memory"], mem_info["cached_memory"], mem_info["free_memory"],
                            mem_info["total_memory"], mem_info["used_memory"], mem_info["usage"])

    def handleStatus(self, message):
        """
        The handler for an incoming Status message.
        """
        log.debug("Received status request: %s" % message)
        # TODO: for now return status on everything
        return self.calculateMemoryInfo()

class MemoryStatus(DeviceManagerStatus):
    def __init__(self, buffer_memory, cached_memory, free_memory, total_memory, used_memory, usage):
        super(MemoryStatus, self).__init__()
        self.buffer_memory = buffer_memory
        self.cached_memory = cached_memory
        self.free_memory = free_memory
        self.total_memory = total_memory
        self.used_memory = used_memory
        self.usage = usage
