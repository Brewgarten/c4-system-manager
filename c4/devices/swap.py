import logging

from c4.system.deviceManager import DeviceManagerImplementation, DeviceManagerStatus

log = logging.getLogger(__name__)

__version__ = "0.1.0.0"

class Swap(DeviceManagerImplementation):
    """
    Swap devmgr
    """
    def __init__(self, clusterInfo, name, properties=None):
        super(Swap, self).__init__(clusterInfo, name, properties=properties)

    def calculateSwapUsage(self):
        """
        Calculates swap usage

        :returns: total (k), used (k), used_percent
        """
        skipFirstLine = True
        total = 0
        used = 0
        with open("/proc/swaps") as f:
            # Example contents from /proc/swaps
            # Filename                Type        Size    Used    Priority
            # /dev/dm-1              partition    24780792    0    -1
            for line in f:
                if skipFirstLine:
                    skipFirstLine = False
                else:
                    swap_tokens = line.split()
                    if len(swap_tokens) >= 4:
                        # could be multiple swap files, so keep a running total
                        total = total + int(swap_tokens[2])
                        used = used + int(swap_tokens[3])
        # handle case when file is empty, like on a VM
        if total == 0:
            usage = 0
        else:
            usage = used / float(total) * 100
        return (total, used, usage)

    def handleStatus(self, message):
        """
        The handler for an incoming Status message.
        """
        log.debug("Received status request: %s" % message)
        (total, used, usage) = self.calculateSwapUsage()
        return SwapStatus(total, used, usage)

class SwapStatus(DeviceManagerStatus):
    def __init__(self, total, used, usage):
        super(SwapStatus, self).__init__()
        self.total = total
        self.used = used
        self.usage = usage
