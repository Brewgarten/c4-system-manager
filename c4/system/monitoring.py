"""
Copyright (c) IBM 2015-2017. All Rights Reserved.
Project name: c4-system-manager
This project is licensed under the MIT License, see LICENSE
"""

from datetime import datetime
import json
import logging
import re

from c4.system.backend import Backend
from c4.utils.logutil import ClassLogger
from c4.utils.util import exclusiveWrite
from c4.utils.jsonutil import (Datetime,
                               JSONSerializable)

log = logging.getLogger(__name__)

class MonitorBackend(object):
    """
    Interface for Monitor data I/O
    """
    def __init__(self, isWriteOnly=True):
        self.isWriteOnly = isWriteOnly

    def reportEvent(self, eventName, eventStatus, otherInfoJson):
        """
        Add an entry for a tracked event
        :param eventName: Event for which status is being reported
        :type eventName: str
        :param result: Resultant status
        :type result: int
        :param otherInfo: Other information to be stored about the event
        :type otherInfo: dict
        """
        raise NotImplementedError("Subclass must implement reportEvent!")

    def queryEvents(self, eventName=None, startTime=None):
        """
        Query entries for tracked event(s)

        Return Value Format:
            {
                "statuses": [
                    {
                        "timestamp": "2016-11-02 18:11:08"
                        "event": "Console"
                        "status": 0
                        "details": {}
                    }
                ]
            }

        :param eventName: Specific event to return; All events if None (default)
        :type eventName: str
        :param startTime: Return events after the specified time.
        :type startTime: str Default: None
        :returns: events
        :rtype: Dictionary
        """
        raise NotImplementedError("Subclass must implement queryEvents!")

    def initResponse(self):
        """
        Initialize a response dictionary.
        :returns: Initialized respnse
        :rtype: dict
        """
        return {"statuses" : []}

    def addStatusToResponse(self, response, timestamp, event, status, details):
        """
        Adds a status to the response dictionary.
        :param response: Status response object to which to add a new status
        :type response: dict
        :param event: Event for which status is being reported
        :type event: str
        :param status: Resultant status
        :type status: int
        :param details: Other information to be stored about the event
        :type details: dict
        """
        status_dict = {
            "timestamp" : timestamp,
            "event" : event,
            "status" : status,
            "details" : json.loads(details)
        }

        response["statuses"].append(status_dict)

class BackendFile(MonitorBackend):
    """
    Monitor data I/O to a regular file
    """
    OUTPUT_FILE = "/var/adm/sm/monitoring.log"

    def __init__(self):
        super(BackendFile, self).__init__(False)

    def reportEvent(self, eventName, eventStatus, otherInfo):
        """
        Add an entry for a tracked event
        :param eventName: Event for which status is being reported
        :type eventName: str
        :param result: Resultant status
        :type result: int
        :param otherInfo: Other information to be stored about the event
        :type otherInfo: dict
        """
        msg = "{0} ; {1} ; {2} ; {3}\n"
        reportString = msg.format(str(datetime.now()), eventName, eventStatus, otherInfo)
        exclusiveWrite(BackendFile.OUTPUT_FILE, reportString)


    def queryEvents(self, eventName=None, startTime=None):
        """
        Query entries for tracked event(s)

        Return Value Format:
            {
                "statuses": [
                    {
                        "timestamp": "2016-11-02 18:11:08"
                        "event": "Console"
                        "status": 0
                        "details": {}
                    }
                ]
            }

        :param eventName: Specific event to return; All events if None (default)
        :type eventName: str
        :param startTime: Return events after the specified time.
        :type startTime: str Default: None
        :returns: events
        :rtype: Dictionary
        """
        lines = []

        if startTime != None:
            log.warning("startTime argument is not supported for this BackendFile, ignoring.")

        with open(BackendFile.OUTPUT_FILE, 'r') as infile:
            contents = infile.read()

            if eventName is not None:
                pattern = ".*" + eventName + ".*"
                lines = re.findall(pattern, contents, re.MULTILINE)
            else:
                lines = contents.split("\n")

        response = self.initResponse()

        for line in lines:
            if line.find(';') >= 0:
                row = line.split(" ; ")
                self.addStatusToResponse(response, row[0], row[1], row[2], row[3])

        return response


@ClassLogger
class BackendSMLog(MonitorBackend):
    """
    Monitor data I/O to the log
    """
    def __init__(self):
        super(BackendSMLog, self).__init__()

    def reportEvent(self, eventName, eventStatus, otherInfo):
        """
        Add an entry for a tracked event
        :param eventName: Event for which status is being reported
        :type eventName: str
        :param result: Resultant status
        :type result: int
        :param otherInfo: Other information to be stored about the event
        :type otherInfo: dict
        """
        msg = "Monitor event status: {0}, {1}, {2}\n"
        reportString = msg.format(eventName, eventStatus, otherInfo)
        self.log.info(reportString)

    def queryEvents(self, eventName=None, startTime=None):
        """
        Query entries for tracked event(s)

        Return Value Format:
            {
                "statuses": [
                    {
                        "timestamp": "2016-11-02 18:11:08"
                        "event": "Console"
                        "status": 0
                        "details": {}
                    }
                ]
            }

        :param eventName: Specific event to return; All events if None (default)
        :type eventName: str
        :param startTime: Return events after the specified time.
        :type startTime: str
        :returns: events
        :rtype: Dictionary
        """
        return []

@ClassLogger
class EtcdBackend(MonitorBackend):
    """
    Monitoring data backend for etcd
    """
    def __init__(self):
        super(EtcdBackend, self).__init__(False)

    def reportEvent(self, eventName, eventStatus, otherInfo):
        """
        Add an entry for a tracked event
        :param eventName: Event for which status is being reported
        :type eventName: str
        :param result: Resultant status
        :type result: int
        :param otherInfo: Other information to be stored about the event
        :type otherInfo: dict
        """
        status = MonitoringStatus(eventName, eventStatus, otherInfo)
        eventKey = "/".join(["/monitoring", status.timestamp.toISOFormattedString()])
        Backend().keyValueStore.put(eventKey, status.toJSON(includeClassInfo=True))

    def queryEvents(self, eventName=None, startTime=None):
        """
        Query entries for tracked event(s)
        Return Value Format:
            {
                "statuses": [
                    {
                        "timestamp": "2016-11-02 18:11:08"
                        "event": "Console"
                        "status": 0
                        "details": {}
                    }
                ]
            }
        :param eventName: Specific event to return; All events if None (default)
        :type eventName: str
        :param startTime: Return events after the specified time.
        :type startTime: str Default: None
        :returns: events
        :rtype: Dictionary
        """
        datetimeFormat = "%Y-%m-%d %H:%M:%S"
        pairs = Backend().keyValueStore.getPrefix("/monitoring")
        if startTime:
            try:
                startDateTime = datetime.strptime(startTime, datetimeFormat).isoformat("T")
                filteredStatuses = [pair[1] for pair in pairs if pair[0].split("/")[2] >= startDateTime]
            except ValueError, e:
                self.log.error("Couldn't parse startTime! %s", e)
                return {}
        else:
            filteredStatuses = [pair[1] for pair in pairs]

        response = self.initResponse()

        for jsonStatus in filteredStatuses:
            status = MonitoringStatus.fromJSON(jsonStatus)
            self.addStatusToResponse(response, str(status.timestamp), status.event, status.eventStatus, status.otherInfo)

        return response

@ClassLogger
class Monitor(object):
    """
    Monitor class to report status of component recovery success/failure
    """
    SUCCESS = 0
    FAILURE = 1
    NA = 2

    backends = None
    preferredBackendForQuery = None

    @classmethod
    def configureBackends(cls):
        """
        Get which MonitorBackends will be supported from the configuration and
        add them to the list of backends.
        """
        settings = Backend().configuration.getPlatform().settings
        backendNames = settings.get("monitor_backends", [])

        cls.backends = []

        for backendName in backendNames:
            backend = None
            if backendName == 'etcd':
                backend = EtcdBackend()
                # If enabled, etcd is the preferred query backend
                cls.preferredBackendForQuery = backend
            elif backendName == 'smlog':
                backend = BackendSMLog()
            elif backendName == 'file':
                backend = BackendFile()

            if backend is not None:
                cls.backends.append(backend)
                # pick the first available query backend
                if not backend.isWriteOnly and cls.preferredBackendForQuery is None:
                    cls.preferredBackendForQuery = backend

    @classmethod
    def queryEvents(cls, eventName=None, startTime=None):
        """
        Return event statuses from the preferred backend
        :param eventName: Optional specifier for a specific event to query
        :type eventName: str Default: None
        :param startTime: Return events after the specified time.
        :type startTime: str Default: None
        :returns: events
        :rtype: Dictionary
        """
        if cls.backends is None:
            cls.configureBackends()

        if cls.preferredBackendForQuery is not None:
            return cls.preferredBackendForQuery.queryEvents(eventName=eventName, startTime=startTime)
        return {}


    def __init__(self, event):
        self.event = event


    def report(self, result=None, otherInfo={}):
        """
        Add an entry for a tracked event
        :param result: Resultant status Default: None
        :type result: ActionStatus, Monitor.SUCCESS, Monitor.FAILURE, or Monitor.NA Default: None
        :param otherInfo: Other information to be stored about the event. Default: {}
        :type otherInfo: dict
        """
        if Monitor.backends is None:
            Monitor.configureBackends()

        otherInfoJson = json.dumps(otherInfo)

        for backend in Monitor.backends:
            backend.reportEvent(self.event, result, otherInfoJson)

def ClassMonitor(cls):
    """
    Class decorator that creates a per-class Monitor object
    """
    cls.monitor = Monitor(cls.__name__)
    return cls

class MonitoringStatus(JSONSerializable):
    """
    Monitoring event status
    :param event: Event for which status is being reported
    :type event: str
    :param eventStatus: Resultant status
    :type eventStatus: int
    :param otherInfo: Other information to be stored about the event
    :type otherInfo: dict
    """
    def __init__(self, event, eventStatus, otherInfo={}):
        super(MonitoringStatus, self).__init__()
        self.timestamp = Datetime.utcnow()
        self.event = event
        self.eventStatus = eventStatus
        self.otherInfo = otherInfo