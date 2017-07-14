import mock
import os
import tempfile
import pytest
from datetime import datetime, timedelta

from c4.backends.etcdBackend import EtcdBackend #@UnusedImport
from c4.system.monitoring import (Monitor,
                                  BackendFile,
                                  BackendSMLog,
                                  ClassMonitor,
                                  EtcdBackend as EtcdMonitor)
import logging
log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

class TestBackendFile(object):
    @pytest.fixture
    def backendFile(self):
        return BackendFile()

    def test_reportEvent(self, backendFile):
        BackendFile.OUTPUT_FILE = tempfile.NamedTemporaryFile(delete=False).name
        with open(BackendFile.OUTPUT_FILE, "w") as logfile:
            logfile.truncate()

        backendFile.reportEvent("testEvent", Monitor.SUCCESS, {})
        with open(BackendFile.OUTPUT_FILE, "r") as logfile:
            line = logfile.readline()
        date = line.split(";")[0]
        os.remove(BackendFile.OUTPUT_FILE)
        assert line == date + "; testEvent ; 0 ; {}\n"

    def test_queryEvents_noEventName(self, backendFile):
        BackendFile.OUTPUT_FILE = tempfile.NamedTemporaryFile(delete=False).name
        with open(BackendFile.OUTPUT_FILE, "w") as logfile:
            logfile.truncate()

        backendFile.reportEvent("testEvent", Monitor.SUCCESS, {})
        with open(BackendFile.OUTPUT_FILE, "r") as logfile:
            line = logfile.readline()
        date = line.split(" ;")[0]
        events = backendFile.queryEvents()
        os.remove(BackendFile.OUTPUT_FILE)
        assert events == {"statuses": [{"status": "0", "timestamp": date, "event": "testEvent", "details": {}}]}

    def test_queryEvents_withEventName(self, backendFile):
        BackendFile.OUTPUT_FILE = tempfile.NamedTemporaryFile(delete=False).name
        with open(BackendFile.OUTPUT_FILE, "w") as logfile:
            logfile.truncate()

        backendFile.reportEvent("testEvent1", Monitor.FAILURE, {})
        backendFile.reportEvent("testEvent2", Monitor.SUCCESS, {})
        with open(backendFile.OUTPUT_FILE, "r") as logfile:
            line = logfile.readline()
        date = line.split(" ;")[0]
        events = backendFile.queryEvents("testEvent1")
        os.remove(BackendFile.OUTPUT_FILE)
        assert events == {"statuses": [{"status": "1", "timestamp": date, "event": "testEvent1", "details": {}}]}

class TestBackendSMLog(object):
    @pytest.fixture
    def backendSMLog(self):
        return BackendSMLog()

    def test_reportEvent(self, backendSMLog):
        backendSMLog.reportEvent("testEvent", Monitor.SUCCESS, {})
        #We"re really just making sure we don"t error out here. Nothing to check when we are only logging.

    def test_queryEvents(self, backendSMLog):
        assert backendSMLog.queryEvents() == []

class MockPlatform(object):
    settings = { "monitor_backends" : [ "file", "smlog", "etcd" ] }

class MockConfiguration(object):
    def getPlatform(self):
        return MockPlatform()

class TestMonitor(object):
    @pytest.fixture
    def monitor(self):
        return Monitor(__name__)

    @pytest.fixture
    def monitorNoDB(self):
        monitor = Monitor(__name__)
        monitor.backends = [BackendFile()]
        monitor.preferredBackendForQuery = monitor.backends[0]
        return monitor

    def test_configureBackends(self, backend, monitor):
        # Don't run this for other backends
        if not isinstance(backend, EtcdBackend):
            log.info("Skipping because backend is not instance of EtcdBackend")
            return
        with mock.patch("c4.backends.etcdBackend.EtcdConfiguration.getPlatform", return_value=MockPlatform()):
            monitor.configureBackends()
            assert len(monitor.backends) == len(MockPlatform.settings["monitor_backends"])
            for backend in monitor.backends:
                assert type(backend) in [BackendFile, BackendSMLog, EtcdMonitor]

    def test_reportEventAndQueryEvents(self, monitor, backend):
        # Don't run this for other backends
        if not isinstance(backend, EtcdBackend):
            log.info("Skipping because backend is not instance of EtcdBackend")
            return
        with mock.patch("c4.system.monitoring.Backend", return_value=backend):
            BackendFile.OUTPUT_FILE = tempfile.NamedTemporaryFile(delete=False).name
            with open(BackendFile.OUTPUT_FILE, "w") as logfile:
                logfile.truncate()

            datetimeFormat = "%Y-%m-%d %H:%M:%S"
            beforeTime = datetime.utcnow()
            afterTime = beforeTime + timedelta(days=1)
            monitor.report(Monitor.SUCCESS, {})

            # query events with no startTime specified
            allEvents = monitor.queryEvents()
            assert len(allEvents["statuses"]) == 1

            # startTime before recorded event
            eventsBefore = monitor.queryEvents(startTime=beforeTime.strftime(datetimeFormat))
            assert len(eventsBefore["statuses"]) == 1

            # startTime one day after recorded event
            eventsAfter = monitor.queryEvents(startTime=afterTime.strftime(datetimeFormat))
            assert len(eventsAfter["statuses"]) == 0

@ClassMonitor
class TestClassMonitor(object):
    def test_ClassMonitor(self):
        assert type(self.monitor) == Monitor
        assert self.monitor.event == self.__class__.__name__
