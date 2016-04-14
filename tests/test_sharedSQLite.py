import datetime
import logging
import os
import shutil
import sqlite3
import tempfile

import pytest

from c4.backends.sharedSQLite import (DBManager,
                                      SharedSqliteDBBackend)
from c4.system.backend import BackendInfo


log = logging.getLogger(__name__)

@pytest.fixture(scope="function")
def temporarySharedSQLiteBackend(request):

    newpath = tempfile.mkdtemp(dir="/dev/shm")
#     newpath = tempfile.mkdtemp(dir="/tmp")
    infoProperties = {
        "path.database": newpath,
        "path.backup": newpath
    }
    info = BackendInfo("c4.backends.sharedSQLite.SharedSqliteDBBackend", properties=infoProperties)
    testBackendImplementation = SharedSqliteDBBackend(info)

    def removeTemporaryDirectory():
        shutil.rmtree(newpath)
    request.addfinalizer(removeTemporaryDirectory)
    return testBackendImplementation

class TestDBManager():

    def test_automaticSQLSchema(self, temporarySharedSQLiteBackend):

        # test that DBManager will create a database with the sysmgr.sql schema
        # when the test database doesn't exist
        backend = temporarySharedSQLiteBackend

        rows = backend.database.query("select name from sqlite_master where type='table'")
        assert len(rows) > 0

        # test that a sysmgr table exists
        backend.database.write("insert into t_sm_platform (property, value) values ('foo', 'bar')")
        rows = backend.database.query("select * from t_sm_platform")
        backend.database.close()

        assert rows[0]["property"] == "foo"
        assert rows[0]["value"] == "bar"
        assert rows[0][0] == "foo"
        assert rows[0][1] == "bar"

    def test_backup(self, cleandir, temporarySharedSQLiteBackend):

        backend = temporarySharedSQLiteBackend

        numberOfRows = 10000

        backend.database.write("begin")
        for number in range(numberOfRows):

            node = "node"
            name = None

            testTime = datetime.datetime(2014, 1, 1)
            testTime += datetime.timedelta(days=number)
            timestamp = unicode("{:%Y-%m-%d %H:%M:%S}.{:03d}".format(testTime, testTime.microsecond // 1000))

            backend.database.write("""
                insert into t_sm_history (history_date, node, name)
                values (?, ?, ?)""",
                (timestamp, node, name))

        backend.database.write("commit")

        rows = backend.database.query("select * from t_sm_history")
        assert len(rows) == numberOfRows

        assert backend.database.backup(cleandir) is not None

    def test_create(self, temporarySharedSQLiteBackend):

        backend = temporarySharedSQLiteBackend
        # make sure we delete automatically create database
        os.remove(backend.database.fullDBName)
        backend.database.create(backend.database.databasePath)

        connection = sqlite3.connect(backend.database.databasePath + "/" + "sysmgr.db")
        cursor = connection.cursor()
        cursor.execute("select * from sqlite_master where type = 'table'")
        rows = cursor.fetchall()
        assert len(rows) > 0

    def test_disableNamedColumns(self, temporarySharedSQLiteBackend):

        backend = temporarySharedSQLiteBackend
        database = DBManager(backend.info, enableNamedColumns=False)
        database.write("insert into t_sm_platform (property, value) values ('foo', 'bar')")
        rows = database.query("select * from t_sm_platform")
        database.close()

        with pytest.raises(TypeError):
            assert rows[0]["property"] == "foo"
        with pytest.raises(TypeError):
            assert rows[0]["value"] == "bar"
        assert rows[0][0] == "foo"
        assert rows[0][1] == "bar"

    def test_historyDevicesInserts(self, temporarySharedSQLiteBackend):

        backend = temporarySharedSQLiteBackend

        nodes = 5
        devices = 5
        days = 5
        insertLatestCount = 0

        for nodeNumber in range(nodes):

            node = "node{0:03d}".format(nodeNumber)
            senderType = None

            for deviceNumber in range(devices):

                name = "name{0:03d}".format(deviceNumber)

                for day in range(days):

                    testTime = datetime.datetime(2014, 1, 1)
                    testTime += datetime.timedelta(days=day)
                    timestamp = unicode("{:%Y-%m-%d %H:%M:%S}.{:03d}".format(testTime, testTime.microsecond // 1000))

                    statusJSON = "status{0:03d}".format(day+1)

                    backend.database.write("begin")
                    backend.database.write("""
                        insert into t_sm_history (history_date, node, name)
                        values (?, ?, ?)""",
                        (timestamp, node, name))
                    # t_sm_latest holds the latest status
                    # the table never grows more than the total number of components
                    updated = backend.database.write("""
                        update t_sm_latest set details = ?
                        where node is ? and name is ?""",
                        (statusJSON, node, name))
                    if updated < 1:
                        insertLatestCount = insertLatestCount + 1
                        backend.database.write("""
                            insert into t_sm_latest (node, name, type, details)
                            values (?, ?, ?, ?)""",
                            (node, name, senderType, statusJSON))
                    backend.database.write("commit")

        rows = backend.database.query("select * from t_sm_history")
        assert len(rows) == nodes * devices * days

        assert insertLatestCount == nodes * devices
        rows = backend.database.query("select node, details from t_sm_latest order by node")
        assert len(rows) == nodes * devices
        for nodeNumber in range(nodes):
            for deviceNumber in range(devices):
                assert rows[nodeNumber * devices + deviceNumber]["node"] == "node{0:03d}".format(nodeNumber)
                assert rows[nodeNumber * devices + deviceNumber]["details"] == "status{0:03d}".format(days)

    def test_historyNodesInserts(self, temporarySharedSQLiteBackend):

        backend = temporarySharedSQLiteBackend

        nodes = 5
        days = 5
        insertLatestCount = 0

        for nodeNumber in range(nodes):

            node = "node{0:03d}".format(nodeNumber)
            name = None
            senderType = None

            for day in range(days):

                testTime = datetime.datetime(2014, 1, 1)
                testTime += datetime.timedelta(days=day)
                timestamp = unicode("{:%Y-%m-%d %H:%M:%S}.{:03d}".format(testTime, testTime.microsecond // 1000))

                statusJSON = "status{0:03d}".format(day+1)

                backend.database.write("begin")
                backend.database.write("""
                    insert into t_sm_history (history_date, node, name)
                    values (?, ?, ?)""",
                    (timestamp, node, name))
                # t_sm_latest holds the latest status
                # the table never grows more than the total number of components
                updated = backend.database.write("""
                    update t_sm_latest set details = ?
                    where node is ? and name is ?""",
                    (statusJSON, node, name))
                if updated < 1:
                    insertLatestCount = insertLatestCount + 1
                    backend.database.write("""
                        insert into t_sm_latest (node, name, type, details)
                        values (?, ?, ?, ?)""",
                        (node, name, senderType, statusJSON))
                backend.database.write("commit")

        rows = backend.database.query("select * from t_sm_history")
        assert len(rows) == nodes * days

        assert insertLatestCount == nodes
        rows = backend.database.query("select node, details from t_sm_latest order by node")
        assert len(rows) == nodes
        for nodeNumber in range(nodes):
            assert rows[nodeNumber]["node"] == "node{0:03d}".format(nodeNumber)
            assert rows[nodeNumber]["details"] == "status{0:03d}".format(days)

        # 2 x 2
        # /tmp journal:     nodes insert took 0:00:00.534776
        # /tmp wal:         nodes insert took 0:00:00.252447
        # /dev/shm journal: nodes insert took 0:00:00.004624

        # 5 x 5
        # /tmp journal:     nodes insert took 0:00:03.843103
        # /tmp wal:         nodes insert took 0:00:01.256525
        # /dev/shm journal: nodes insert took 0:00:00.018497

    def test_restore(self, temporarySharedSQLiteBackend):

        backend = temporarySharedSQLiteBackend

        numberOfRows = 10000

        backend.database.write("begin")
        for number in range(numberOfRows):

            node = "node"
            name = None

            testTime = datetime.datetime(2014, 1, 1)
            testTime += datetime.timedelta(days=number)
            timestamp = unicode("{:%Y-%m-%d %H:%M:%S}.{:03d}".format(testTime, testTime.microsecond // 1000))

            backend.database.write("""
                insert into t_sm_history (history_date, node, name)
                values (?, ?, ?)""",
                (timestamp, node, name))

        backend.database.write("commit")

        rows = backend.database.query("select * from t_sm_history")
        assert len(rows) == numberOfRows

        backupFileName = backend.database.backup()
        assert backupFileName
        assert backupFileName in DBManager.getBackupFiles(backend.database.backupPath)

        os.remove(backend.database.fullDBName)

        # automatic restore
        backend.database = DBManager(backend.info)
        rows = backend.database.query("select * from t_sm_history")
        assert len(rows) == numberOfRows

        # manual restore
        backend.database.restore(backupFileName)
        rows = backend.database.query("select * from t_sm_history")
        assert len(rows) == numberOfRows

    def test_write(self, temporarySharedSQLiteBackend):

        backend = temporarySharedSQLiteBackend
        backend.database.write("create table t_sm_comp (id integer)")
        backend.database.write("insert into t_sm_comp values(1)")
        backend.database.writeCommit("insert into t_sm_comp values(2)")
        rows = backend.database.query("select * from t_sm_comp where id = ?", (1,))
        assert len(rows) == 1
        rows = backend.database.query("select * from t_sm_comp where id = ?", (2,))
        assert len(rows) == 1
        backend.database.close()

        # /tmp journal
        # Executing sql update/insert: create table t_sm_comp (id integer) () took 0:00:00.148872
        # Executing sql update/insert: insert into t_sm_comp values(1) () took 0:00:00.151514
        # Executing sql update/insert with commit: insert into t_sm_comp values(2) () took 0:00:00.148222
        # Executing sql query: select * from t_sm_comp where id = ? ((1,),) took 0:00:00.000110
        # Executing sql query: select * from t_sm_comp where id = ? ((2,),) took 0:00:00.000035

        # /tmp wal
        # Executing sql update/insert: create table t_sm_comp (id integer) () took 0:00:00.094115
        # Executing sql update/insert: insert into t_sm_comp values(1) () took 0:00:00.049763
        # Executing sql update/insert with commit: insert into t_sm_comp values(2) () took 0:00:00.049643
        # Executing sql query: select * from t_sm_comp where id = ? ((1,),) took 0:00:00.000078
        # Executing sql query: select * from t_sm_comp where id = ? ((2,),) took 0:00:00.000017

        # /dev/shm journal
        # Executing sql update/insert: create table t_sm_comp (id integer) () took 0:00:00.000700
        # Executing sql update/insert: insert into t_sm_comp values(1) () took 0:00:00.000369
        # Executing sql update/insert with commit: insert into t_sm_comp values(2) () took 0:00:00.000348
        # Executing sql query: select * from t_sm_comp where id = ? ((1,),) took 0:00:00.000088
        # Executing sql query: select * from t_sm_comp where id = ? ((2,),) took 0:00:00.000049