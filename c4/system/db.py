import datetime
import glob
import logging
import os
import sqlite3

import pkg_resources

from c4.utils.logutil import ClassLogger


log = logging.getLogger(__name__)

BACKUP_PATH = "/tmp"
"""Directory of the backups"""

DATABASE_PATH = "/dev/shm"
"""Directory of the database"""

@ClassLogger
class DBManager(object):
    """
    Simple database wrapper that shares a single database connection.
    """
    def __init__(self, dbName="sysmgr.db", enableNamedColumns=True):
        """
        Connects to the database.  If the database does not exist we check backup files
        and if there is a backup we automatically restore the latest, if not then a new
        database will be created with the schema file that is in the c4.data directory.

        :param dbName: name of the database.  Defaults to sysmgr.db
        :type dbName: str
        :param enableNamedColumns: enable named columns (returns rows as :class:`sqlite3.Row`)
        :type enableNamedColumns: bool
        """
        # if the database does not exist, then create it
        self.fullDBName = os.path.join(DATABASE_PATH, dbName)
        if not os.path.isfile(self.fullDBName):
            # look for backup files
            backupFiles = DBManager.getBackupFiles()
            if backupFiles:
                # restore latest backup
                self.restore(backupFiles.pop())
            else:
                DBManager.create(self.fullDBName)

        # connect to the database
        self.conn = sqlite3.connect(self.fullDBName)
        # turn off autocommit
        self.conn.isolation_level = None
        if enableNamedColumns:
            # allow columns to be accessed by name
            self.conn.row_factory = sqlite3.Row

    @classmethod
    def create(cls, dbName="sysmgr.db", overwrite=False):
        """
        Creates the sysmgr.db database if it does not already exist.

        :param dbName: name of the database.  Defaults to sysmgr.db
        :type dbName: str
        :param overwrite: If overwrite is True and the database exists, then it will be deleted.  Defaults to False.
        :type overwrite: bool
        :returns: True if successful, False if not
        """
        # if the database exists
        fullDBName = os.path.join(DATABASE_PATH, dbName)
        if os.path.isfile(fullDBName):
            # if we are allowed to overwrite it, then delete it
            if overwrite:
                cls.log.warn("Database %s exists.  Deleting.", fullDBName)
                os.remove(fullDBName)
            # else error
            else:
                cls.log.error("Error creating database: %s.  Already exists.", fullDBName)
                return False
        # the schema file is in the data dir
        schema = pkg_resources.resource_string("c4.data", "sql/sysmgr.sql")  # @UndefinedVariable
        # create the database
        connection = sqlite3.connect(fullDBName)
        cursor = connection.cursor()
        cursor.executescript(schema)
        return True

    @classmethod
    def getBackupFiles(cls, backupPath=None):
        """
        Get backup files

        :param backupPath: backup path
        :type backupPath: str
        :returns: list of backup files sorted by date (from earliest to latest)
        :rtype: [str]
        """
        backupPath = backupPath or BACKUP_PATH
        return sorted(glob.glob(os.path.join(backupPath, "sysmgr.db_backup-*")))

    def backup(self, backupPath=None):
        """
        Perform a backup of the stored data

        :param backupPath: backup path
        :type backupPath: str
        :returns: backup file name
        :rtype: str
        """
        try:
            start = datetime.datetime.utcnow()
            backupPath = backupPath or BACKUP_PATH
            backupFileName = os.path.join(backupPath, "sysmgr.db_backup-{:%Y-%m-%dT%H:%M:%SZ}".format(start))
            with open(backupFileName, "w") as f:
                batch = []
                counter = 0

                dumpIterator = self.conn.iterdump()

                # get BEGIN TRANSACTION part
                batch.append(dumpIterator.next())

                # add table drops
                rows = self.query("""
                    select name from sqlite_master
                    where sql not null and type is 'table'
                    order by name""")
                for row in rows:
                    batch.append("DROP TABLE IF EXISTS {0};".format(row["name"]))

                # continue with create and insert statements
                for line in dumpIterator:
                    batch.append(line)
                    counter += 1
                    if counter % 500 == 0:
                        f.write("\n".join(batch))
                        f.write("\n")
                        batch[:] = []

                if batch:
                    f.write("\n".join(batch))
                    f.write("\n")
                    batch[:] = []

            end = datetime.datetime.utcnow()
            self.log.debug("Backing up database to %s took %s", backupFileName, end-start)
            return backupFileName
        except Exception as e:
            self.log.error("Could perform backup %s", e)

    def close(self):
        """
        Close the connection to the database
        """
        self.conn.close()

    def restore(self, backupFileName):
        """
        Restore backed up data from the specified backup file

        :param backupFileName: name of a backup file
        :type backupFileName: str
        :returns: ``True`` if successful, ``False`` if not
        :rtype: bool
        """
        if not os.path.exists(backupFileName):
            self.log.error("Backup file '%s' does not exist", backupFileName)
            return False
        start = datetime.datetime.utcnow()
        with open(backupFileName) as f:
            schema = f.read()
            connection = sqlite3.connect(self.fullDBName)
            cursor = connection.cursor()
            cursor.executescript(schema)
        end = datetime.datetime.utcnow()
        self.log.debug("Restoring database from %s took %s", backupFileName, end-start)
        return True

    def query(self, statement, *parameters):
        """
        Select query

        :param statement: select statement
        :type statement: str
        :param parameters: optional sql parameters
        :returns: rows
        """
        try:
            start = datetime.datetime.utcnow()
            cur = self.conn.cursor()
            cur.execute(statement, *parameters)
            rows = cur.fetchall()
            end = datetime.datetime.utcnow()
            self.log.debug("Executing sql query: %s %s took %s", statement, parameters, end-start)
        except sqlite3.Error as e:
            self.log.error("Could not execute sql query: %s %s, %s", statement, parameters, e)
            rows = []
        return rows

    def writeCommit(self, statement, *parameters):
        """
        Write to the database. Commits.

        :param statement: insert or update statement
        :type statement: str
        :param parameters: optional sql parameters
        :returns: affected rows
        :rtype: int
        """
        try:
            start = datetime.datetime.utcnow()
            cur = self.conn.cursor()
            cur.execute(statement, *parameters)
            self.conn.commit()
            end = datetime.datetime.utcnow()
            self.log.debug("Executing sql update/insert with commit: %s %s took %s", statement, parameters, end-start)
            return cur.rowcount
        except sqlite3.Error, message:
            self.log.error("Could not execute sql update/insert with commit: %s %s, %s", statement, parameters, message)

    def write(self, statement, *parameters):
        """
        Write to the database. Does not commit.
        Must start transaction with a write("begin") and
        end with a write("commit")

        :param statement: insert or update statement
        :type statement: str
        :param parameters: optional sql parameters
        :returns: affected rows
        :rtype: int
        """
        try:
            start = datetime.datetime.utcnow()
            cur = self.conn.cursor()
            cur.execute(statement, *parameters)
            end = datetime.datetime.utcnow()
            self.log.debug("Executing sql update/insert: %s %s took %s", statement, parameters, end-start)
            return cur.rowcount
        except sqlite3.Error, message:
            self.log.error("Could not execute sql update/insert: %s %s, %s", statement, parameters, message)

    # TODO: merge with write
    def writeMany(self, statement, *parameters):
        """
        Write to the database. Does not commit.
        Must start transaction with a write("begin") and
        end with a write("commit")

        :param statement: insert or update statement
        :type statement: str
        :param parameters: parameter sequences
        :returns: affected rows
        :rtype: int
        """
        try:
            start = datetime.datetime.utcnow()
            cur = self.conn.cursor()
            cur.executemany(statement, parameters)
            end = datetime.datetime.utcnow()
            if (self.log.isEnabledFor(logging.DEBUG)):
                self.log.debug("Executing sql update/insert: \n%s\n%s\ntook %s",
                               str.strip(statement), "\n".join(str(p) for p in parameters), end-start)
            return cur.rowcount
        except sqlite3.Error, message:
            self.log.error("Could not execute sql update/insert: %s %s, %s", statement, parameters, message)
