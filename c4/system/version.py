"""
Copyright (c) IBM 2015-2017. All Rights Reserved.
Project name: c4-system-manager
This project is licensed under the MIT License, see LICENSE
"""
from c4.system.backend import Backend


class Version(object):

    @classmethod
    def clearVersion(cls):
        # FIXME: this only works with the shared SQLite backend right now
        backend = Backend()
        backend.database.writeCommit("delete from t_sm_version")

    @classmethod
    def saveVersion(cls, node, name, deviceType, version):
        # FIXME: this only works with the shared SQLite backend right now
        backend = Backend()
        backend.database.writeCommit("insert or replace into t_sm_version (node, name, type, version) values (?, ?, ?, ?)",
                        (node, name, deviceType, version))

    @classmethod
    def deleteVersion(cls, node, name, deviceType):
        # FIXME: this only works with the shared SQLite backend right now
        backend = Backend()
        backend.database.writeCommit("delete from t_sm_version where node = ? and name = ? and type = ?",
                        (node, name, deviceType))
