import logging

from c4.system import db

class Version(object):
    
    @classmethod
    def clearVersion(cls):
        dbm = db.DBManager()
        dbm.writeCommit("delete from t_sm_version")
        dbm.close()
        
        
    @classmethod
    def saveVersion(cls, node, name, deviceType, version):
        dbm = db.DBManager()
        dbm.writeCommit("insert or replace into t_sm_version (node, name, type, version) values (?, ?, ?, ?)",
                        (node, name, deviceType, version))
        dbm.close()


    @classmethod
    def deleteVersion(cls, node, name, deviceType):
        dbm = db.DBManager()
        dbm.writeCommit("delete from t_sm_version where node = ? and name = ? and type = ?",
                        (node, name, deviceType))
        dbm.close()
