import logging

from c4.system import db
from c4.system.resthandlers.eggBase import _DeployUndeployEggBase
from c4.system.messages import DeployDeviceManager, UndeployDeviceManager

class SystemDevices(_DeployUndeployEggBase):
    """
    Handles REST requests for deploying and undeploying device managers.
    And listing all DEPLOYED device managers.

    e.g. POST /system/devices

    Post arguments are "type" and "data".  Type is the dotted module names for the custom
    device manager class.  e.g. c4.system.devices.mydm.MyDM
    Data is the base64 encoded egg file.

    e.g. DELETE /system/devices

    Delete arguments are "type".

    e.g. GET /system/devices

    Sends message to the active system manager, does not wait for a response.
    """
    URL_PATTERN = r"/system/devices"

    def getDeployMessageObject(self):
        """
        Supplying the DeployDeviceManager message for SystemDevices
        """
        return DeployDeviceManager(self.node + "/rest", "system-manager")


    def getUndeployMessageObject(self):
        """
        Supplying the UndeployDeviceManager message for SystemDevices
        """
        return UndeployDeviceManager(self.node + "/rest", "system-manager")

    def get(self):
        """
        ..
            @api {get} /system/devices List all DEPLOYED devices
            @apiName ListDeployedDevices
            @apiGroup System

            @apiSuccessExample {json} Success-Response:
                HTTP/1.1 200 OK
                {
                    "c4.system.devices.cpu.Cpu": {
                        "version": "0.1.0.0"
                    },
                    "c4.system.devices.disk.Disk": {
                        "version": "0.1.0.0"
                    },
                    "c4.system.devices.mem.Memory": {
                        "version": "0.1.0.0"
                    }
                }
        """
        if not self.logUser("Handling list deployed devices get request"):
            return

        response = {}

        # TODO: this should probably be in the separate Version interface
        dbm = db.DBManager()
        # get DEPLOYED devices
        # regardless of whether or not the devices are in the configuration
        # the DEPLOYED devices should be the same on all of the nodes, so
        # we just grab the DEPLOYED devices on the active node
        sql = "select type, version from t_sm_version where node = ? and name == ''"
        rows = dbm.query(sql, (self.node,))
        for row in rows:
            deviceType = row[0]
            version = row[1]
            response[deviceType] = {"version": version}
        dbm.close()
        self.write(response)
