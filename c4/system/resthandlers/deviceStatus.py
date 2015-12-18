import json
import tornado.web

from c4.system.rest import BaseRequestHandler
from c4.messaging import sendMessage
from c4.system.messages import DisableDeviceManagerRequest, StopDeviceManagersRequest, EnableDeviceManagerRequest, \
    StartDeviceManagersRequest, UnregisterDeviceManagersRequest, RegisterDeviceManagerRequest
from c4.utils.logutil import ClassLogger

@ClassLogger
class DeviceStatus(BaseRequestHandler):
    URL_PATTERN = r"/nodes/" + BaseRequestHandler.ALL_COMPONENT_PATTERN + "/devices/" + BaseRequestHandler.DEVICE_PATTERN

    @tornado.web.asynchronous
    def delete(self, nodeId, deviceId):
        """
        Handles REST device unregister requests
        e.g. DELETE /nodes/localhost/devices/Cpu1

        ..
            @api {delete} /nodes/{nodeId}/devices/{deviceId} Unregister device
            @apiname UnregisterDeviceManager
            @apiGroup Devices

            @apiParam (Query String) {String} nodeId Node id
            @apiParam (Query String) {String} deviceId Device id
        """
        if not self.logUser("Handling unregister request for device '%s' on '%s'", deviceId, nodeId):
            return

        nodeList = self.parseComponentId(nodeId)
        deviceList = self.parseComponentId(deviceId)
        self.log.debug(nodeList)
        self.log.debug(deviceList)

        message = UnregisterDeviceManagersRequest(self.node + "/rest", nodeList, deviceList)
        # when the sysmgr responds, call writeNodes
        self.addHandler(self.getAck, message.MessageID)
        # tcp://127.0.0.1:5000"
        sendMessage(self.localSysmgrAddress, message)

    def get(self, nodeId, deviceId):
        """
        Handles REST requests for device status per node(s).
        e.g. GET /nodes/localhost/devices/Cpu1

        ..
            @api {get} /nodes/{nodeId}/devices/{deviceId} Get device status
            @apiName GetDeviceStatus
            @apiGroup Devices

            @apiParam (Query String) {String} nodeId Node id
            @apiParam (Query String) {String} deviceId Device id

            @apiSuccess {String}  node       Node id
            @apiSuccess {String}  node.device   Device id on node
            @apiSuccess {String}  node.device.property Device property
            @apiSuccessExample {json} Success-Response:
                HTTP/1.1 200 OK
                {
                    "node1": {
                        "memory": {
                            "total_memory": 15894740,
                            "used_memory": 3518780,
                            "free_memory": 9433760,
                            "buffer_memory": 136692,
                            "usage": 22.13801546926845,
                            "type": "c4.system.devices.mem.MemoryStatus",
                            "cached_memory": 2805508
                        },
                        "swap": {
                            "usage": 0,
                            "total": 8019964,
                            "type": "c4.system.devices.swap.SwapStatus",
                            "used": 0
                        },
                        "cpu": {
                            "usage": 9.242994562944375,
                            "iowait": 1.8402342116269343,
                            "type": "c4.system.devices.cpu.CPUStatus"
                        }
                    }
                }
        """
        if not self.logUser("Handling devices status get request for node: %s, device: %s", nodeId, deviceId):
            return

        nodeList = self.parseComponentId(nodeId)
        deviceList = self.parseComponentId(deviceId)

        status = self.getStatus(nodeList, deviceList)
        self.log.debug(status)

        if len(status) == 0:
            n = self.printComponentList(nodeList)
            d = self.printComponentList(deviceList)
            self.send_error(status_code=400, c4_error_message="Location not found nodes: %s devices: %s" % (n, d))
            return

        self.write(status)


    def getStatus(self, nodeList, nameList):
        """
        Get status from the t_sm_latest table in the database.

        :param nodeList: The list of nodes returned from parseComponentId(nodeId)
        :type nodeList: list
        :param nameList: The list of device names returned from parseComponentId(deviceId)
        :type nameList: list
        :returns: list of dictionary where dictionary contains the following keys: node, name, details
        """

        where_clause = ""
        params = []

        # add nodeList to where clause as sql parameters (i.e. ?)
        # skip empty list and all nodes
        if len(nodeList) > 0 and nodeList[0] != "*":
            node_clause = ",".join("?" for node in nodeList)
            where_clause += "node in (" + node_clause + ") "
            params += nodeList

        # add nameList to where clause as sql parameters (i.e. ?)
        # skip empty list and all nodes
        if len(nameList) > 0 and nameList[0] != "*":
            if where_clause != "":
                where_clause += "and "
            name_clause = ",".join("?" for name in nameList)
            where_clause += "name in (" + name_clause + ") "
            params += nameList

        # add where keyword if there is a where clause
        if where_clause != "":
            where_clause = " and " + where_clause

        # skip blank name which is the system manager
        rows = self.dbm.query("select node, name, type, details from t_sm_latest where name != ''" + where_clause, params)
        if len(rows) == 0:
            self.log.error("No row in t_sm_latest for node: %s name: %s", nodeList, nameList)
            return []
#        print rows
        status = {}

        # {
        #       "node1": {
        #                   "cpu": {
        #                       "used_percent": 3.2,
        #                       "io_wait": 0.013
        #                   },
        #                   "memory" {
        #                       "used_memory": 12000544,
        #                       "free_memory": 176555264
        #                   },
        #                   ...
        #       },
        #
        #       "node2" : {
        #           ...
        #       }
        #       ...
        # }

        # for each device
        for device_row in rows:
            device_status = {}
            node_name = device_row[0]
            device_name = device_row[1]
            device_type = device_row[2]
            raw_details = device_row[3]
            if raw_details is None:
                details = {}
            else:
                details = json.loads(raw_details)

            # each node is a dictionary key
            if node_name in status:
                devices = status[node_name]
            else:
                devices = {}
                status[node_name] = devices

            device_status = {}
            devices[device_name] = device_status

            # each type is a dictionary key
            device_status["type"] = device_type
            for name, value in details.iteritems():
                # the user doesn't need to know the class
                if name != "@class":
                    device_status[name] = value
                # also we need to remove the class from within the disk status
                if name == "disks" and device_type.startswith("c4.system.devices.disk"):
                    all_disk_status = value
                    for disk_status in all_disk_status.itervalues():
                        # get rid of class in disk status
                        if "@class" in disk_status:
                            del disk_status["@class"]
        return status


    @tornado.web.asynchronous
    def put(self, nodeId, deviceId):
        """
        Handles REST requests for device action per node(s).
        e.g. PUT /nodes/localhost/devices/Cpu1

        where "action" argument is either enable, disable, start, stop, or register.

        ..
            @api {put} /nodes/{nodeId}/devices/{deviceId} Perform device action
            @apiName PutDevice
            @apiGroup Devices

            @apiParam (Query String) {String} nodeId Node id
            @apiParam (Query String) {String} deviceId Device id
            @apiParam (JSON) {String} action Action to be performed (enable, disable, start, stop, or register)
            @apiParam (JSON) {String} type Device type to be registered (for register only)
        """
        if not self.logUser("Handling devices action put request for node: %s, device: %s", nodeId, deviceId):
            return

        nodeList = self.parseComponentId(nodeId)
        deviceList = self.parseComponentId(deviceId)

        action = self.get_argument("action")
        if action == "enable":
            message = EnableDeviceManagerRequest(self.node + "/rest", deviceList, nodeList)
            # when the sysmgr responds, call getAck
            self.addHandler(self.getAck, message.MessageID)
            sendMessage(self.localSysmgrAddress, message)
        elif action == "disable":
            message = DisableDeviceManagerRequest(self.node + "/rest", deviceList, nodeList)
            # when the sysmgr responds, call getAck
            self.addHandler(self.getAck, message.MessageID)
            sendMessage(self.localSysmgrAddress, message)
        elif action == "stop":
            message = StopDeviceManagersRequest(self.node + "/rest", deviceList, nodeList)
            # when the sysmgr responds, call getAck
            self.addHandler(self.getAck, message.MessageID)
            sendMessage(self.localSysmgrAddress, message)
        elif action == "start":
            message = StartDeviceManagersRequest(self.node + "/rest", deviceList, nodeList)
            # when the sysmgr responds, call getAck
            self.addHandler(self.getAck, message.MessageID)
            sendMessage(self.localSysmgrAddress, message)
        elif action == "register":
            deviceType = self.get_argument("type")
            message = RegisterDeviceManagerRequest(self.node + "/rest", deviceList, nodeList, deviceType)
            # when the sysmgr responds, call getAck
            self.addHandler(self.getAck, message.MessageID)
            sendMessage(self.localSysmgrAddress, message)
        else:
            self.send_error(status_code=400, c4_error_message="Invalid action: %s.  Should be enable or disable." % (action))
            return

    def getAck(self, envelope):
        self.log.debug("getAck handler received envelope message: %s" % (envelope.Message))

        # handle an error response
        if "error" in envelope.Message:
            self.send_error(status_code=400, c4_error_message="%s" % (envelope.Message["error"]))
            return

        # handle ack response
        restResponse = envelope.Message
        self.write(restResponse)
        self.finish()