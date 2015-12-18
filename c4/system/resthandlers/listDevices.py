import logging

from c4.system.configuration import Configuration
from c4.system.rest import BaseRequestHandler

class ListDevices(BaseRequestHandler):
    """
    Handles REST requests for listing devices per node(s).
    e.g. GET /nodes/localhost/devices
    """
    URL_PATTERN = r"/nodes/" + BaseRequestHandler.ALL_COMPONENT_PATTERN + "/devices"

    def get(self, nodeId):
        """
        ..
            @api {get} /nodes/{nodeId}/devices Get devices
            @apiName GetDevices
            @apiGroup Devices

            @apiParam (Query String) {String} nodeId Node id

            @apiSuccess {String} device Device
            @apiSuccess {String} state  State of the device
            @apiSuccessExample {json} Success-Response:
                HTTP/1.1 200 OK
                {
                    "node1": {
                        "devices": {
                            "memory": {
                                "state": "running",
                                "devices": {},
                                "type": "c4.system.devices.mem.Memory"
                            },
                            "db2": {
                                "state": "running",
                                "type": "c4.system.devices.db2.DB2",
                                "devices": {
                                    "mln1": {
                                        "state": "running",
                                        "devices": {},
                                        "type": "c4.system.devices.mln.MLN"
                                    }
                                }
                            }
                        }
                    }
                }
        """

        if not self.logUser("Handling listing devices get request for node: %s", nodeId):
            return

        rest_node_list = self.parseComponentId(nodeId)
        logging.debug(rest_node_list)
        # FIXME: all dependent code should probably use the configuration directly
        configurationInfo = Configuration().toInfo()
        try:
            node_list = self.validateNodeListAndTransformWildcards(rest_node_list, configurationInfo)
        except Exception, e:
            self.send_error(status_code=400, c4_error_message=str(e))
            return

        # for each node list the device name and state
        # in a hierarchy
        response = {}
        for node_name in node_list:
            config_node = configurationInfo.nodes[node_name]
            node_dict = {}
            node_dict["devices"] = {}
            response[node_name] = node_dict
            self._getDevicesAndStates(config_node, node_dict["devices"])
        self.write(response)

    def _getDevicesAndStates(self, nodeOrDevice, hierarchy_dict):
        for device in nodeOrDevice.devices.itervalues():
            device_dict = {}
            device_dict["state"] = device.state.value
            device_dict["type"] = device.type
            device_dict["devices"] = {}
            hierarchy_dict[device.name] = device_dict
            self._getDevicesAndStates(device, device_dict["devices"])