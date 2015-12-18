import logging

from c4.system import db
from c4.system.configuration import Configuration
from c4.system.rest import BaseRequestHandler

class SoftwareVersion(BaseRequestHandler):
    """
    Handles REST requests for software attributes (e.g. version) per node(s).
    e.g. GET /nodes/localhost/software/system-manager
    """
    URL_PATTERN = r"/nodes/" + BaseRequestHandler.ALL_COMPONENT_PATTERN + "/software/" + BaseRequestHandler.DEVICE_PATTERN

    def get(self, nodeId, softwareId):
        """
        ..
            @api {get} /nodes/{nodeId}/software/{deviceId} Get device software version
            @apiName GetSoftwareVersion
            @apiGroup Software

            @apiParam (Query String) {String} nodeId Node id
            @apiParam (Query String) {String} deviceId Device id

            @apiSuccess {String}  device fully qualified device id
            @apiSuccess {String}  device.version version
            @apiSuccessExample {json} Success-Response:
                HTTP/1.1 200 OK
                {
                    "localhost/swap": {
                        "version": "0.1.0.0"
                    },
                    "localhost/cpu": {
                        "version": "0.1.0.0"
                    },
                    "localhost/memory": {
                        "version": "0.1.0.0"
                    },
                    "localhost/system-manager": {
                        "version": "14.11.5.1"
                    }
                }
        """
        if not self.logUser("Handling software attribute get request for node: %s, software: %s", nodeId, softwareId):
            return

        rest_node_list = self.parseComponentId(nodeId)
        rest_software_list = self.parseComponentId(softwareId)

        # FIXME: all dependent code should probably use the configuration directly
        configurationInfo = Configuration().toInfo()
        try:
            node_list = self.validateNodeListAndTransformWildcards(rest_node_list, configurationInfo)
            software_list = self.validateSoftwareListAndTransformWildcards(rest_software_list, node_list, configurationInfo)
        except Exception, e:
            self.send_error(status_code=400, c4_error_message=str(e))
            return

        response = {}

        # TODO: this should probably be in the separate Version interface
        dbm = db.DBManager()
        # add a question mark for each node and software
        sql = "select node, name, version from t_sm_version where node in ({nodes}) and name in ({software})".\
                    format(nodes=", ".join(["?"] * len(node_list)),
                           software=", ".join(["?"] * len(software_list)))
        rows = dbm.query(sql, node_list + software_list)
        for row in rows:
            node = row[0]
            name = row[1]
            version = row[2]
            response[node + "/" + name] = {"version": version}
        dbm.close()
        self.write(response)


    def validateSoftwareListAndTransformWildcards(self, rest_software_list, node_list, configuration):
        """
        Validate that the software in the rest software list belongs to at least one of the given nodes in
        the node list.

        Assume node_list has already been validated and wildcards transformed.
        """

        # handle wildcard *
        software_list = []
        if "*" in rest_software_list:
            def getAllDevices(parent_prefix, node_or_device, software_list):
                for device_name, device_info in node_or_device.devices.iteritems():
                    full_device_name = parent_prefix + device_name
                    if full_device_name not in software_list:
                        software_list.append(full_device_name)
                    getAllDevices(full_device_name + ".", device_info, software_list)

            # add all devices from all given nodes to the software list
            for node_name, node in configuration.nodes.iteritems():
                if node_name in node_list:
                    getAllDevices("", node, software_list)
            # also add system-manager as software
            software_list.append("system-manager")
            return software_list

        # else
        # check each software to see if it is system-manager
        # or if software is one of the devices
        for node_name, node in configuration.nodes.iteritems():
            if node_name in node_list:
                all_devices_for_this_node = node.devices.keys()
                for software in rest_software_list:
                    if software == "system-manager":
                        # append software if we haven't already
                        if software not in software_list:
                            software_list.append(software)
                    # else check if software exists on one of the nodes as a device
                    else:
                        software_parts = software.split(".")
                        # software does not exist on this node
                        if software_parts[0] not in all_devices_for_this_node:
                            raise Exception("Software named " + software + " does not exist on node " + node_name + ".")
                        device_info = node.devices[software_parts[0]]
                        software_parts.pop(0)
                        for software_part in software_parts:
                            if software_part not in device_info.devices:
                                raise Exception("Software named " + software + " does not exist on node " + node_name + ".")
                            device_info = device_info.devices[software_part]
                        # at this point software matches a device
                        # append software if we haven't already
                        if software not in software_list:
                            software_list.append(software)
        return software_list