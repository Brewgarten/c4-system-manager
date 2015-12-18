import logging
import tornado.web

from c4.messaging import sendMessage
from c4.system import messages
from c4.system.configuration import Configuration
from c4.system.rest import BaseRequestHandler

class Nodes(BaseRequestHandler):
    URL_PATTERN = r"/nodes/" + BaseRequestHandler.ALL_COMPONENT_PATTERN

    @tornado.web.asynchronous
    def delete(self, nodeId):
        """
        Handles REST requests for unregistering nodes

        e.g. DELETE /nodes/localhost

        e.g. DELETE /nodes/[localhost, localhost2]

        ..
            @api {delete} /nodes/{nodeId} unregister node
            @apiName UnregisterNode
            @apiGroup Nodes

            @apiSuccess {String} response       Sent message
            @apiSuccessExample {json} Success-Response:
                HTTP/1.1 200 OK
                {
                    "response": "sent"
                }
        """
        if not self.logUser("Handling unregister request for node: %s", nodeId):
            return

        nodeList = self.parseComponentId(nodeId)
        logging.debug(nodeList)

        message = messages.UnregisterNodeRequest(self.node + "/rest", nodeList)
        # when the sysmgr responds, call writeNodes
        self.addHandler(self.writeNodes, message.MessageID)
        # tcp://127.0.0.1:5000"
        sendMessage(self.localSysmgrAddress, message)


    def writeNodes(self, envelope):
        logging.debug("writeNodes handler received envelope message: %s" % (envelope.Message))
        if "error" in envelope.Message:
            self.send_error(status_code=400, c4_error_message="%s" % (envelope.Message["error"]))
            return
        # handle response
        restResponse = envelope.Message
        self.write(restResponse)
        self.finish()


    def get(self, nodeId):
        """
        Handles REST requests for getting node status

        e.g. GET /nodes/localhost

        Outputs a dictionary that has the node name as the key
        and a value that is a dictionary that contains the node's status.

        ..
            @api {get} /nodes/{nodeId} Get node status
            @apiName GetNodeStatus
            @apiGroup Nodes

            @apiSuccess {String} node               Node name
            @apiSuccess {String} node.property      Node status property
            @apiSuccessExample {json} Success-Response:
                HTTP/1.1 200 OK
                {
                    "localhost": {
                        "state": "running",
                        "role": "active",
                        "address": "tcp://127.0.0.1:5000",
                        "devices": ["disk", "memory", "loadavg", "cpu", "swap"]
                    }
                }
        """
        if not self.logUser("Handling node status request"):
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
        response = {}
        for node_name in node_list:
            config_node = configurationInfo.nodes[node_name]
            node_status = {}
            response[node_name] = node_status
            # note: not including name since it is redundant
            # because the name is the key
            node_status["state"] = config_node.state.value
            node_status["role"] = config_node.role.value
            node_status["address"] = config_node.address
            node_status["devices"] = config_node.devices.keys()
        # node name is key and node status (dict) is value
        self.write(response)

    @tornado.web.asynchronous
    def put(self, nodeId):
        """
        Handles REST requests for device action per node(s).
        e.g. PUT /nodes/localhost

        where "action" argument is either enable, disable, start, or stop
        """
        if not self.logUser("Handling node action put request for node: %s", nodeId):
            return

        nodeList = self.parseComponentId(nodeId)

        action = self.get_argument("action")
        if action == "enable":
            enable_env = messages.EnableNodeRequest('{0}/rest'.format(self.node), nodeList)
            self.addHandler(self.getAck, enable_env.MessageID)
            sendMessage(self.localSysmgrAddress, enable_env)
        elif action == "disable":
            disable_env = messages.DisableNodeRequest('{0}/rest'.format(self.node), nodeList)
            self.addHandler(self.getAck, disable_env.MessageID)
            sendMessage(self.localSysmgrAddress, disable_env)
#         elif action == "stop":
#             stop_env = messages.StopNode('{0}/rest'.format(self.node), nodeList)
#             # when the sysmgr responds, call getAck
#             self.addHandler(self.getAck, stop_env.MessageID)
#             sendMessage(self.localSysmgrAddress, stop_env)
#         elif action == "start":
#             start_env = messages.StartNode('{0}/rest'.format(self.node), nodeList)
#             # when the sysmgr responds, call getAck
#             self.addHandler(self.getAck, start_env.MessageID)
#             sendMessage(self.localSysmgrAddress, start_env)
        else:
            self.send_error(status_code=400, c4_error_message="Invalid action: %s.  Should be enable, disable, start, or stop." % (action))
            return

    def getAck(self, envelope):
        logging.debug("getAck handler received envelope message: %s" % (envelope.Message))

        # handle an error response
        if "error" in envelope.Message:
            self.send_error(status_code=400, c4_error_message="%s" % (envelope.Message["error"]))
            return

        # handle ack response
        restResponse = envelope.Message
        self.write(restResponse)
        self.finish()
