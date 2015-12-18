import logging

from c4.system.configuration import Configuration
from c4.system.rest import BaseRequestHandler

class ListNodes(BaseRequestHandler):
    """
    Handles REST requests for listing nodes
    e.g. GET /nodes
    """
    URL_PATTERN = r"/nodes"

    def get(self):
        """
        Outputs a dictionary that has a key called "nodes"
        and a value that is a list of nodes.

        ..
            @api {get} /nodes Get nodes
            @apiName GetNodes
            @apiGroup Nodes

            @apiSuccess {String[]} nodes       List of nodes
            @apiSuccessExample {json} Success-Response:
                HTTP/1.1 200 OK
                {
                    "nodes": ["localhost"]
                }
        """
        if not self.logUser("Handling listing nodes request"):
            return

        # get the list of nodes from the configuration
        configuration = Configuration()
        restResponse = {"nodes": configuration.getNodeNames()}
        self.write(restResponse)
