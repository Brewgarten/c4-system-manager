import abc
import logging

from c4.system.rest import BaseRequestHandler
from c4.messaging import sendMessage

class _DeployUndeployEggBase(BaseRequestHandler):
    """
    Handles REST requests for deploying and undeploying eggs.

    e.g. POST /system/devices or DELETE /system/devices

    Post arguments are "type" and "data".  Type is the dotted module names for the custom
    class.  e.g. c4.system.devices.mydm.MyDM
    Data is the base64 encoded egg file.

    Delete argument is "type".

    Sends message to the active system manager, does not wait for a response.

    Notice that the name of this class starts with an underscore.  This is
    special syntax that is understood by the rest.RestServer.loadHandlers method.
    It means that we don't want this class executed by the REST server.
    """
    def post(self):
        if not self.logUser("Handling egg post request"):
            return

        restResponse = {}
        restResponse["response"] = "sent"

        # get the post arguments
        type = self.get_argument("type")
        data = self.get_argument("data")
        file_name = self.get_argument("file_name")

        # validate arguments
        if not self._typeOK(type):
            logging.warning("Unsupported type %s", type)
            self.send_error(status_code=400, c4_error_message="Unsupported type %s" % type)
            return
        if not self._fileNameOK(file_name):
            logging.warning("Invalid file name %s", file_name)
            self.send_error(status_code=400, c4_error_message="Invalid file name %s" % file_name)
            return

        # for debugging only
#        restResponse["type"] = type
#        restResponse["data"] = data
#        restResponse["file_name"] = file_name

        # send a message to the active sysmgr
        # note: we are not expecting a response
        message = self.getDeployMessageObject()
        message.Message["type"] = type
        # base64 encoded egg file
        message.Message["data"] = data
        message.Message["file_name"] = file_name
        sendMessage(self.localSysmgrAddress, message)

        self.write(restResponse)


    def delete(self):
        if not self.logUser("Handling egg delete request"):
            return
        restResponse = {}
        restResponse["response"] = "sent"

        # get arguments
        type = self.get_argument("type")

        # validate arguments
        if not self._typeOK(type):
            logging.warning("Unsupported type %s", type)
            self.send_error(status_code=400, c4_error_message="Unsupported type %s" % type)
            return

        # send a message to the active sysmgr
        # note: we are not expecting a response
        message = self.getUndeployMessageObject()
        message.Message["type"] = type
        sendMessage(self.localSysmgrAddress, message)

        self.write(restResponse)


    @abc.abstractmethod
    def getDeployMessageObject(self):
        """
        Derived class must override this and provide the correct message to use.
        Either DeployDeviceManager or DeployPolicy
        """
        return None


    @classmethod
    def _typeOK(cls, type):
        """
        Validates that type starts with c4.system.devices
        """
        if type.startswith("c4.system.devices") or \
           type.startswith("c4.system.policies") or \
           type.startswith("c4.system.events") or \
           type.startswith("c4.system.actions"):
            return True
        return False


    @classmethod
    def _fileNameOK(cls, file_name):
        """
        Validates that the file name has a version.  e.g. MyCustomDM-0.1-py2.7.egg
        """
        if not file_name.endswith(".egg"):
            return False
        file_name_parts = file_name.split("-")
        part_len = len(file_name_parts)
        if part_len < 3:
            return False
        # validate version
        version = file_name_parts[part_len - 2]
        for version_part in version.split("."):
            if not version_part.isdigit():
                return False
        # validate py2.7
        if not file_name_parts[part_len - 1].startswith("py"):
            return False
        return True


