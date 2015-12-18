
from c4.system.resthandlers.eggBase import _DeployUndeployEggBase
from c4.system.messages import DeployPolicy, UndeployPolicy

class SystemPolicies(_DeployUndeployEggBase):
    """
    Handles REST requests for deploying and undeploying policies, actions, and events.

    e.g. POST /system/policies

    Post arguments are "type" and "data".  Type is the dotted module names for the custom
    policy class.  e.g. c4.system.policies.mypolicy.MyPolicy
    Data is the base64 encoded egg file.

    e.g. DELETE /system/policies
    
    Delete arguments are "type".

    Sends message to the active system manager, does not wait for a response.
    """
    URL_PATTERN = r"/system/policies"
    
    def getDeployMessageObject(self):
        """
        Supplying the DeployPolicy message for SystemPolicies
        """
        return DeployPolicy(self.node + "/rest", "system-manager")


    def getUndeployMessageObject(self):
        """
        Supplying the UndeployPolicy message for SystemPolicies
        """
        return UndeployPolicy(self.node + "/rest", "system-manager")
