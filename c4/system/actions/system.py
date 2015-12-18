import json

from c4.messaging import sendMessage
from c4.system.db import DBManager
from c4.system.policyEngine import Action
from c4.utils.logutil import ClassLogger

@ClassLogger
class SendMessage(Action):
    """
    Send specified message.
    """
    id = "system.sendMessage"

    def perform(self, msg):
        """
        Send message.

        :param msg: Message to send
        :type msg:  :class:`c4.system.messages.Envelope`
        """
        # FIXME: there should be a better way to contact the system manager
        database = DBManager()
        rows = database.query("select node_name from t_sm_configuration_alias where alias is ?", ("system-manager",))
        systemManagerNode = rows[0][0]
        rows = database.query("select details from t_sm_configuration where name is ?", (systemManagerNode,))
        database.close()
        systemManagerAddress = json.loads(rows[0][0])["address"]

        self.log.debug("sending message '%s' to %s", type(msg).__name__, msg.To)
        sendMessage(systemManagerAddress, msg)

        return True
