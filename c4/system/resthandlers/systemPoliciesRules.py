import logging
import tornado.web

from c4.system import db
from c4.system.rest import BaseRequestHandler
from c4.messaging import sendMessage
from c4.system.messages import EnablePolicyRequest, DisablePolicyRequest

class SystemPoliciesRules(BaseRequestHandler):
    """
    Handles REST requests for describing policies.

    e.g. GET /system/policies/rules/cpu
    """
    URL_PATTERN = r"/system/policies/rules/" + BaseRequestHandler.DEVICE_PATTERN

    def get(self, policyId):
        """
        ..
            @api {get} /system/policies/rules/{policyId} Describe policies
            @apiName DescribePolicies
            @apiGroup Policies

            @apiSuccess {String} policies Policies
            @apiSuccess {String} policy  Name of policy
            @apiSuccess {String} policy.property Policy property
            @apiSuccessExample {json} Success-Response:
                HTTP/1.1 200 OK
                {
                    "cpu.policy": {
                        "representation": "",
                        "state": "ENABLED",
                        "type": "c4.system.policyEngine.PolicyComponent"
                    }
                }
        """

        if not self.logUser("Handling describe policies get request. Policy ID=%s", policyId):
            return

        rest_policy_list = self.parseComponentId(policyId)

        response = {}
        # FIXME: this should use the PolicyDatabase interface instead
        dbm = db.DBManager()
        query = "select a.name, a.representation, a.state, a.type, ifnull(b.name, '') as parent from t_sm_policies a left outer join t_sm_policies b on a.parent_id = b.id"
        # policy name(s) in list, describe each policy
        if "*" not in rest_policy_list and len(rest_policy_list) > 0:
            name_clause = ",".join("?" for policy_name in rest_policy_list)
            query = query + " where a.name in (" + name_clause + ")"
            rows = dbm.query(query, rest_policy_list)
        # * in list, so describe all policies
        else:
            rows = dbm.query(query)
        # for each policy
        for row in rows:
            name = row[0]
            representation = row[1]
            state = row[2]
            ptype = row[3]
            parent_name = row[4]
            policy_dict = {}
            policy_dict["representation"] = representation
            policy_dict["state"] = state
            policy_dict["type"] = ptype
            policy_dict["parent"] = parent_name
            response[name] = policy_dict
        dbm.close()
        self.write(response)


    @tornado.web.asynchronous
    def put(self, policyId):
        """
        Handles REST requests for policy action.
        e.g. PUT /system/policies/rules/cpu

        where "action" argument is either enable or disable.

        ..
            @api {put} /system/policies/rules/{policyId} Perform policy action
            @apiName PolicyAction
            @apiGroup Policies

            @apiParam (Query String) {String} policyId Policy id
            @apiParam (JSON) {String} action Action to be performed (enable or disable)
        """
        if not self.logUser("Handling policy action put request for policy: %s", policyId):
            return

        rest_policy_list = self.parseComponentId(policyId)

        action = self.get_argument("action")
        if action == "enable":
            message = EnablePolicyRequest(self.node + "/rest", rest_policy_list)
            # when the sysmgr responds, call getAck
            self.addHandler(self.getAck, message.MessageID)
            sendMessage(self.localSysmgrAddress, message)
        elif action == "disable":
            message = DisablePolicyRequest(self.node + "/rest", rest_policy_list)
            # when the sysmgr responds, call getAck
            self.addHandler(self.getAck, message.MessageID)
            sendMessage(self.localSysmgrAddress, message)
        else:
            self.send_error(status_code=400, c4_error_message="Invalid state: %s" % (action))
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