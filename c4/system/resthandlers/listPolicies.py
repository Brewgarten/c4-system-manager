import logging

from c4.system import db
from c4.system.rest import BaseRequestHandler

class ListPolicies(BaseRequestHandler):
    """
    Handles REST requests for listing policies.

    e.g. GET /system/policies/rules
    """
    URL_PATTERN = r"/system/policies/rules"

    def get(self):
        """
        ..
            @api {get} /system/policies/rules List policies
            @apiName ListPolicies
            @apiGroup Policies

            @apiSuccess {String} policies Policies
            @apiSuccess {String} policy  Name of policy
            @apiSuccessExample {json} Success-Response:
                HTTP/1.1 200 OK
                {
                    "policies": [
                        "policy1",
                        "policy2"
                    ]
                }
        """

        if not self.logUser("Handling listing policies get request"):
            return

        response = {}
        policy_list = []
        response["policies"] = policy_list
        # FIXME: this should use the PolicyDatabase interface instead
        dbm = db.DBManager()
        rows = dbm.query("select name from t_sm_policies")
        for row in rows:
            name = row[0]
            policy_list.append(name)
        dbm.close()
        self.write(response)

