#!/usr/bin/env python

import logging

from c4.system.policyEngine import Action

logging.basicConfig(format='%(asctime)s [%(levelname)s] <%(processName)s> [%(filename)s:%(lineno)d] - %(message)s', level=logging.INFO)

class Log(Action):

    id = "System.log"
    
    def perform(self, string):
        logging.error("'%s' '%s'", self.id, string)
        return True

