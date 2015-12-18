#!/usr/bin/env python

import logging

import c4.system.policyEngine

class And(c4.system.policyEngine.BinaryOperator):

    id = "and"

    def evaluateOperation(self, one, two):
        if isinstance(one, bool) and isinstance(two, bool):
            return one and two
        return False

class Not(c4.system.policyEngine.UnaryOperator):

    id = "not"

    def evaluateOperation(self, one):
        if isinstance(one, bool):
            return not one
        return False

class Or(c4.system.policyEngine.BinaryOperator):

    id = "or"

    def evaluateOperation(self, one, two):
        if isinstance(one, bool) and isinstance(two, bool):
            return one or two
        return False
