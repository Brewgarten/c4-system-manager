import inspect
import logging
import traceback

from abc import ABCMeta, abstractmethod
from collections import OrderedDict
from datetime import datetime

import c4.utils.util
import c4.system.actions
import c4.system.events
import c4.system.policies

from c4.system.db import DBManager
from c4.utils.enum import Enum
from c4.utils.jsonutil import JSONSerializable
from c4.utils.logutil import ClassLogger
from c4.utils.util import callWithVariableArguments, getFullModuleName

log = logging.getLogger(__name__)

class States(Enum):
    """
    Enumeration of states
    """
    ENABLED = "enabled"
    DISABLED = "disabled"

@ClassLogger
class Event(object):
    """
    An event implementation
    """
    __metaclass__ = ABCMeta
    id = None

    def __init__(self):
        pass
        # TODO: type, group, severity, description
        # see http://www-01.ibm.com/support/knowledgecenter/SSULQD_7.1.0/com.ibm.nz.adm.doc/r_sysadm_template_event_rules.html

    @abstractmethod
    def evaluate(self):
        """
        Evaluate the event

        .. note::

            Subclasses should implement this

        :returns: value
        """
        return None

    @property
    def value(self):
        """
        Value of the event
        """
        return self.evaluate()

    def __repr__(self, *args, **kwargs):
        return "({0})".format(self.id)

    def __str__(self, *args, **kwargs):
        return "({0} -> {1})".format(self.id, self.evaluate())

@ClassLogger
class EventReference(Event):
    """
    A reference to an :class:`Event`

    :param event: event
    :type event: :class:`Event`
    :param arguments: arguments
    :param keyValueArguments: key value arguments
    """
    def __init__(self, event, arguments=None, keyValueArguments=None):
        self.event = event
        self.id = event.id
        if arguments is None:
            self.arguments = []
        else:
            self.arguments = arguments
        if keyValueArguments is None:
            self.keyValueArguments = {}
        else:
            self.keyValueArguments = keyValueArguments

    def evaluate(self):
        """
        Evaluate the specified event using the given
        arguments and key value arguments

        :returns: result
        """
        try:
            arguments = []
            for argument in self.arguments:
                if isinstance(argument, (EventReference, CachableEvent)):
                    arguments.append(argument.evaluate())
                elif isinstance(argument, Event):
                    raise ValueError("'{0}' needs to be an EventReference".format(repr(argument)))
                else:
                    arguments.append(argument)

            keyValueArguments = {}
            for key, value in self.keyValueArguments.items():
                if isinstance(value, (EventReference, CachableEvent)):
                    keyValueArguments[key] = value.evaluate()
                elif isinstance(value, Event):
                    raise ValueError("'{0}={1}' needs to be an EventReference".format(key, repr(value)))
                else:
                    keyValueArguments[key] = value

            return callWithVariableArguments(
                  self.event.evaluate, *arguments, **keyValueArguments)
        except Exception as e:
            self.log.error(self.event)
            self.log.error(e)
            self.log.error(traceback.format_exc())

    def __repr__(self, *args, **kwargs):
        return "({0}{1})".format(self.id,
                                 getFormattedArgumentString(self.arguments, self.keyValueArguments))

    def __str__(self, evaluatedValue=None, *args, **kwargs):
        # TODO: what about if the value is actually None?
        if evaluatedValue is None:
            evaluatedValue = self.evaluate()

        return "({0}{1} -> {2})".format(self.id,
                                        getFormattedArgumentString(self.arguments, self.keyValueArguments),
                                        evaluatedValue)

@ClassLogger
class Action(object):
    """
    An action implementation
    """
    __metaclass__ = ABCMeta
    id = None

    @abstractmethod
    def perform(self):
        """
        Perform specified action

        .. note::

            Subclasses should add arguments as needed

        :returns: result
        """
        return None

    def __repr__(self, *args, **kwargs):
        return "{0}(...)".format(self.id)

@ClassLogger
class ActionReference(Action):
    """
    A reference to an :class:`Action`

    :param action: action
    :type action: :class:`Action`
    :param arguments: arguments
    :param keyValueArguments: key value arguments
    """
    def __init__(self, action, arguments=None, keyValueArguments=None):
        self.action = action
        self.id = action.id
        if arguments is None:
            self.arguments = []
        else:
            self.arguments = arguments
        if keyValueArguments is None:
            self.keyValueArguments = {}
        else:
            self.keyValueArguments = keyValueArguments

    def perform(self):
        """
        Perform specified action using the given
        arguments and key value arguments

        :returns: result
        """
        try:
            return callWithVariableArguments(
                  self.action.perform, *self.arguments, **self.keyValueArguments)
        except Exception as e:
            self.log.error(self.action)
            self.log.error(e)
            self.log.error(traceback.format_exc())

    def __repr__(self, *args, **kwargs):
        return "{0}{1}".format(self.id, getFormattedArgumentString(self.arguments, self.keyValueArguments))

@ClassLogger
class BinaryOperator(Event):
    """
    A binary operator base class

    :param one: event one
    :type one: :class:`Event`
    :param two: event two
    :type two: :class:`Event`
    """
    __metaclass__ = ABCMeta
    id = "binaryOperator"

    def __init__(self, one, two):
        super(BinaryOperator, self).__init__()
        self.one = ValueEvent.create(one)
        self.two = ValueEvent.create(two)

    @abstractmethod
    def evaluateOperation(self, one, two):
        pass

    def evaluate(self):
        one = self.one.evaluate()
        two = self.two.evaluate()
        return self.evaluateOperation(one, two)

    def __repr__(self, *args, **kwargs):
        return "({0} {1} {2})".format(repr(self.one), self.id, repr(self.two))

    def __str__(self, *args, **kwargs):
        return "({0} {1} {2} -> {3})".format(self.one, self.id, self.two, self.evaluate())

@ClassLogger
class Cache(dict):
    """
    A memory-based dictionary cache
    """
    def __init__(self):
        super(Cache, self).__init__()
        self.enabled = True

@ClassLogger
class CachableEvent(Event):
    """
    An event which value can be cached

    :param cache: cache
    :type cache: :class:`Cache`
    :param event: event
    :type event: :class:`Event`
    """
    def __init__(self, cache, event):
        self.cache = cache
        self.event = event
        self.id = event.id

    def evaluate(self):
        if self.cache.enabled:
            if self.id not in self.cache:
                self.cache[self.id] = self.event.evaluate()
            return self.cache[self.id]
        else:
            return self.event.evaluate()

    def __repr__(self, *args, **kwargs):
        return repr(self.event)

    def __str__(self, *args, **kwargs):
        if isinstance(self.event, EventReference):
            return self.event.__str__(evaluatedValue=self.evaluate())
        return "({0} -> {1})".format(self.id, self.evaluate())

@ClassLogger
class Policy(object):
    """
    A policy base class

    :param cache: cache
    :type cache: :class:`Cache`
    """
    __metaclass__ = ABCMeta
    id = None

    def __init__(self, cache=None):
        self.cache = cache
        self.state = States.ENABLED

    @property
    def description(self):
        """
        Formatted description based on the doc string
        """
        description = []
        for line in self.__doc__.splitlines():
            line = line.strip()
            if line and not line.startswith(":"):
                description.append(line)
        return "\n".join(description)

    @abstractmethod
    def evaluateEvent(self):
        pass

    @abstractmethod
    def performActions(self):
        pass

    def __hash__(self, *args, **kwargs):
        return hash(repr(self))

    def __repr__(self, *args, **kwargs):
        return "{0}".format(self.id)

    def __str__(self, *args, **kwargs):
        return "{0}".format(self.id)

@ClassLogger
class PolicyComponent(Policy):
    """
    A policy component consisting of an event and respective list of actions

    :param name: name
    :type name: str
    :param event: event
    :type event: :class:`Event`
    :param actions: list of actions
    :param actions: [:class:`ActionReference`]
    """
    def __init__(self, name, event, actions, cache=None):
        super(PolicyComponent, self).__init__(cache)
        self.id = name
        self.event = event
        self.actions = actions
        self.policyHashes = {}
        self.policies = OrderedDict()

    def addPolicy(self, policy):
        """
        Add a child policy

        :param policy: policy
        :type policy: :class:`Policy`
        """
        policyHash = hash(policy)
        if policyHash in self.policyHashes:
            self.log.error("policy '%s' already exists", repr(policy))
        else:
            self.policyHashes[policyHash] = policy
            self.policies[policy.id] = policy
            if isinstance(policy, PolicyComponent):
                self.log.debug("'%s' added policy '%s' '%s'", self.id, policy.id, repr(policy))
            else:
                self.log.debug("'%s' added policy '%s'", self.id, policy.id)

    def evaluateEvent(self):
        return self.event.evaluate()

    def performActions(self):
        if self.actions:
            for action in self.actions:
                action.perform()

    def __hash__(self, *args, **kwargs):
        return hash(repr(self))

    def __repr__(self, *args, **kwargs):
        return "{0} -> {1}".format(repr(self.event), ",".join([str(a) for a in self.actions]))

    def __str__(self, *args, **kwargs):
        return "{0}: {1} -> {2}".format(self.id, self.event, ",".join([str(a) for a in self.actions]))

@ClassLogger
class PolicyDatabase(object):
    """
    An abstraction of the underlying database where policies are stored
    """
    def __init__(self):
        self.database = DBManager()

    def addPolicyUsingName(self, fullPolicyName, policy):
        """
        Add a policy

        :param fullPolicyName: fully qualified policy name
        :type fullPolicyName: str
        :param policy: policy
        :type policy: :class:`Policy`
        """
        nameHierarchy = fullPolicyName.split("/")
        if len(nameHierarchy) == 1:
            # no parent
            if self.policyExists(policy):
                self.log.error("policy '%s' already exists", repr(policy))
                return False
            else:
                self.addPolicy(policy)

                # check if we can add children
                if hasattr(policy, "policies"):
                    for childPolicy in policy.policies.values():
                        self.addPolicyUsingName("{0}/{1}".format(fullPolicyName, childPolicy.id), childPolicy)

        else:
            parentPolicyName = "/".join(nameHierarchy[:-1])
            parentDatabaseId = self.getPolicyDatabaseId(parentPolicyName)
            if parentDatabaseId:
                if self.policyExists(policy, parentDatabaseId):
                    self.log.error("policy '%s' already exists", repr(policy))
                    return False
                else:
                    self.addPolicy(policy, parentDatabaseId)

                    # check if we can add children
                    if hasattr(policy, "policies"):
                        for childPolicy in policy.policies.values():
                            self.addPolicyUsingName("{0}/{1}".format(fullPolicyName, childPolicy.id), childPolicy)
            else:
                self.log.error("parent policy '%s' is missing", parentPolicyName)
                return False

        return True

    def addPolicy(self, policy, parentDatabaseId=None):
        """
        Add a policy

        :param policy: policy
        :type policy: :class:`Policy`
        :param parentDatabaseId: parent database id
        :type parentDatabaseId: int
        """
        propertiesString = None
        representation = repr(policy)
        if isinstance(policy, PolicyComponent):
            # TODO: do we care about policy wrappers or class names of extended policy components?
            policyType = "{}.{}".format(getFullModuleName(PolicyComponent), PolicyComponent.__name__)

        else:
            policyType = "{}.{}".format(getFullModuleName(policy), policy.__class__.__name__)
            properties = PolicyProperties()
            properties.description = policy.description
            propertiesString = properties.toJSON(True)
        self.database.writeCommit("""
            insert into t_sm_policies (parent_id, name, representation, hash, state, type, properties)
            values (?, ?, ?, ?, ?, ?, ?)""",
            (parentDatabaseId, policy.id, representation, hash(policy),
             policy.state.name, policyType, propertiesString))
        if isinstance(policy, PolicyComponent):
            self.log.debug("stored policy '%s' '%s'", policy.id, repr(policy))
        else:
            self.log.debug("stored policy '%s'", policy.id)

    def clear(self):
        """
        Remove all policies
        """
        self.database.writeCommit("delete from t_sm_policies")

    def disablePolicyById(self, policy_id):
        """
        Disables the policy in the database given its id
        """
        self.log.debug("Disabling policy in database: %s", policy_id)
        self.database.writeCommit("update t_sm_policies set state = ? where id = ?", (States.DISABLED.name, policy_id))

    def enablePolicyById(self, policy_id):
        """
        Enables the policy in the database given its id
        """
        self.log.debug("Enabling policy in database: %s", policy_id)
        self.database.writeCommit("update t_sm_policies set state = ? where id = ?", (States.ENABLED.name, policy_id))

    def getNumberOfTopLevelPolicies(self):
        """
        Get number of top level policies

        :returns: number of top level policies
        :rtype: int
        """
        rows = self.database.query("""
            select count()
            from t_sm_policies
            where parent_id is null""")
        return rows[0][0]

    def getPolicyDatabaseId(self, fullPolicyName):
        """
        Get database id for the specified policy

        :param fullPolicyName: fully qualified policy name
        :type fullPolicyName: str
        :returns: database id
        :rtype: int
        """
        # TODO: convert to recursive query
        try:
            nameHierarchy = fullPolicyName.split("/")
            parentId = None
            for policyName in nameHierarchy:
                rows = self.database.query("""
                    select id
                    from t_sm_policies
                    where parent_id is ? and name is ?""",
                    (parentId, policyName))
                if not rows:
                    raise Exception("not found")
                parentId = rows[0]["id"]

            return rows[0]["id"]
        except:
            return None

    def getPolicyInfo(self, fullPolicyName):
        """
        Get policy info for the specified policy

        :param fullPolicyName: fully qualified policy name
        :type fullPolicyName: str
        :returns: policy info
        :rtype: :class:`PolicyInfo`
        """
        databaseId = self.getPolicyDatabaseId(fullPolicyName)
        if databaseId:
            return self.getPolicyInfoByDatabaseId(databaseId)
        return None

    def getPolicyInfoByDatabaseId(self, databaseId):
        """
        Get policy info for the specified policy

        :param databaseId: database id
        :type databaseId: int
        :returns: policy info
        :rtype: :class:`PolicyInfo`
        """
        policyInfo = None
        rows = self.database.query("""
            with recursive
                policies(id, level, name, representation, state, type, properties) as (
                    select id, 0, name, representation, state, type, properties
                    from t_sm_policies
                    where id is ?
                    union all
                    select t.id, policies.level+1, policies.name || "/" || t.name, t.representation, t.state, t.type, t.properties
                    from t_sm_policies as t join policies on t.parent_id=policies.id
                 order by 2 desc
                )
            select * from policies;""", (databaseId,))

        policyInfos = self._convertRowsToPolicyInfos(rows)
        if policyInfos:
            policyInfo = policyInfos.pop()
        return policyInfo

    def getPolicyInfos(self):
        """
        Get all policy infos

        :returns: list of policy infos
        :rtype: [:class:`PolicyInfo`]
        """
        rows = self.database.query("""
            with recursive
                policies(id, level, name, representation, state, type, properties) as (
                    select id, 0, name, representation, state, type, properties
                    from t_sm_policies
                    where parent_id is null
                    union all
                    select t.id, policies.level+1, policies.name || "/" || t.name, t.representation, t.state, t.type, t.properties
                    from t_sm_policies as t join policies on t.parent_id=policies.id
                 order by 2 desc
                )
            select * from policies;""")
        return self._convertRowsToPolicyInfos(rows)

    def policyExists(self, policy, parentDatabaseId=None):
        """
        Does the specified policy already exist

        :param policy: policy
        :type policy: :class:`Policy`
        :param parentDatabaseId: parent database id
        :type parentDatabaseId: int
        :returns: whether policy exists
        :rtype: bool
        """
        rows = self.database.query("""
            select name
            from t_sm_policies
            where parent_id is ? and hash = ?""",
            (parentDatabaseId, hash(policy),))
        if rows:
            return True
        return False

    def _convertRowsToPolicyInfos(self, rows):
        """
        Convert database rows into policy infos

        :param rows: database rows
        :type rows: [:class:`~sqlite3.row`]
        :returns: list of policy infos
        :rtype: [:class:`PolicyInfo`]
        """
        if not rows:
            return []

        root = PolicyInfo("root", None, None, None, None)
        for row in rows:

            state = States.valueOf(row["state"])
            if row["properties"]:
                properties = PolicyProperties.fromJSON(row["properties"])
            else:
                properties = None

            # split fully qualified name into path and name
            currentPath = row["name"].split("/")
            name = currentPath.pop()
            policyInfo = PolicyInfo(name, row["representation"], state, row["type"], properties)

            # traverse path to parent
            currentPolicyInfo = root
            for pathElement in currentPath:
                currentPolicyInfo = currentPolicyInfo.policies[pathElement]
            currentPolicyInfo.addPolicyInfo(policyInfo)

        return root.policies.values()

@ClassLogger
class PolicyEngine(object):
    """
    Policy engine that allows iterating over policies and performing their actions
    based on whether the specified event matches
    """
    def __init__(self):
        self.events = {}
        self.cache = Cache()
        self.cache.enabled = False
        self.actions = {}
        self.policyParser = PolicyParser(self)
        self.policies = OrderedDict()
        self.policyDatabase = PolicyDatabase()

        self.loadActions()
        self.loadEvents()

        if self.policyDatabase.getNumberOfTopLevelPolicies() > 0:
            self.loadPoliciesFromDatabase()
        else:
            self.loadDefaultPolicies()

    def addAction(self, action):
        """
        Add known action

        :param action: action
        :type action: :class:`Action`
        """
        self.log.debug("adding action '%s'", action.id)
        self.actions[action.id] = action

    def addActions(self, actions):
        """
        Add known actions

        :param actions: actions
        :type actions: [:class:`Action`]
        """
        for action in actions:
            self.addAction(action)

    def addEvent(self, event):
        """
        Add known event

        :param event: event
        :type event: :class:`Event`
        """
        if event == Event:
            self.log.warn("cannot add base event class")
        elif issubclass(event, (UnaryOperator, BinaryOperator)):
            self.log.warn("cannot add operator '%s'", event.id)
        else:
            self.log.debug("adding event '%s'", event.id)
            self.events[event.id] = event

    def addEvents(self, events):
        """
        Add known events

        :param events: events
        :type events: [:class:`Event`]
        """
        for event in events:
            self.addEvent(event)

    def addPolicy(self, policy):
        """
        Add a policy

        :param policy: policy
        :type policy: :class:`Policy`
        """
        if self.policyDatabase.addPolicyUsingName(policy.id, policy):
            self.policies[policy.id] = policy

    def addPolicies(self, policies):
        """
        Add policies

        :param policies: policies
        :type policies: [:class:`Policy`]
        """
        for policy in policies:
            self.addPolicy(policy)

    def convertToPolicies(self, policyInfos):
        """
        Convert policy infos into actual policies

        :param policyInfos: policy infos
        :type policyInfos: [:class:`PolicyInfo`]
        :returns: policies
        :rtype: [:class:`Policy`]
        """
        policyComponentType = "{}.{}".format(getFullModuleName(PolicyComponent), PolicyComponent.__name__)

        policies = []
        for policyInfo in policyInfos:

            if policyInfo.type == policyComponentType:

                try:
                    policy = self.policyParser.parsePolicy(policyInfo.name + ":" + policyInfo.representation)
                    policy.state = policyInfo.state

                    # load children
                    if policyInfo.policies:
                        childPolicies = self.convertToPolicies(policyInfo.policies.values())
                        for childPolicy in childPolicies:
                            policy.addPolicy(childPolicy)

                    policies.append(policy)
                    self.log.debug("loaded policy '%s: %s'", policy.id, repr(policy))

                except Exception as e:
                    self.log.error("could not load policy '%s': '%s': %s", policyInfo.name, policyInfo.representation, e)

            else:
                try:
                    # get class info
                    info = policyInfo.type.split(".")
                    className = info.pop()
                    moduleName = ".".join(info)

                    # load class from module
                    module = __import__(moduleName, fromlist=[className])
                    clazz = getattr(module, className)

                    # create instance based off constructor
                    args = inspect.getargspec(clazz.__init__)
                    if len(args[0]) > 1:
                        policy = clazz(self.cache)
                    else:
                        policy = clazz()
                    policy.state = policyInfo.state
                    policies.append(policy)
                    self.log.debug("loaded policy '%s' of type '%s'", policyInfo.name, policyInfo.type)
                except Exception as e:
                    self.log.error("could not load policy '%s' of type '%s'", policyInfo.name, policyInfo.type, e)

        return policies

    def disablePolicy(self, policy):
        """
        Disables the given policy
        """
        self.log.debug("Disabling policy %s", str(policy))
        policy_id = self.policyDatabase.getPolicyDatabaseId(policy.id)
        if policy_id is None:
            self.log.error("Unable to get policy from the database: %s", str(policy))
            return
        # disable the policy in memory and in the database
        if policy.state == States.ENABLED:
            policy.state = States.DISABLED
            self.policyDatabase.disablePolicyById(policy_id)
        else:
            self.log.info("Policy is already disabled %s", str(policy))

    def enablePolicy(self, policy):
        """
        Enables the given policy
        """
        self.log.debug("Enabling policy %s", str(policy))
        policy_id = self.policyDatabase.getPolicyDatabaseId(policy.id)
        if policy_id is None:
            self.log.error("Unable to get policy from the database: %s", str(policy))
            return
        # enable the policy in memory and in the database
        if policy.state == States.DISABLED:
            policy.state = States.ENABLED
            self.policyDatabase.enablePolicyById(policy_id)
        else:
            self.log.info("Policy is already enabled %s", str(policy))

    def loadActions(self):
        """
        Loads Actions from the c4/system/policies directory.
        """
        actions = c4.utils.util.getModuleClasses(c4.system.actions, c4.system.policyEngine.Action)
        actions.extend(c4.utils.util.getModuleClasses(c4.system.policies, c4.system.policyEngine.Action))
        # filter out base classes
        actions = [action for action in actions if action != Action and action != ActionReference]
        self.addActions(actions)

    def loadDefaultPolicies(self):
        """
        Loads Policies from the c4/system/policies directory.
        """
        # load policies
        policies = c4.utils.util.getModuleClasses(c4.system.policies, c4.system.policyEngine.Policy)
        # filter out base class
        policies = [policy for policy in policies if policy != c4.system.policyEngine.Policy]
        for policy in policies:
            try:
                self.log.debug("loading default policy '%s' of type '%s.%s'", policy.id, policy.__module__, policy.__name__)
                self.addPolicy(policy(self.cache))
            except Exception as e:
                self.log.error(e)
                self.log.error(traceback.format_exc())

        wrappedPolicies = c4.utils.util.getModuleClasses(c4.system.policies, c4.system.policyEngine.PolicyWrapper)
        # remove base class
        if c4.system.policyEngine.PolicyWrapper in wrappedPolicies:
            wrappedPolicies.remove(c4.system.policyEngine.PolicyWrapper)
        for wrappedPolicy in wrappedPolicies:
            self.log.debug("loading default policy '%s' '%s'", wrappedPolicy.id, wrappedPolicy.policy)
            self.loadPolicy(wrappedPolicy.id + ":" + wrappedPolicy.policy)

    def loadEvents(self):
        """
        Loads Events from the c4/system/policies directory.
        """
        events = c4.utils.util.getModuleClasses(c4.system.events, c4.system.policyEngine.Event)
        events.extend(c4.utils.util.getModuleClasses(c4.system.policies, c4.system.policyEngine.Event))
        # filter out base classes and operators
        events = [event for event in events if event != Event and not issubclass(event, (UnaryOperator, BinaryOperator))]
        self.addEvents(events)

    def loadPoliciesFromDatabase(self):
        """
        Load policies from the policy database table
        """
        policies = self.convertToPolicies(self.policyDatabase.getPolicyInfos())
        self.policies.clear()
        for policy in policies:
            self.policies[policy.id] = policy

    def loadPolicy(self, string):
        """
        Load a policy into the engine

        :param string: policy string
        :type string: str
        """
        try:
            policy = self.policyParser.parsePolicy(string)
            self.addPolicy(policy)
        except Exception as e:
            self.log.error("could not load policy '%s': %s", string, e)

    def run(self, policy=None):
        """
        If a policy is given then check if specified event
        matches and perform actions accordingly, followed
        by running its child policies.

        If no policy is specified start with root policies.

        :param policy: policy
        :type policy: :class:`Policy`
        """
        if policy:
            start = datetime.utcnow()
            if policy.evaluateEvent():
                self.log.debug("event match for '%s'", policy)
                policy.performActions()
                if hasattr(policy, "policies"):
                    for childPolicy in policy.policies.values():
                        if childPolicy.state == States.ENABLED:
                            try:
                                self.run(childPolicy)
                            except Exception as e:
                                self.log.error(e)
                                self.log.error(traceback.format_exc())
            else:
                self.log.debug("no event match for '%s'", policy)
            end = datetime.utcnow()
            self.log.debug("executing policy '%s' took %s", policy.id, end-start)
            self.checkPerformanceIssues(policy.id, start, end)

        else:
            start = datetime.utcnow()

            # clear cache on events
            self.cache.clear()
            self.cache.enabled = True

            # go through policies in order
            for policy in self.policies.values():
                if policy.state == States.ENABLED:
                    try:
                        self.run(policy)
                    except Exception as e:
                        self.log.error(e)
                        self.log.error(traceback.format_exc())
                else:
                    self.log.debug("'%s' disabled", policy.id)

            # clear cache on events
            self.cache.clear()
            self.cache.enabled = False

            end = datetime.utcnow()
            self.log.debug("executing policy engine took %s", end-start)

    def checkPerformanceIssues(self, policyName, start, end):
        """
        TODO: documentation
        """
        # this value might require tweaking for complex policies and multinode systems
        policyPerfomanceWarn = 2
        execTime = (end-start).total_seconds()
        if execTime > policyPerfomanceWarn:
            self.log.warning("Executing policy '%s' has taken: %s seconds", policyName, execTime)

class PolicyInfo(JSONSerializable):
    """
    Policy information

    :param name: name
    :type name: str
    :param representation: representation
    :type representation: str
    :param state: state
    :type state: :class:`States`
    :param policyType: type
    :type policyType: str
    :param properties: properties
    :type properties: dict
    """
    def __init__(self, name, representation, state, policyType, properties):
        self.name = name
        self.representation = representation
        self.state = state
        self.type = policyType
        self.policies = None
        self.properties = properties

    def addPolicyInfo(self, policyInfo):
        """
        Add child policy information

        :param policyInfo: policy info
        :type policyInfo: :class:`PolicyInfo`
        :returns: :class:`PolicyInfo`
        """
        if self.policies is None:
            self.policies = OrderedDict()
        if policyInfo.name in self.policies:
            log.error("'%s' already part of '%s'", policyInfo.name, self.name)
        else:
            self.policies[policyInfo.name] = policyInfo
        return self

@ClassLogger
class PolicyParser(object):
    """
    Base implementation of a policy parser using ``pyparsing``

    :param policyEngine: policy engine
    :type policyEngine: :class:`PolicyEngine`
    """
    def __init__(self, policyEngine):
        self.policyEngine = policyEngine
        self.unaryOperators = {}
        self.binaryOperators = {}

        import pyparsing
        import c4.system.events.operators

        # constant values
        self.stringConstantElement = (pyparsing.QuotedString("\"", unquoteResults=True) |
                                      pyparsing.QuotedString("'", unquoteResults=True))
        self.numberConstantElement = pyparsing.Word(pyparsing.nums + ".")
        def numberConstantElementParseAction(tokens):
            self.log.debug("found number constant '%s'", tokens[0])
            if "." in tokens[0]:
                try:
                    return float(tokens[0])
                except:
                    pass
            else:
                try:
                    return int(tokens[0])
                except:
                    pass
            return tokens
        self.numberConstantElement.addParseAction(numberConstantElementParseAction)

        self.constantElement = self.stringConstantElement |  self.numberConstantElement

        # key-value pair constant
        self.namedConstantElement = pyparsing.Word(pyparsing.alphanums) + "=" + self.constantElement
        def namedConstantParseAction(string, location, tokens):
            self.log.debug("found named constant  '%s = %s'", tokens[0], tokens[2])
            return {tokens[0]: tokens[2]}
        self.namedConstantElement.addParseAction(namedConstantParseAction)

        self.eventReferenceElement = pyparsing.Forward()

        # parameters
        self.parameterElement = self.constantElement | self.namedConstantElement | self.eventReferenceElement
        self.parametersElement = self.parameterElement + pyparsing.ZeroOrMore(pyparsing.Suppress(",") + self.parameterElement)
        def parametersParseAction(string, location, tokens):
            arguments = []
            keyValueArguments = {}
            for parameter in tokens:
                self.log.debug("found parameter '%s'", repr(parameter))
                if isinstance(parameter, dict):
                    keyValueArguments.update(parameter)
                else:
                    arguments.append(parameter)
            return (arguments, keyValueArguments)
        self.parametersElement.addParseAction(parametersParseAction)

        # event references
        self.eventReferenceElement << (
                                        (pyparsing.Word(pyparsing.alphanums + ".") +
                                               pyparsing.Suppress("(") +
                                                   pyparsing.Optional(self.parametersElement) +
                                               pyparsing.Suppress(")")) |
                                       pyparsing.Word(pyparsing.alphanums + ".")
                                       )
        def eventReferenceElementParseAction(string, location, tokens):

            if len(tokens) == 1:
                self.log.debug("found event reference '%s'", tokens[0])
                parameters = ([], {})
            else:
                self.log.debug("found event reference '%s%s'", tokens[0], repr(tokens[1]))
                parameters = tokens[1]

            if tokens[0] not in self.policyEngine.events:
                raise pyparsing.ParseFatalException(string, location,
                        "found unknown event reference '{0}'".format(repr(tokens[0])))

            # set up event implementation
            event = self.policyEngine.events[tokens[0]]()
            self.checkParameters(event, "evaluate", parameters[0], parameters[1])

            return CachableEvent(self.policyEngine.cache,
                                EventReference(event, parameters[0], parameters[1]))
        self.eventReferenceElement.addParseAction(eventReferenceElementParseAction)

        # event operators
        self.unaryOperatorElement = pyparsing.Or([])
        self.binaryOperatorElement = pyparsing.Or([])

        # TODO: outsource to load function?
        unaryOperatorList = c4.utils.util.getModuleClasses(c4.system.events.operators, UnaryOperator)
        for operatorImplementation in unaryOperatorList:
            self.unaryOperators[operatorImplementation.id] = operatorImplementation
            self.unaryOperatorElement.append(pyparsing.Or(operatorImplementation.id))

        binaryOperatorList = c4.utils.util.getModuleClasses(c4.system.events.operators, BinaryOperator)
        for operatorImplementation in binaryOperatorList:
            self.binaryOperators[operatorImplementation.id] = operatorImplementation
            self.binaryOperatorElement.append(pyparsing.Or(operatorImplementation.id))

        # basic value event with an optional unary operator
        self.valueEventElement = (pyparsing.Optional(self.unaryOperatorElement) +
                                    (self.constantElement | self.eventReferenceElement))
        def valueEventElementParseAction(string, location, tokens):
            if len(tokens) == 1:
                self.log.debug("found event '%s'", repr(tokens[0]))
                return tokens[0]

            # check for unary operators
            if len(tokens) == 2:
                self.log.debug("found event '%s %s'", tokens[0], repr(tokens[1]))
                if tokens[0] in self.unaryOperators:
                    return self.unaryOperators[tokens[0]](tokens[1])
                else:
                    raise pyparsing.ParseException("found unknown unary operator '{0}'".format(repr(tokens[0])))
        self.valueEventElement.addParseAction(valueEventElementParseAction)

        # complex event that may consist of a combination of events
        self.eventElement = pyparsing.Forward()
        self.eventElement << (
                (pyparsing.Optional(self.unaryOperatorElement) + pyparsing.Suppress("(") + self.eventElement + pyparsing.Suppress(")") +
                        pyparsing.Optional(self.binaryOperatorElement +
                                pyparsing.Or([
                                                pyparsing.Optional(self.unaryOperatorElement) + pyparsing.Suppress("(") + self.eventElement + pyparsing.Suppress(")"),
                                                self.valueEventElement]))) |
                (self.valueEventElement + self.binaryOperatorElement + self.valueEventElement) |
                self.valueEventElement
                  )
        def eventElementParseAction(string, location, tokens):
            if len(tokens) == 1:
                self.log.debug("found event '%s'", repr(tokens[0]))
                return tokens[0]

            # check for unary operators
            if len(tokens) == 2:
                self.log.debug("found event '%s %s'", tokens[0], repr(tokens[1]))
                if tokens[0] in self.unaryOperators:
                    return self.unaryOperators[tokens[0]](tokens[1])
                else:
                    raise pyparsing.ParseException("found unknown unary operator '{0}'".format(repr(tokens[0])))

            # check for binary operators
            if len(tokens) == 3:
                self.log.debug("found event '%s %s %s)'", repr(tokens[0]), tokens[1], repr(tokens[2]))
                if tokens[1] in self.binaryOperators:
                    return self.binaryOperators[tokens[1]](tokens[0], tokens[2])
                else:
                    raise pyparsing.ParseException("found unknown binary operator '{0}'".format(tokens[1]))
        self.eventElement.addParseAction(eventElementParseAction)

        # action identifier
        self.actionIdElement = pyparsing.Word(pyparsing.alphanums + ".")

        # action specified by an id and optional parameters
        self.actionElement = (self.actionIdElement +
                              pyparsing.Suppress("(") + pyparsing.Optional(self.parametersElement) + pyparsing.Suppress(")"))
        def actionElementParseAction(string, location, tokens):
            if len(tokens) == 1:
                self.log.debug("found action '%s'", tokens[0])
                parameters = ([], {})
            else:
                self.log.debug("found action '%s%s'", tokens[0], repr(tokens[1]))
                parameters = tokens[1]

            if tokens[0] not in self.policyEngine.actions:
                raise pyparsing.ParseFatalException(string, location,
                        "found unknown action reference '{0}'".format(tokens[0]))

            # set up action implementation
            action = self.policyEngine.actions[tokens[0]]()
            arguments = parameters[0]
            keyValueArguments = parameters[1]
            handlerArgSpec = inspect.getargspec(action.perform)
            handlerArguments = handlerArgSpec[0][1:]

            # check for named arguments
            handlerKeyValueArguments = {}
            if handlerArgSpec[3]:
                keys = handlerArguments[-len(handlerArgSpec[3]):]
                handlerKeyValueArguments = dict(zip(keys, handlerArgSpec[3]))
                handlerArguments = handlerArguments[:len(handlerArguments)-len(handlerArgSpec[3])]

            # make sure we have at least the number of arguments that the action requires
            if len(handlerArguments) != len(arguments):
                raise pyparsing.ParseFatalException(string, location,
                        "action '{0}' requires {1} arguments but {2}: {3} are given".format(
                            action, len(handlerArguments), len(arguments), arguments))

            # check for unknown named arguments
            for key in keyValueArguments:
                if key not in handlerKeyValueArguments:
                    raise pyparsing.ParseFatalException(string, location,
                        "action '{0}' does not have a named argument '{1}'".format(action, key))

            return ActionReference(self.policyEngine.actions[tokens[0]](), parameters[0], parameters[1])
        self.actionElement.addParseAction(actionElementParseAction)

        # list of actions
        self.actionsElement = self.actionElement + pyparsing.ZeroOrMore(pyparsing.Suppress(",") + self.actionElement)

        # policy element consisting of a name, an event and a set of actions
        self.policyElement = pyparsing.Word(pyparsing.alphanums + "." + "_") + pyparsing.Suppress(":") + self.eventElement + pyparsing.Suppress("->") + self.actionsElement

    def checkParameters(self, o, method, arguments, keyValueArguments):
        """
        Check parameters for the specified method

        :param o: object
        :type o: object
        :param method: method
        :type method: str
        :param arguments: arguments
        :type arguments: list
        :param keyValueArguments: key value arguments
        :type keyValueArguments: dict
        :raises ValueError: if parameters are not valid
        """

        # set up implementation
        handlerArgSpec = inspect.getargspec(getattr(o, method))
        handlerArguments = handlerArgSpec[0][1:]

        # check for named arguments
        handlerKeyValueArguments = {}
        if handlerArgSpec[3]:
            keys = handlerArguments[-len(handlerArgSpec[3]):]
            handlerKeyValueArguments = dict(zip(keys, handlerArgSpec[3]))
            handlerArguments = handlerArguments[:len(handlerArguments)-len(handlerArgSpec[3])]

        # make sure we have at least the number of arguments that the object requires
        if len(handlerArguments) != len(arguments) and handlerArgSpec[1] is None:
            raise ValueError("object '{0}' requires {1} arguments but {2}: {3} are given".format(
                        repr(o), len(handlerArguments), len(arguments), arguments))

        # check for unknown named arguments
        for key in keyValueArguments:
            if key not in handlerKeyValueArguments:
                raise ValueError("object '{0}' does not have a named argument '{1}'".format(repr(o), key))

    def parseAction(self, string):
        """
        Parse string into :class:`Action`

        :returns: :class:`Action`
        """
        return self.actionElement.parseString(string, parseAll=True)[0]

    def parseActions(self, string):
        """
        Parse string into multiple :class:`Action` s

        :returns: [:class:`Action`]
        """
        return self.actionsElement.parseString(string, parseAll=True)

    def parseEvent(self, string):
        """
        Parse string into :class:`Event`

        :returns: :class:`Event`
        """
        return self.eventElement.parseString(string, parseAll=True)[0]

    def parsePolicy(self, string):
        """
        Parse string into :class:`Policy`

        :returns: :class:`Policy`
        """
        policyItems = self.policyElement.parseString(string, parseAll=True)
        return PolicyComponent(policyItems[0], policyItems[1], policyItems[2:])

class PolicyProperties(JSONSerializable):
    """
    Policy properties
    """
    def __init__(self):
        self.description = None

@ClassLogger
class PolicyWrapper(object):
    """
    Derived classes need to provide an id and policy string.
    The PolicyWrapper class is used to load policies from disk,
    see c4/system/policies/
    """
    id = ""
    policy = ""

@ClassLogger
class UnaryOperator(Event):
    """
    A unary operator base class

    :param one: event one
    :type one: :class:`Event`
    """
    __metaclass__ = ABCMeta
    id = "unaryOperator"

    def __init__(self, one):
        super(UnaryOperator, self).__init__()
        self.one = ValueEvent.create(one)

    @abstractmethod
    def evaluateOperation(self, one):
        pass

    def evaluate(self):
        one = self.one.evaluate()
        return self.evaluateOperation(one)

    def __repr__(self, *args, **kwargs):
        return "({0} {1})".format(self.id, repr(self.one))

    def __str__(self, *args, **kwargs):
        return "({0} {1} -> {2})".format(self.id, self.one, self.evaluate())

@ClassLogger
class ValueEvent(Event):
    """
    A base value event

    :param value: value
    """
    id = "value"

    def __init__(self, value):
        super(ValueEvent, self).__init__()
        self._value = value

    def evaluate(self):
        """
        Return the value of the event

        :returns: value
        """
        return self._value

    @staticmethod
    def create(value):
        """
        Create a :class:`ValueEvent` given the value.

        .. note::

            If ``value`` is already an :class:`Event` then
            itself is returned instead

        :param value: value
        """
        if isinstance(value, Event):
            return value
        else:
            return ValueEvent(value)

    def __repr__(self, *args, **kwargs):
        return repr(self._value)

    def __str__(self, *args, **kwargs):
        return str(self.evaluate())

def getFormattedArgumentString(arguments, keyValueArguments):
    """
    Get a formatted version of the argument string such that it can be used
    in representations.

    E.g., action("test", key="value") instead of action(["test"],{"key"="value"}

    :param arguments: arguments
    :type arguments: []
    :param keyValueArguments: key value arguments
    :type keyValueArguments: dict
    :returns: formatted argument string
    :rtype: str
    """
    argumentString = ""
    if arguments:
        argumentList = []
        for argument in arguments:
            if isinstance(argument, (str, unicode)):
                argumentList.append("'{0}'".format(argument))
            else:
                argumentList.append(str(argument))
        argumentString += ",".join(argumentList)

    if keyValueArguments:
        if arguments:
            argumentString += ","
        keyValueArgumentList = []
        for key, value in keyValueArguments.items():
            if isinstance(value, (str, unicode)):
                keyValueArgumentList.append("{0}='{1}'".format(key, value))
            else:
                keyValueArgumentList.append("{0}={1}".format(key, str(value)))
        argumentString += ",".join(keyValueArgumentList)

    if argumentString:
        argumentString = "({0})".format(argumentString)

    return argumentString
