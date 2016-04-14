from abc import abstractproperty, ABCMeta, abstractmethod
import logging

from c4.utils.jsonutil import JSONSerializable
from c4.utils.logutil import ClassLogger


log = logging.getLogger(__name__)

@ClassLogger
class Backend(object):
    """
    Singleton backend reference that abstracts different backend implementations
    """
    __implementation = None

    def __new__(cls, implementation=None):
        if implementation:
            if isinstance(implementation, BackendImplementation):
                Backend.__implementation = implementation
                Backend.log.debug("setting backend implementation to '%s'", Backend.__implementation.__class__.__name__)
            else:
                Backend.log.error("'%s' is not of type '%s'", implementation, BackendImplementation)
        if Backend.__implementation is None:
            raise ValueError("need to initialize backend first")
        return Backend.__implementation

class BackendImplementation(object):
    """
    Backend implementation base class

    :param info: backend info
    :type info: :class:`~BackendInfo`
    """
    __metaclass__ = ABCMeta

    def __init__(self, info):
        self.info = info

    @abstractproperty
    def configuration(self):
        """
        Reference to the configuration implementation
        """

    @abstractmethod
    def ClusterInfo(self, node, address, systemManagerAddress, role, state):
        """
        A basic cluster information object

        :param node: node
        :type node: str
        :param address: address of the node
        :type address: str
        :param systemManagerAddress: address of the active system manager
        :type systemManagerAddress: str
        :param role: role of the node
        :type role: :class:`~Roles`
        :param state: state of the node
        :type state: :class:`~States`
        """

class BackendInfo(JSONSerializable):
    """
    Backend information base class

    :param backend: fully qualified backend type name
    :type backend: str
    :param properties: properties
    :type properties: dict
    """
    __metaclass__ = ABCMeta

    def __init__(self, backend, properties=None):
        self.backend = backend
        if properties is None:
            self.properties = {}
        else:
            self.properties = properties
