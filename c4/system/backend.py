from abc import ABCMeta, abstractmethod, abstractproperty
import logging
import os

from c4.utils.jsonutil import JSONSerializable
from c4.utils.logutil import ClassLogger


log = logging.getLogger(__name__)

class BackendImplementation(object):
    """
    Backend implementation base class

    :param info: backend info
    :type info: :class:`~BackendInfo`
    """
    __metaclass__ = ABCMeta

    def __init__(self, info):
        self.info = info

    @staticmethod
    def fromConfigFile(fileName):
        """
        Load implementation from the specified JSON config file

        :param fileName: a file with the JSON object
        :type fileName: str
        :returns: implementation
        :rtype: :class:`~BackendImplementation`
        """
        if not os.path.exists(fileName):
            log.error("could not find backend configuration file '%s'", fileName)
            return None
        try:
            backendInfo = BackendInfo.fromJSONFile(fileName)

            # get class info
            info = backendInfo.backend.split(".")
            className = info.pop()
            moduleName = ".".join(info)

            # load class from module
            log.info("loading backend implementation '%s' from module '%s'", className, moduleName)
            module = __import__(moduleName, fromlist=[className])
            clazz = getattr(module, className)

            # create instance based off constructor
            backendImplementation = clazz(backendInfo)
            return backendImplementation
        except Exception as e:
            log.error("could not load backend from configuration file '%s' because '%s'", fileName, e)
        return None

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

    @abstractproperty
    def configuration(self):
        """
        Reference to the configuration implementation
        """

    @abstractproperty
    def deviceHistory(self):
        """
        Reference to the DeviceHistory implementation
        """

    @abstractproperty
    def keyValueStore(self):
        """
        Reference to the key-value store implementation
        """

    @abstractproperty
    def nodeHistory(self):
        """
        Reference to the NodeHistory implementation
        """

@ClassLogger
class Backend(BackendImplementation):
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
        return Backend.__implementation.ClusterInfo(node, address, systemManagerAddress, role, state)

    @property
    def configuration(self):
        """
        Reference to the configuration implementation
        """
        return Backend.__implementation.configuration

    @property
    def deviceHistory(self):
        """
        Reference to the DeviceHistory implementation
        """
        return Backend.__implementation.deviceHistory

    @property
    def keyValueStore(self):
        """
        Reference to the key-value store implementation
        """
        return Backend.__implementation.keyValueStore

    @property
    def nodeHistory(self):
        """
        Reference to the NodeHistory implementation
        """
        return Backend.__implementation.nodeHistory

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

class BackendKeyValueStore(object):
    """
    Backend key-value store
    """
    @abstractmethod
    def delete(self, key):
        """
        Delete value at specified key

        :param key: key
        :type key: str
        """

    @abstractmethod
    def deletePrefix(self, keyPrefix):
        """
        Delete all values with the specified key prefix

        :param keyPrefix: key prefix
        :type keyPrefix: str
        """

    @abstractmethod
    def get(self, key, default=None):
        """
        Get value at specified key

        :param key: key
        :type key: str
        :param default: default value to return if key not found
        :returns: value or default value
        :rtype: str
        """

    @abstractmethod
    def getAll(self):
        """
        Get all key-value pairs

        :returns: key-value pairs
        :rtype: [(key, value), ...]
        """

    @abstractmethod
    def getPrefix(self, key):
        """
        Get all key-value pairs with the specified key prefix

        :param key: key
        :type key: str
        :returns: key-value pairs
        :rtype: [(key, value), ...]
        """

    @abstractmethod
    def put(self, key, value):
        """
        Put value at specified key

        :param key: key
        :type key: str
        :param value: value
        :type value: str
        """

    @abstractproperty
    def transaction(self):
        """
        A transaction to perform puts and deletes in an atomic manner

        :returns: transaction object
        """
