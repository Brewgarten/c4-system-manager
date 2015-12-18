
from multiprocessing import Process
import threading
import logging
import sys
import inspect
import traceback
import abc
import zmq
from zmq.eventloop import ioloop
# must setup the ioloop with tornado
ioloop.install()
import tornado.web
import tornado.httpserver

from c4.utils.util import getModuleClasses
import c4.system.resthandlers
from c4.system.db import DBManager
from c4.messaging import Dealer, Envelope

class BaseRequestHandler(tornado.web.RequestHandler):
    """
    This is the abstract base class for REST request handlers.
    These handlers should be located in the resthandlers
    directory, so that they can be auto-loaded.

    Derived classes need to supply a URL_PATTERN and
    a get method.  The URL_PATTERN is the URL to handle
    (e.g. r"/nodes")

    If the URL_PATTERN contains a regex capture group
    (e.g. r"/nodes/([\w\-]+|%5B[\w,\-]+%5D|\*)/devices"),
    then the captured value will be a parameter to the
    get method.

    Derived classes can use the ALL_COMPONENT_PATTERN
    to capture a component.  A component is a hostname,
    device name, etc.  A component cannot be blank.

    OPTIONAL_TRAILING_SLASH is automatically appended
    to the URL_PATTERN.  This means that a URL with
    or without a trailing slash always goes to the
    same handler.
    """
    ___metaclass__ = abc.ABCMeta


    # derived classes need to override
    # do not add a trailing slash to the URL_PATTERN
    # since OPTIONAL_TRAILING_SLASH will be appended
    # to the URL_PATTERN
    URL_PATTERN = r""

    # To match "all components"
    # To be used by derived classes to build the URL_PATTERN
    # e.g.
    # Single component:
    # rack1-data1
    # List of components:
    # [cpu,memory]
    # Wildcard for all components
    # *
    # Note that [] URL encoded is %5B and %5D respectively
    # Does not match blank
    ALL_COMPONENT_PATTERN = "([\w\-]+|%5B[\w,\-]+%5D|\*)"

    DEVICE_PATTERN = "([\w\-/\.]+|%5B[\w,\-/\.]+%5D|\*)"

    # OPTIONAL_TRAILING_SLASH will automatically be appended
    # to the end of the URL_PATTERN
    OPTIONAL_TRAILING_SLASH = "/?"

    def initialize(self, dbm, restDealer, node, localSysmgrAddress):
        """
        dbm, restDealer, node, and localSysmgrAddress came from createURLSpec.
        All rest handlers have access to these objects.
        """
        self.dbm = dbm
        self.restDealer = restDealer
        self.node = node
        self.localSysmgrAddress = localSysmgrAddress


    @abc.abstractmethod
    def get(self):
        """
        """
        pass


    def logUser(self, message, *parameters):
        """
        The user parameter is required and is used for auditing
        calls to the REST interface.

        The logUser method has the same interface as logging.info

        Returns True if the user parameter is supplied, False otherwise
        """
        user = self.get_argument("user", default=None)
        if user is None:
            self.send_error(status_code=400, c4_error_message="Missing required parameter: user")
            return False
        f = sys._getframe(1)
        mod = inspect.getmodule(f)
        # log the user, module of caller, line number of caller, and message
        logging.info("User %s in %s:%s\n" + message,
                    user, mod.__name__, f.f_lineno,
                    *parameters)
        return True


    @classmethod
    def parseComponentId(cls, componentId):
        """
        Derived classes should call this method
        to parse a componentId (nodeId, deviceId, etc)
        into a list of components.

        A componentId can consist
        of a single component, a list of components, or "*".

        A componentId is added as an argument to the get method when
        ALL_COMPONENT_PATTERN is added to the URL_PATTERN

        This method always returns a list.
        """
        # *
        if componentId == "*":
            return ["*"]
        # comma separated list surrounded by square brackets
        elif componentId.startswith("[") and componentId.endswith("]"):
            # strip brackets
            temp = componentId[1:-1]
            # split on comma
            return temp.split(",")
        # single component
        else:
            return [componentId]


    @classmethod
    def printComponentList(cls, componentList):
        """
        Prints the output of parseComponetId nicely without brackets and unicode string prefix.
        """
        return ", ".join(componentList)


    def write_error(self, status_code, **kwargs):
        if "c4_error_message" in kwargs:
            self.write(kwargs["c4_error_message"])
            self.write("<br/>")
        super(BaseRequestHandler, self).write_error(status_code, **kwargs)


    def addHandler(self, handler, id):
        """
        Before sending a message to sysmgr, register a callback handler that
        will be called when the specific message id is received.
        """
        self.restDealer.addHandler(handler, id)


    def validateNodeListAndTransformWildcards(self, node_list, configuration):
        """
        Throws exception on invalid node in node list.
        """
        # * means all nodes
        if "*" in node_list:
            return configuration.nodes.keys()
        for node_name in node_list:
            # make sure node exists
            if node_name not in configuration.nodes:
                raise Exception("Invalid node: %s" % node_name)
        return node_list


class RestServer(Process):
    """
    Starts the REST server in a new process.  Dynamically loads
    REST handler classes from resthandlers directory.

    The listen address is used to restrict requests to a particular network.
    e.g. 127.0.0.1 limits requests from localhost only
    """
    def __init__(self, node, localSysmgrAddress, port=8081,
                 ssl_enabled=True, ssl_certificate_file="ssl.crt", ssl_key_file="ssl.key",
                 listen_address=""):
        Process.__init__(self)
        self.node = node
        self.localSysmgrAddress = localSysmgrAddress
        self.port = port
        self.ssl_enabled = ssl_enabled
        self.ssl_certificate_file = ssl_certificate_file
        self.ssl_key_file = ssl_key_file
        self.listen_address = listen_address


    def loadHandlers(self):
        """
        Dynamically load the REST handlers from the
        c4/system/resthandlers directory.
        """
        restHandlers = sorted(getModuleClasses(c4.system.resthandlers, c4.system.rest.BaseRequestHandler))
        # remove base class
        if c4.system.rest.BaseRequestHandler in restHandlers:
            restHandlers.remove(c4.system.rest.BaseRequestHandler)
        # remove any classes with a name that starts with underscore
        # these are base classes that we don't want executed
        restHandlers[:] = [handler for handler in restHandlers if not handler.__name__.startswith("_")]
        return restHandlers


    def createURLSpec(self, dbm, restDealer, node, localSysmgrAddress):
        """
        Creates a list of tuples that define handlers
        for use in the Tornado Application.
        Tuples are the url pattern, handler class, arguments to init handler class.
        """
        urlSpec = []
        restHandlers = self.loadHandlers()
        # for each handler, create a URL spec
        for restHandler in restHandlers:
            logging.info("Found rest handler %s", restHandler.__name__)
            # append the tuple
            urlSpec.append((restHandler.URL_PATTERN + restHandler.OPTIONAL_TRAILING_SLASH, restHandler,
                        dict(dbm=dbm,
                             restDealer=restDealer,
                             node=node,
                             localSysmgrAddress=localSysmgrAddress)))
        return urlSpec


    def run(self):
        logging.info("Rest Server Process starting")

        dbm = DBManager()

        restDealer = RestDealer(self.node)
        restDealer.start()

        main_loop = ioloop.IOLoop.instance()

        # load the REST handlers
        urlSpec = self.createURLSpec(dbm, restDealer, self.node, self.localSysmgrAddress)
        # create the REST server
        application = tornado.web.Application(urlSpec)
        # ssl
        ssl_options = None
        if self.ssl_enabled:
            ssl_options = {
                "certfile": self.ssl_certificate_file,
                "keyfile": self.ssl_key_file
            }
            logging.debug("Rest Server SSL enabled: port=%d %s", self.port, str(ssl_options))
        else:
            logging.debug("Rest Server SSL disabled: port=%d", self.port)
        rest_server = tornado.httpserver.HTTPServer(application, ssl_options = ssl_options)
        # add static path so that index.html can be provided
        import os
        import c4.data
        webRoot = os.path.abspath(os.path.join(c4.data.__path__[0], "web"))
        application.settings["static_path"] = webRoot
        application.settings["template_path"] = webRoot

        # use listen address of 127.0.0.1 to accept REST
        # requests from localhost only
        try:
            rest_server.listen(self.port, address=self.listen_address)
            main_loop.start()
        except KeyboardInterrupt:
            logging.info("Exiting..")
        except:
            logging.info("Forced exiting..")
            logging.error(traceback.format_exc())

class RestDealer(threading.Thread):
    """
    The RestServer has a single RestDealer which communicates with the local sysmgr.
    This RestDealer is shared among any REST requests that need to talk with sysmgr.
    Before the REST requests tries to communicate with sysmgr (via sendMessage),
    the REST request must register a callback function handler via the addHandler method.
    """

    def __init__(self, node):
        threading.Thread.__init__(self)
        self.node = node
        self.handlers = {}


    def addHandler(self, handler, id):
        """
        Adds a callback handler function to handle response messages with the given id.
        Use MessageID for id.
        """
        logging.info("Adding RestDealer handler with id " + id)
        self.handlers[id] = handler


    def run(self):
        logging.info("Rest Dealer Thread starting")

        dealerServer = Dealer("ipc://" + self.node + ".ipc", self.node + "/rest", "rest")
        dealerServer.register()

        poller = zmq.Poller()
        poller.register(dealerServer.socket, zmq.POLLIN)

        try:
            while True:
                sockets = dict(poller.poll())

                if dealerServer.hasNewMessage(sockets):
                    logging.debug("RestDealer received '%s'", dealerServer.newMessage)
                    envelope = Envelope.fromJSON(dealerServer.newMessage[0])
                    # only handle Version response messages
                    # ignore all others
                    #if isinstance(envelope, c4.system.messages.Version)
                    # or maybe better to be a little more generic and
                    # handle all response messages
                    # (i.e. response messages have RelatesTo attribute, request messages do not)
                    if hasattr(envelope, "RelatesTo"):
                        id = envelope.RelatesTo
                        if id in self.handlers:
                            # call the handler for this particular message
                            self.handlers[id](envelope)
                            # handler now called, so
                            # remove this id from the list of handlers
                            del self.handlers[id]
                        else:
                            logging.error("No RestDealer handler found for id = " + id)

        except KeyboardInterrupt:
            logging.info("Exiting..")
        except:
            logging.info("Forced exiting..")
            logging.error(traceback.format_exc())
        finally:
            poller.unregister(dealerServer.socket)
            del dealerServer
