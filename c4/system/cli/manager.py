#!/usr/bin/env python
"""
Copyright (c) IBM 2015-2017. All Rights Reserved.
Project name: c4-system-manager
This project is licensed under the MIT License, see LICENSE

Commandline interface
"""
import argparse
import inspect
import logging
import re
import sys

from c4.utils.util import (getDocumentation, getVariableArguments,
                           initWithVariableArguments)

from .base import getCommands


log = logging.getLogger(__name__)

def getArgumentParser():
    """
    Dynamically generate argument parser based on available command line implementations
    """
    parser = argparse.ArgumentParser(description="A utility to perform cluster management",
                                     formatter_class=argparse.RawTextHelpFormatter)

    driverParser = argparse.ArgumentParser(add_help=False)
    driverParser.add_argument("-v", "--verbose",
                              action="count",
                              help="display debug information")

    def addCommandArgumentParser(parser, commandOrMap):
        """
        Add commands to the specified parser

        :param parser: parser
        :type parser: argparse parser
        :param commandOrMap: command class or dictionary with path to command class mapping
        :type commandOrMap: :class:`Command` or dict
        """
        if isinstance(commandOrMap, dict):
            commandMap = commandOrMap
            subcommandParser = parser.add_subparsers()
            for pathPart, subcommand in sorted(commandMap.items()):
                subparser = subcommandParser.add_parser(pathPart, parents=[driverParser])
                addCommandArgumentParser(subparser, subcommand)

        else:
            command = commandOrMap

            parser.set_defaults(commandClass=command)

            documentation = getDocumentation(command)

            handlerArgumentMap = getVariableArguments(command.__init__)[0]
            # add variable argument if specified in constructor
            variableArgument = inspect.getargspec(command.__init__).varargs
            if variableArgument:
                handlerArgumentMap[variableArgument] = "_notset_"
            for name, value in sorted(handlerArgumentMap.items()):

                argumentProperties = {}

                # check for description
                if name in documentation["parameters"]:
                    argumentProperties["help"] = documentation["parameters"][name].get("description")

                    # check if we can get information about type
                    if "type" in documentation["parameters"][name]:
                        if re.match(r"\[.+\]", documentation["parameters"][name]["type"]):
                            argumentProperties["action"] = "append"
                else:
                    log.warn("'%s' documentation is missing information for parameter '%s'", command, name)

                if value == "_notset_":
                    if argumentProperties.get("action") == "append":
                        # if multiple values required for a positional argument prefix it with -- and remove trailing s
                        argumentProperties["dest"] = name
                        name = "--{0}".format(name.strip("s"))
                        argumentProperties["required"] = True
                        if "help" in argumentProperties:
                            argumentProperties["help"] = "\n".join([argumentProperties["help"], "(can be specified multiple times)"])
                        else:
                            argumentProperties["help"] = "(can be specified multiple times)"
                else:
                    name = "--{0}".format(name)
                    argumentProperties["default"] = value
                    if isinstance(value, bool):
                        argumentProperties["action"] = "store_true"
                    if "help" in argumentProperties:
                        argumentProperties["help"] = "\n".join([argumentProperties["help"], "(default value '{0}')".format(value)])
                    else:
                        argumentProperties["help"] = "(default value '{0}')".format(value)

                parser.add_argument(name, **argumentProperties)

    addCommandArgumentParser(parser, getCommands())

    return parser

def main():
    """
    Main function of the commandline interface
    """
    logging.basicConfig(format='%(asctime)s [%(levelname)s] [%(name)s(%(filename)s:%(lineno)d)] - %(message)s', level=logging.INFO)

    parser = getArgumentParser()
    args = parser.parse_args()

    logging.getLogger("c4.utils").setLevel(logging.INFO)

    if args.verbose > 0:
        logging.getLogger("c4.utils").setLevel(logging.INFO)
    if args.verbose > 1:
        logging.getLogger("c4.utils").setLevel(logging.DEBUG)

    # get parameters from the commandline arguments
    parameters = {
        name: value
        for name, value in args.__dict__.items()
        if not name.startswith("_")
    }

    # remove common options
    parameters.pop("verbose", None)

    # use the command class to run the specified command
    commandClass = parameters.pop("commandClass")
    command = initWithVariableArguments(commandClass, **parameters)
    command.run()

    return 0

if __name__ == '__main__':
    sys.exit(main())
