"""
Base components and functionality for the cli
"""

from abc import ABCMeta, abstractmethod
import logging
import re

from c4.utils.util import getModuleClasses


log = logging.getLogger(__name__)

class Command(object):
    """
    Base command class
    """
    __metaclass__ = ABCMeta

    path = None

    @abstractmethod
    def run(self):
        """
        Runs this command
        """

def getCommands():
    """
    Get command classes

    :returns: nested command path to command class map
    :rtype: dict
    """
    commands = {}
    import c4.cli
    commandClasses = getModuleClasses(c4.cli, Command)
    for command in commandClasses:

        if command.path:
            if isinstance(command.path, (str, unicode)):

                parts = re.split(r"\s+", command.path.strip())
                currentLevel = commands
                for part in parts[:-1]:
                    if part not in currentLevel:
                        currentLevel[part] = {}
                    currentLevel = currentLevel[part]
                currentLevel[parts[-1]] = command
            else:
                log.error("'%s' needs to be a string", command)
        else:
            log.error("'%s' is missing required 'path' attribute", command)

    return commands
