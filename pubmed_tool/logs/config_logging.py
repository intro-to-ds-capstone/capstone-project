"""
imported as logs

Contains functions specific to logger
use and functionality.

FUNCTIONS:
    get_logger(): creates a logger.
    display_message(): processes the
        messages to logger.

REQUIREMENTS/DEPENDENCIES: 
    logging

calls:
logging = get_logger()
"""

import logging

def get_logger(logger_name = __name__):
    """
    Creates a logger that can print to the console. 
    Records messages and warnings with standardized format.
    
    INPUTS:
        logger_name (string): name of the logger module, default
            is the name of the module/file
    RETURNS:
        logger (logger): Logger to handle recording of messages.

    REQUIREMENTS/DEPENDENCIES:
        logging
    """
    # Initiate the Logger
    # ========================================================================

    logger = logging.getLogger(logger_name)
    # Prohibit propogation to root, clear existing handlers
    # ------------------------------------------------------------------------
    logger.propagate = False
    logger.handlers.clear()

    # Set to record INFO or higher messages
    # ------------------------------------------------------------------------
    logger.setLevel(logging.INFO)

    # Create StreamHandler for Logger, to print to console.
    # ========================================================================
    _handler = logging.StreamHandler()
    _handler.setLevel(logging.INFO)

    # Add message formatting
    # ------------------------------------------------------------------------
    _formatter = logging.Formatter(
                    '%(asctime)s - %(levelname)s: %(message)s',
                    r'%Y-%m-%d %H:%M:%S'
                    )
    _handler.setFormatter(_formatter)

    # Add StreamHandler to Logger
    # ------------------------------------------------------------------------
    logger.addHandler(_handler)
    
    return logger

logger = get_logger()

def display_message(message, type = None):
    if type == 'info':
        logger.info(message)
    elif type == 'error':
        logger.error(message)
    elif type == 'debug':
        logger.debug(message)
    elif type == 'warning':
        logger.warning(message)
    else:
        logger.info(message)