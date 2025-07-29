"""
Class for general configurations
"""

import logging
import os
from pathlib import Path
import sys

# Project root added to the sys.path, so that scripts can be run unpackaged as well as packaged.
sys.path.append(str(Path(__file__).resolve().parent.parent))

# Local libraries


class Config:
    """
    Configuration object, to share parameters across functions.
    Primarily for logging and standardizing parameters when requesting
    resources across domains.
    """

    def __init__(
        self,
        debug=False,
        log="./log",
        verbose=False,
        pause=5,
        user_agent=None,
    ):
        """
        Inputs:
         - debug (bool): Whether to set log.log at DEBUG level (default is INFO)
         - log (str): Relative or absolute path to directory for saving log files.
         - verbose (bool): Whether to output to terminal (at INFO level)
         - pause (int): Base number of seconds between requests to non-loc.gov domains.
         - user_agent(str): User-Agent header for non-loc.gov requests.

        Returns:
         - locgov_data.Config
        """

        self.debug = debug
        self.log = log
        self.verbose = verbose
        self.pause = pause
        self.user_agent = user_agent
        self.logger = self._setup_logging()

    def _setup_logging(self):
        """
        Sets up: log.log, error.log.
        """

        # Clean attributes
        if self.log.endswith("/") is False:
            self.log = self.log + "/"

        logger = logging.getLogger(__name__)

        # Skip set up if already set up
        if logger.handlers:
            return logger

        # Basic logger configurations
        if self.debug is True:
            logger.setLevel(logging.DEBUG)
        else:
            logger.setLevel(logging.INFO)
        logger.propagate = True

        # Create log dir, if it doesn't exist
        if not os.path.exists(self.log):
            os.makedirs(self.log)

        # File handler: log.log
        lh = logging.FileHandler(f"{self.log}log.log", "a")
        lh.setLevel(logger.level)
        formatter = logging.Formatter("%(asctime)s - %(levelname)s: %(message)s")
        lh.setFormatter(formatter)
        logger.addHandler(lh)

        # File handler: error.log
        eh = logging.FileHandler(f"{self.log}error.log", "a")
        eh.setLevel(logging.WARNING)
        formatter = logging.Formatter("%(asctime)s - %(levelname)s: %(message)s")
        eh.setFormatter(formatter)
        logger.addHandler(eh)

        # Stream handler: terminal
        if self.verbose is True:
            sh = logging.StreamHandler()
            sh.setLevel(logger.level)
            formatter = logging.Formatter("%(asctime)s - %(levelname)s: %(message)s")
            sh.setFormatter(formatter)
            logger.addHandler(sh)
        else:
            sh = logging.StreamHandler()
            sh.setLevel(logging.WARNING)
            formatter = logging.Formatter("%(asctime)s - %(levelname)s: %(message)s")
            sh.setFormatter(formatter)
            logger.addHandler(sh)

        return logger
