import logging
import time
from contextlib import AbstractContextManager


class log_time(AbstractContextManager):
    def __init__(self, logger, message):
        self.logger = logging.getLogger() if isinstance(logger, str) else logger
        self.message = message
        self.start_time = None

    def __enter__(self):
        self.start_time = time.clock()
        return self

    def __exit__(self, *exc):
        duration = time.clock() - self.start_time
        self.logger.info(f"{self.message} within {duration} seconds")
        return duration
