import logging
import time
from contextlib import AbstractContextManager


class log_time(AbstractContextManager):
    def __init__(self, logger=None, message=None):
        self.logger = logger
        self.message = message
        self.start_time = None
        self.duration = None

    def __enter__(self):
        self.start_time = time.perf_counter()
        return self

    def __exit__(self, *exc):
        self.duration = time.perf_counter() - self.start_time
        if self.message:
            if isinstance(self.logger, str):
                logger = logging.getLogger(self.logger)
            elif self.logger is None:
                logger = logging.getLogger("xcube")
            else:
                logger = self.logger
            logger.info(f"{self.message} completed within %.2f ms" % (self.duration * 1000))

