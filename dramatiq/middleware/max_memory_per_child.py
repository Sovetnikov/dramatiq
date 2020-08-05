import threading

from .middleware import Middleware, RestartWorker
from ..logging import get_logger

class MaxMemoryPerChild(Middleware):
    """Middleware that lets you configure the maximum number of memory a worker can take before it’s replaced by a new process
    Must be placed as last middleware in middlewares list
    Parameters:
      max_memory_per_child(int): Maximum memory in bytes a worker process can take before it’s replaced with a new one. Default is no limit.
    """

    def __init__(self, *, max_memory_per_child=None):
        try:
            import psutil
        except ImportError:
            raise Exception('psutil required for MaxMemoryPerChild middleware')
        self.logger = get_logger(__name__, type(self))
        self.max_memory_per_child = max_memory_per_child

    def after_process_message(self, broker, message, *, result=None, exception=None):
        if self.max_memory_per_child:
            import psutil
            import os
            process = psutil.Process(os.getpid())
            rss = process.memory_info().rss
            if rss >= self.max_tasks_per_child:
                    self.logger.debug("Max memory limit per child limit reached (%r), restarting worker process.", rss)
                    raise RestartWorker
