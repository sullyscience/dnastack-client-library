import sys
from imagination.decorator import service
from threading import Lock


@service.registered()
class Console:
    """
    Virtual Console

    This class is a workaround to allow external processes capture the output through it.
    """
    def __init__(self):
        self.__output_lock = Lock()

    def print(self, content, end='\n', to_stderr = False):
        with self.__output_lock:
            if to_stderr:
                sys.stderr.write(content + end)
                sys.stderr.flush()
            else:
                print(content, end=end)
                sys.stdout.flush()