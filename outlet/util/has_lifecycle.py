from abc import ABC, abstractmethod

from pydispatch import dispatcher
from pydispatch.errors import DispatcherKeyError

from ui import actions


class HasLifecycle(ABC):

    def __del__(self):
        self.shutdown()

    def start(self):
        dispatcher.connect(signal=actions.SHUTDOWN_APP, receiver=self.shutdown)

    def shutdown(self):
        try:
            dispatcher.disconnect(signal=actions.SHUTDOWN_APP, receiver=self.shutdown)
        except DispatcherKeyError:
            pass
