from abc import ABC, abstractmethod
import logging
from typing import Callable, List, Optional

from pydispatch import dispatcher
from pydispatch.dispatcher import Any
from pydispatch.errors import DispatcherKeyError

from ui import actions

logger = logging.getLogger(__name__)


class ListenerInfo:
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS ListenerInfo
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, signal: str, receiver: Callable, sender: Optional[str] = None):
        self.signal: str = signal
        self.receiver: Callable = receiver
        self.sender: Optional[str] = sender


class HasLifecycle(ABC):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS HasLifecycle
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self):
        self._connected_listeners: List[ListenerInfo] = []

    def __del__(self):
        self.shutdown()

    def connect_dispatch_listener(self, signal: str, receiver: Callable, sender: Optional[str] = None, weak=True):
        if not sender:
            sender = Any
        self._connected_listeners.append(ListenerInfo(signal, receiver, sender))
        logger.debug(f'CONNECTING: signal={signal} sender={sender} weak={weak}')
        dispatcher.connect(signal=signal, receiver=receiver, sender=sender, weak=weak)

    @staticmethod
    def disconnect_dispatch_listener(listener_info: ListenerInfo):
        try:
            dispatcher.disconnect(signal=listener_info.signal, receiver=listener_info.receiver, sender=listener_info.sender)
        except DispatcherKeyError:
            pass

    def disconnect_listeners(self, signal_list: List[str]):
        """NOTE: Not thread safe!"""
        new_connected_listeners = []

        for listener_info in self._connected_listeners:
            if listener_info.signal in signal_list:
                self.disconnect_dispatch_listener(listener_info)
            else:
                new_connected_listeners.append(listener_info)

        self._connected_listeners = new_connected_listeners

    def disconnect_all_listeners(self):
        connected_listeners = self._connected_listeners
        self._connected_listeners = []

        for listener_info in connected_listeners:
            self.disconnect_dispatch_listener(listener_info)

    def start(self):
        self.connect_dispatch_listener(signal=actions.SHUTDOWN_APP, receiver=self.shutdown)

    def shutdown(self):
        self.disconnect_all_listeners()
