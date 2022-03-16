import functools
from abc import ABC, abstractmethod
import logging
from typing import Callable, List, Optional

from pydispatch import dispatcher
from pydispatch.dispatcher import Any
from pydispatch.errors import DispatcherKeyError

from signal_constants import Signal

logger = logging.getLogger(__name__)


def start_func(func):
    """Decorator for "start" func of lifecycle.
    Should be able to use @start_func instead of calling HasLifecycle.start() & HasLifecycle.stop() and logging calls like below
    """

    @functools.wraps(func)
    def wrapper(obj_self, *args, **kwargs):
        logger.debug(f'[{obj_self.__class__.__name__}] Startup started')
        obj_self.start_lifecycle()
        retval = func(obj_self, *args, **kwargs)
        logger.debug(f'[{obj_self.__class__.__name__}] Startup done')
        return retval

    return wrapper


def stop_func(func):
    """Decorator for "shutdown" func of lifecycle.
    Should be able to use @stop_func instead of calling HasLifecycle.shutdown() & HasLifecycle.stop() and logging calls like below
    """

    @functools.wraps(func)
    def wrapper(obj_self, *args, **kwargs):
        logger.debug(f'[{obj_self.__class__.__name__}] Shutdown started')
        obj_self.shutdown_lifecycle()
        retval = func(obj_self, *args, **kwargs)
        logger.debug(f'[{obj_self.__class__.__name__}] Shutdown done')
        return retval

    return wrapper


class ListenerInfo:
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS ListenerInfo
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, signal: Signal, receiver: Callable, sender: Optional[str] = None):
        self.signal: Signal = signal
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
        self.was_shutdown = False

    def __del__(self):
        if self.was_shutdown:
            return

        self.shutdown()

    def connect_dispatch_listener(self, signal: Signal, receiver: Callable, sender: Optional[str] = None, weak=True):
        if not sender:
            sender = Any
        self._connected_listeners.append(ListenerInfo(signal, receiver, sender))
        logger.debug(f'CONNECTING: signal={signal.name} sender={sender} weak={weak}')
        dispatcher.connect(signal=signal, receiver=receiver, sender=sender, weak=weak)

    @staticmethod
    def disconnect_dispatch_listener(listener_info: ListenerInfo):
        try:
            logger.debug(f'DISCONNECTING: signal={listener_info.signal.name} sender={listener_info.sender}')
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

    def start_lifecycle(self):
        """Do not call this directly. It is called by the decorator"""
        self.connect_dispatch_listener(signal=Signal.SHUTDOWN_APP, receiver=self.shutdown_lifecycle)

    def shutdown_lifecycle(self):
        """Do not call this directly. It is called by the decorator"""
        self.disconnect_all_listeners()
        self.was_shutdown = True

    def start(self):
        self.start_lifecycle()  # for backwards compatibility only. Future child classes should not need to call super.start().

    def shutdown(self):
        """
        Note to self: need to stop asking myself why I don't name this method "stop". This just feels more right, ok?
        """
        self.shutdown_lifecycle()  # for backwards compatibility only. Future child classes should not need to call super.start().
