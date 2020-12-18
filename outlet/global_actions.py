import logging

from pydispatch import dispatcher

from signal import Signal
from util.has_lifecycle import HasLifecycle

logger = logging.getLogger(__name__)


class GlobalActions(HasLifecycle):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS GlobalActions
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, backend):
        HasLifecycle.__init__(self)
        self.backend = backend

    def start(self):
        logger.debug('Starting GlobalActions listeners')
        HasLifecycle.start(self)

    def shutdown(self):
        HasLifecycle.shutdown(self)
        logger.debug('GlobalActions shut down')

    @staticmethod
    def display_error_in_ui(sender: str, msg: str, secondary_msg: str = None):
        logger.debug(f'Sender "{sender}" sent an error msg to display')
        dispatcher.send(signal=Signal.ERROR_OCCURRED, sender=sender, msg=msg, secondary_msg=secondary_msg)

    @staticmethod
    def disable_ui(sender):
        logger.debug(f'Sender "{sender}" requested to disable the UI')
        dispatcher.send(signal=Signal.TOGGLE_UI_ENABLEMENT, sender=sender, enable=False)

    @staticmethod
    def enable_ui(sender):
        logger.debug(f'Sender "{sender}" requested to enable the UI')
        dispatcher.send(signal=Signal.TOGGLE_UI_ENABLEMENT, sender=sender, enable=True)
