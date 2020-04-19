from pydispatch import dispatcher
import logging

TOGGLE_UI_ENABLEMENT = 'toggle-ui-enablement'
ROOT_PATH_UPDATED = 'root-path-updated'
DO_DIFF = 'do-diff'
DOWNLOAD_GDRIVE_META = 'download-gdrive-meta'

SET_STATUS = 'set-status'
SET_TOTAL_PROGRESS = 'set-total-progress'
PROGRESS_MADE = 'progress_made'


logger = logging.getLogger(__name__)


def get_dispatcher():
    return dispatcher


def send_signal(signal, sender):
    """
    (Convenience method)
    Send a given signal from the given sender, with no additional args.
    """
    dispatcher.send(signal=signal, sender=sender)


def connect(signal, handler, sender=dispatcher.Any):
    dispatcher.connect(handler, signal=signal, sender=sender)


def disable_ui(sender):
    dispatcher.send(signal=TOGGLE_UI_ENABLEMENT, sender=sender, enable=False)


def enable_ui(sender):
    dispatcher.send(signal=TOGGLE_UI_ENABLEMENT, sender=sender, enable=True)


def set_status(sender, status_msg):
    dispatcher.send(signal=SET_STATUS, sender=sender, status_msg=status_msg)
