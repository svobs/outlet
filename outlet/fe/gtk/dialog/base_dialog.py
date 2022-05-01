import logging

import gi
gi.require_version("Gtk", "3.0")
from gi.repository import GLib, Gtk

logger = logging.getLogger(__name__)


class BaseDialog:
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS BaseDialog

    Base class for GTK dialogs
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, app):
        self.app = app

    @property
    def backend(self):
        return self.app.backend

    def show_error_ui(self, msg: str, secondary_msg: str = None):
        """Same as show_error_msg() but on UI thread"""
        def do_on_ui_thread(m, sm):
            GLib.idle_add(lambda: self.show_error_msg(m, sm))
        do_on_ui_thread(msg, secondary_msg)

    def show_error_msg(self, msg: str, secondary_msg=None):
        dialog = Gtk.MessageDialog(transient_for=self, modal=True, message_type=Gtk.MessageType.ERROR, buttons=Gtk.ButtonsType.CANCEL, text=msg)
        dialog.set_default_response(Gtk.ResponseType.CANCEL)
        if secondary_msg is None:
            logger.warning(f'Displaying error: {msg}')
        else:
            logger.warning(f'Displaying error: {msg}: {secondary_msg}')
            dialog.format_secondary_text(secondary_msg)

        def run_on_ui_thread():

            dialog.run()
            dialog.destroy()

        run_on_ui_thread()

    def show_question_dialog(self, msg, secondary_msg=None) -> bool:
        dialog = Gtk.MessageDialog(transient_for=self, modal=True, message_type=Gtk.MessageType.QUESTION, buttons=Gtk.ButtonsType.YES_NO, text=msg)
        if secondary_msg is None:
            logger.debug(f'Q: {msg}')
        else:
            logger.debug(f'Q: {msg}: {secondary_msg}')
            dialog.format_secondary_text(secondary_msg)
        try:
            response = dialog.run()
            if response == Gtk.ResponseType.YES:
                logger.debug("QUESTION dialog closed by clicking YES button")
                return True

            elif response == Gtk.ResponseType.NO:
                logger.debug("QUESTION dialog closed by clicking NO button")
                return False
        finally:
            dialog.destroy()
