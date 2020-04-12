import logging
import gi
gi.require_version("Gtk", "3.0")
from gi.repository import GLib, Gtk, Gio, GObject

logger = logging.getLogger(__name__)


class BaseDialog:
    def __init__(self):
        pass

    def show_error_ui(self, msg, secondary_msg=None):
        def do_on_ui_thread(m, sm):
            GLib.idle_add(lambda: self.show_error_msg(m, sm))
        do_on_ui_thread(msg, secondary_msg)

    def show_error_msg(self, msg, secondary_msg=None):
        dialog = Gtk.MessageDialog(parent=self, modal=True, message_type=Gtk.MessageType.ERROR, buttons=Gtk.ButtonsType.CANCEL, text=msg)
        if secondary_msg is None:
            logger.debug(f'Displaying user error: {msg}')
        else:
            logger.debug(f'Displaying user error: {msg}: {secondary_msg}')
            dialog.format_secondary_text(secondary_msg)

        def run_on_ui_thread():
            dialog.run()
            dialog.destroy()

        run_on_ui_thread()

    def on_question_clicked(self, msg, secondary_msg=None):
        dialog = Gtk.MessageDialog(parent=self, modal=True, message_type=Gtk.MessageType.QUESTION, buttons=Gtk.ButtonsType.YES_NO, text=msg)
        if secondary_msg is None:
            logger.debug(f'Q: {msg}')
        else:
            logger.debug(f'Q: {msg}: {secondary_msg}')
            dialog.format_secondary_text(secondary_msg)
        response = dialog.run()
        if response == Gtk.ResponseType.YES:
            logger.debug("QUESTION dialog closed by clicking YES button")
        elif response == Gtk.ResponseType.NO:
            logger.debug("QUESTION dialog closed by clicking NO button")

        dialog.destroy()
