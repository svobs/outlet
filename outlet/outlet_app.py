import logging
import signal
import sys

import gi
from pydispatch import dispatcher

from app_config import AppConfig
from OutletBackend import OutletBackend
from OutletFrontend import OutletFrontend
from ui import actions
from ui.actions import ID_DIFF_WINDOW
from ui.two_pane_window import TwoPanelWindow

gi.require_version("Gtk", "3.0")
from gi.repository import Gtk, Gio, GLib

logger = logging.getLogger(__name__)

# CLASS OutletApplication
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼


class OutletApplication(Gtk.Application, OutletBackend, OutletFrontend):

    """Main app.
    See: https://athenajc.gitbooks.io/python-gtk-3-api/content/gtk-group/gtkapplication.html"""
    def __init__(self, config):
        self.config = config
        Gtk.Application.__init__(self)
        OutletBackend.__init__(self, config)
        OutletFrontend.__init__(self, config)

        self.window = None

    def start(self):
        OutletBackend.start(self)
        OutletFrontend.start(self)
        logger.info('Starting app')
        self.executor.start()

        # Kick off cache load now that we have a progress bar
        dispatcher.send(actions.START_CACHEMAN, sender=actions.ID_CENTRAL_EXEC)

    def quit(self):
        OutletBackend.shutdown(self)
        OutletFrontend.shutdown(self)

        logger.info('Shutting down app')

        if self.window:
            # swap into local var to prevent infinite cycle
            win = self.window
            self.window = None
            win.close()

    def do_activate(self):
        # We only allow a single window and raise any existing ones
        if not self.window:
            # Windows are associated with the app
            # when the last one is closed the app shuts down
            self.start()

            logger.debug(f'Creating main window')
            self.window = TwoPanelWindow(app=self, win_id=ID_DIFF_WINDOW)
            self.window.show_all()
            logger.debug(f'Finished window.show_all()')

            # Make sure that the application can be stopped from the terminal using Ctrl-C
            GLib.unix_signal_add(GLib.PRIORITY_DEFAULT, signal.SIGINT, Gtk.main_quit)

        self.window.present()

    def do_command_line(self, command_line):
        options = command_line.get_options_dict()
        # convert GVariantDict -> GVariant -> dict
        options = options.end().unpack()

        if "test" in options:
            # This is printed on the main instance
            logger.info("Test argument received: %s" % options["test"])

        self.activate()
        return 0

    def do_startup(self):
        Gtk.Application.do_startup(self)

        quit_action = Gio.SimpleAction.new("quit", None)
        quit_action.connect("activate", self.quit_callback)
        self.add_action(quit_action)
        # See: https://developer.gnome.org/gtk3/stable/gtk3-Keyboard-Accelerators.html#gtk-accelerator-parse
        self.set_accels_for_action('app.quit', 'q')

    def quit_callback(self, action, parameter):
        logger.info("You chose Quit")
        self.quit()

# ENTRY POINT MAIN
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼


def main():
    if sys.version_info[0] < 3:
        raise Exception("Python 3 or a more recent version is required.")

    if len(sys.argv) >= 2:
        config = AppConfig(sys.argv[1])
    else:
        config = AppConfig()

    app = OutletApplication(config)
    try:
        exit_status = app.run(sys.argv)
        sys.exit(exit_status)
    except KeyboardInterrupt:
        logger.info('Caught KeyboardInterrupt. Quitting')
        app.quit()


if __name__ == '__main__':
    main()
