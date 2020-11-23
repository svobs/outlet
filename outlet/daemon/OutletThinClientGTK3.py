import sys
import signal

import logging

from app_config import AppConfig
from daemon.OutletThinClient import OutletThinClient
from ui.actions import ID_DIFF_WINDOW
from ui.two_pane_window import TwoPanelWindow

import gi
gi.require_version("Gtk", "3.0")
from gi.repository import Gtk, Gio, GLib

logger = logging.getLogger(__name__)


# CLASS OutletThinClientGTK3
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class OutletThinClientGTK3(Gtk.Application):
    """GTK3 thin client which communicates with the OutletDaemon via GRPC."""
    def __init__(self, cfg):
        self.config = cfg
        Gtk.Application.__init__(self)
        self.frontend = OutletThinClient(cfg)
        self.backend = self.frontend.backend
        self.window = None

    def start(self):
        self.frontend.start()

    def shutdown(self):
        self.frontend.shutdown()

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

    def do_startup(self):
        Gtk.Application.do_startup(self)

        quit_action = Gio.SimpleAction.new("quit", None)
        quit_action.connect("activate", self.quit_callback)
        self.add_action(quit_action)
        # See: https://developer.gnome.org/gtk3/stable/gtk3-Keyboard-Accelerators.html#gtk-accelerator-parse
        self.set_accels_for_action('app.quit', 'q')

    def quit_callback(self, action, parameter):
        logger.info("You chose Quit")
        self.shutdown()

    @property
    def assets(self):
        return self.frontend.assets

def main():
    if sys.version_info[0] < 3:
        raise Exception("Python 3 or a more recent version is required.")

    if len(sys.argv) >= 2:
        cfg = AppConfig(sys.argv[1])
    else:
        cfg = AppConfig()

    thin_client = OutletThinClientGTK3(cfg)
    try:
        exit_status = thin_client.run(sys.argv)
        sys.exit(exit_status)
    except KeyboardInterrupt:
        logger.info('Caught KeyboardInterrupt. Quitting')
        thin_client.shutdown()


if __name__ == '__main__':
    main()
