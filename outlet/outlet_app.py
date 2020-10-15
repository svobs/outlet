import sys
import gi
from pydispatch import dispatcher

from executor.central import CentralExecutor
from index.uid.uid_generator import PersistentAtomicIntUidGenerator, UidGenerator
from model.node_identifier_factory import NodeIdentifierFactory
from ui import actions
from ui.actions import ID_DIFF_WINDOW

from index.cache_manager import CacheManager

import logging

from ui.two_pane_window import TwoPanelWindow
from app_config import AppConfig
import ui.assets

gi.require_version("Gtk", "3.0")
from gi.repository import Gtk, Gio

logger = logging.getLogger(__name__)

# CLASS OutletApplication
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼


class OutletApplication(Gtk.Application):

    """Main app.
    See: https://athenajc.gitbooks.io/python-gtk-3-api/content/gtk-group/gtkapplication.html"""
    def __init__(self, config):
        self.config = config
        Gtk.Application.__init__(self)
        self.assets = ui.assets.Assets(config)

        self.window = None
        self.shutdown: bool = False

        self.executor: CentralExecutor = CentralExecutor(self)

        self.uid_generator: UidGenerator = PersistentAtomicIntUidGenerator(self.config)
        self.node_identifier_factory: NodeIdentifierFactory = NodeIdentifierFactory(self)
        self.cacheman: CacheManager = CacheManager(self)

    def start(self):
        logger.info('Starting app')
        self.executor.start()

        # Kick off cache load now that we have a progress bar
        dispatcher.send(actions.START_CACHEMAN, sender=actions.ID_CENTRAL_EXEC)

    def quit(self):
        if self.shutdown:
            return

        logger.info('Shutting down app')
        self.shutdown = True

        # This will emit a cascade of events which will shut down the Executor too:
        if self.cacheman:
            self.cacheman.shutdown()
            self.cacheman = None

        # Just to be sure:
        if self.executor:
            self.executor.shutdown()
            self.executor = None

        if self.window:
            # swap into local var to prevent infinite cycle
            win = self.window
            self.window = None
            win.close()

        # Gtk.main_quit()
        logger.info('App shut down')

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
