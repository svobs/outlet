import signal
import sys
import logging
from typing import Dict

from pydispatch import dispatcher

from executor.central import CentralExecutor
from store.uid.uid_generator import PersistentAtomicIntUidGenerator, UidGenerator
from model.node_identifier_factory import NodeIdentifierFactory
from ui import actions
from ui.actions import ID_DIFF_WINDOW

from store.cache_manager import CacheManager
from ui.tree.controller import TreePanelController

from ui.two_pane_window import TwoPanelWindow
from app_config import AppConfig
import ui.assets

import gi
gi.require_version("Gtk", "3.0")
from gi.repository import Gtk, Gio, GLib

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

        self._tree_controllers: Dict[str, TreePanelController] = {}
        """Keep track of live UI tree controllers, so that we can look them up by ID (e.g. for use in automated testing)"""

        self.window = None
        self._shutdown_requested: bool = False

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
        if self._shutdown_requested:
            return

        logger.info('Shutting down app')
        self._shutdown_requested = True

        dispatcher.send(actions.SHUTDOWN_APP, sender=actions.ID_CENTRAL_EXEC)

        try:
            if self._tree_controllers:
                for controller in list(self._tree_controllers.values()):
                    controller.destroy()
                self._tree_controllers.clear()
        except NameError:
            pass

        self.cacheman = None
        self.executor = None
        if self.window:
            # swap into local var to prevent infinite cycle
            win = self.window
            self.window = None
            win.close()

        # Gtk.main_quit()
        logger.info('App shut down')

    # Tree controller tracking/lookup
    # ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

    def register_tree_controller(self, controller: TreePanelController):
        logger.debug(f'[{controller.tree_id}] Registering controller')
        self._tree_controllers[controller.tree_id] = controller

    def unregister_tree_controller(self, controller: TreePanelController):
        logger.debug(f'[{controller.tree_id}] Unregistering controller')
        popped_con = self._tree_controllers.pop(controller.tree_id, None)
        if popped_con:
            if self._is_live_capture_enabled and self._live_monitor:
                self._live_monitor.stop_capture(controller.tree_id)
        else:
            logger.debug(f'Could not unregister TreeController; it was not found: {controller.tree_id}')

    def get_tree_controller(self, tree_id: str) -> Optional[TreePanelController]:
        return self._tree_controllers.get(tree_id, None)

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
