import logging
import signal

from typing import Dict, Optional

from pydispatch import dispatcher

from be.backend_interface import OutletBackend
from be.uid.uid_generator import SimpleUidGenerator
from constants import TreeID
from signal_constants import Signal
from signal_constants import ID_MAIN_WINDOW
from ui.gtk.icon_store_gtk import IconStoreGtk
from ui.gtk.tree.controller import TreePanelController
from ui.gtk.two_pane_window import TwoPaneWindow

import gi
gi.require_version("Gtk", "3.0")
from gi.repository import Gtk, Gio, GLib

logger = logging.getLogger(__name__)


class OutletApplication(Gtk.Application):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS OutletApplication

    Main app.
    See: https://athenajc.gitbooks.io/python-gtk-3-api/content/gtk-group/gtkapplication.html
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, backend):
        Gtk.Application.__init__(self)
        self.backend: OutletBackend = backend
        self.assets = IconStoreGtk(backend)
        self.ui_uid_generator = SimpleUidGenerator()
        self._tree_controllers: Dict[str, TreePanelController] = {}
        """Keep track of live UI tree controllers, so that we can look them up by ID (e.g. for use in automated testing)"""
        self.window = None

    def start(self):
        logger.debug('Starting up app...')
        self.assets.load_all_icons()
        self.backend.start()

        dispatcher.connect(signal=Signal.DEREGISTER_DISPLAY_TREE, receiver=self._deregister_tree_controller)
        dispatcher.connect(signal=Signal.SHUTDOWN_APP, receiver=self.shutdown)

    def shutdown(self):
        logger.debug('Shutting down app...')
        self.backend.shutdown()

        try:
            if self._tree_controllers:
                for controller in list(self._tree_controllers.values()):
                    controller.shutdown()
                self._tree_controllers.clear()
        except (AttributeError, NameError):
            pass

        # This may take a long time if gRPC needs to time out
        self.quit()

    def do_activate(self):
        # We only allow a single window and raise any existing ones
        if not self.window:
            # Windows are associated with the app
            # when the last one is closed the app shuts down
            self.start()

            logger.debug(f'Creating main window')
            self.window = TwoPaneWindow(app=self, win_id=ID_MAIN_WINDOW)
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

        self.add_simple_action('quit', self.quit_callback)
        # See: https://developer.gnome.org/gtk3/stable/gtk3-Keyboard-Accelerators.html#gtk-accelerator-parse
        self.set_accels_for_action('app.quit', 'q')

    def quit_callback(self, action, parameter):
        logger.info("You chose Quit")
        self.shutdown()

    def add_simple_action(self, name, callback):
        action = Gio.SimpleAction.new(name)
        action.connect("activate", callback)
        self.add_action(action)

    # Tree controller tracking/lookup
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def register_tree_controller(self, controller: TreePanelController):
        logger.debug(f'[{controller.tree_id}] Registering controller')
        self._tree_controllers[controller.tree_id] = controller

    def _deregister_tree_controller(self, sender: str):
        # Sender is tree_id
        logger.debug(f'[{sender}] Deregistering controller in frontend')
        popped_con = self._tree_controllers.pop(sender, None)
        if not popped_con:
            logger.debug(f'Could not deregister controller; it was not found: {sender}')

    def get_tree_controller(self, tree_id: TreeID) -> Optional[TreePanelController]:
        return self._tree_controllers.get(tree_id, None)
