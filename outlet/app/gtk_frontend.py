import logging
import signal
from typing import Dict, Optional

from pydispatch import dispatcher

from app.backend import OutletBackend
from model.node_identifier_factory import NodeIdentifierFactory
from ui import actions
from ui.actions import ID_DIFF_WINDOW
from ui.tree.controller import TreePanelController
from ui.two_pane_window import TwoPanelWindow

import gi
gi.require_version("Gtk", "3.0")
from gi.repository import Gtk, Gio, GLib

import ui.assets

logger = logging.getLogger(__name__)

# CLASS OutletApplication
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼


class OutletApplication(Gtk.Application):

    """Main app.
    See: https://athenajc.gitbooks.io/python-gtk-3-api/content/gtk-group/gtkapplication.html"""
    def __init__(self, config, backend):
        self.config = config
        Gtk.Application.__init__(self)
        self.backend: OutletBackend = backend
        self.assets = ui.assets.Assets(config)
        self._tree_controllers: Dict[str, TreePanelController] = {}
        """Keep track of live UI tree controllers, so that we can look them up by ID (e.g. for use in automated testing)"""
        self.window = None

    def start(self):
        logger.debug('Starting up app...')
        self.backend.start()

        dispatcher.connect(signal=actions.DEREGISTER_DISPLAY_TREE, receiver=self._deregister_tree_controller)

    def shutdown(self):
        logger.debug('Shutting down app...')
        self.backend.shutdown()

        try:
            if self._tree_controllers:
                for controller in list(self._tree_controllers.values()):
                    controller.destroy()
                self._tree_controllers.clear()
        except NameError:
            pass


    # TODO: replace all uses of this with APIs in Backend
    @property
    def cacheman(self):
        return self.backend.cacheman

    # TODO: replace all uses of this with APIs in Backend
    @property
    def uid_generator(self):
        return self.backend.uid_generator

    # TODO: replace all uses of this with APIs in Backend
    @property
    def executor(self):
        return self.backend.executor

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
        self.shutdown()

    # Tree controller tracking/lookup
    # ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

    def register_tree_controller(self, controller: TreePanelController):
        logger.debug(f'[{controller.tree_id}] Registering controller')
        self._tree_controllers[controller.tree_id] = controller

    def _deregister_tree_controller(self, sender: str):
        # Sender is tree_id
        logger.debug(f'[{sender}] Deregistering controller in frontend')
        popped_con = self._tree_controllers.pop(sender, None)
        if not popped_con:
            logger.debug(f'Could not deregister controller; it was not found: {sender}')

    def get_tree_controller(self, tree_id: str) -> Optional[TreePanelController]:
        return self._tree_controllers.get(tree_id, None)