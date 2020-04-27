import sys
import gi

from cache.cache_manager import CacheManager

gi.require_version("Gtk", "3.0")
from gi.repository import Gtk, Gio
import logging

from global_actions import GlobalActions
from task_runner import CentralTaskRunner

from ui.diff_tree.diff_window import DiffWindow
from app_config import AppConfig
import ui.assets

logger = logging.getLogger(__name__)


class UltrasyncApplication(Gtk.Application):

    """Main application.
    See: https://athenajc.gitbooks.io/python-gtk-3-api/content/gtk-group/gtkapplication.html"""
    def __init__(self, config):
        self.config = config
        Gtk.Application.__init__(self)
        self.window = None

        self.task_runner = CentralTaskRunner(self)
        self.cache_manager = CacheManager(self)
        self.global_actions = GlobalActions(self)
        self.global_actions.init()

    def do_activate(self):
        # We only allow a single window and raise any existing ones
        if not self.window:
            # Windows are associated with the application
            # when the last one is closed the application shuts down
            self.window = DiffWindow(application=self)
            self.window.show_all()

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


def main():
    if sys.version_info[0] < 3:
        raise Exception("Python 3 or a more recent version is required.")

    if len(sys.argv) >= 2:
        config = AppConfig(sys.argv[1])
    else:
        config = AppConfig()

    ui.assets.init(config)

    application = UltrasyncApplication(config)
    exit_status = application.run(sys.argv)
    sys.exit(exit_status)


if __name__ == '__main__':
    main()
