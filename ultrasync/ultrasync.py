import sys
import gi
import logging

from task_runner import CentralTaskRunner

gi.require_version("Gtk", "3.0")
from gi.repository import GLib, Gtk, Gio
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

        self.add_main_option("test", ord("t"), GLib.OptionFlags.NONE,
                             GLib.OptionArg.NONE, "Command line test", None)

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

    # TODO: pass location of config from command line
    config = AppConfig()

    logger.debug(f'Main args: {sys.argv}')

    ui.assets.init(config)

    application = UltrasyncApplication(config)
    exit_status = application.run(sys.argv)
    sys.exit(exit_status)


if __name__ == '__main__':
    main()
