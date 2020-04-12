import sys
import gi
import logging
import file_util
gi.require_version("Gtk", "3.0")
from gi.repository import GLib, Gtk, Gio, GObject
from ui.diff_window import DiffWindow

DEBUG_LOG_FILE = file_util.get_resource_path('debug.log')
# NOTE: load the following regex into PyCharm:
# ^([\d-]+ [\d-:,.]+)\s+([\w.]+)\s*([\w]+)\s*(.*)$
# Capture groups: datetime=1 severity=3 category=2
LOG_FMT_DEBUG_FILE = '%(asctime)s %(name)20s %(levelname)-8s %(message)s'
LOG_DATE_FMT_DEBUG_FILE = '%Y-%m-%d %H:%M:%S.%03d'

LOG_FMT_CONSOLE = '%(asctime)s %(name)20s %(levelname)-8s %(message)s'
LOG_DATE_FMT_CONSOLE = '%H:%M:%S.%03d'

logger = logging.getLogger(__name__)


class UltrasyncApplication(Gtk.Application):
    """Main application.
    See: https://athenajc.gitbooks.io/python-gtk-3-api/content/gtk-group/gtkapplication.html"""
    def __init__(self):
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


def configure_logging():
    # filemode='w' == wipe out the prev log on each run
    logging.basicConfig(filename=DEBUG_LOG_FILE, filemode='w', format=LOG_FMT_DEBUG_FILE, level=logging.DEBUG)
    # create logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)

    # create console handler and set level to debug
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)

    # create formatter
    console_formatter = logging.Formatter(fmt=LOG_FMT_CONSOLE, datefmt=LOG_DATE_FMT_CONSOLE)

    # add formatter to ch
    console_handler.setFormatter(console_formatter)

    # add ch to logger
    root_logger.addHandler(console_handler)

    # TODO: figure out how to externalize this
    logging.getLogger('fmeta.fmeta').setLevel(logging.INFO)
    logging.getLogger('fmeta.diff_content_first').setLevel(logging.INFO)


def main():
    configure_logging()
    application = UltrasyncApplication()
    exit_status = application.run(sys.argv)
    sys.exit(exit_status)


if __name__ == '__main__':
    main()
