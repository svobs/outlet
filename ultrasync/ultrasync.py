import sys
import gi
import logging
gi.require_version("Gtk", "3.0")
from gi.repository import GLib, Gtk, Gio, GObject
from ui.diff_window import DiffWindow
from app_config import AppConfig

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


def configure_logging(config):
    # DEBUG LOG FILE
    debug_log_enabled = config.get('logging.debug_log.enable')
    if debug_log_enabled:
        debug_log_path = config.get('logging.debug_log.file_path')
        debug_log_format = config.get('logging.debug_log.format')
        debug_log_datetime_format = config.get('logging.debug_log.datetime_format')
        debug_log_mode = config.get('logging.debug_log.mode')
        logging.basicConfig(filename=debug_log_path, filemode=debug_log_mode,
                            format=debug_log_format, datefmt=debug_log_datetime_format,
                            level=logging.DEBUG)

    # create logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)

    # CONSOLE
    console_enabled = config.get('logging.console.enable')
    if console_enabled:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)

        console_format = config.get('logging.debug_log.format')
        console_datetime_format = config.get('logging.debug_log.datetime_format')
        console_formatter = logging.Formatter(fmt=console_format, datefmt=console_datetime_format)

    # add formatter to ch
    console_handler.setFormatter(console_formatter)

    # add ch to logger
    root_logger.addHandler(console_handler)

    # TODO: figure out how to externalize this
    logging.getLogger('fmeta.fmeta').setLevel(logging.INFO)
    logging.getLogger('fmeta.diff_content_first').setLevel(logging.INFO)


def main():
    if sys.version_info[0] < 3:
        raise Exception("Python 3 or a more recent version is required.")

    # TODO: pass location of config from command line
    config = AppConfig()

    configure_logging(config)
    application = UltrasyncApplication(config)
    exit_status = application.run(sys.argv)
    sys.exit(exit_status)


if __name__ == '__main__':
    main()
