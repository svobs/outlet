import logging
import file_util

import gi

gi.require_version("Gtk", "3.0")
from gi.repository import Gtk

# TODO: constants file
TOGGLE_UI_ENABLEMENT = 'toggle-ui-enable'


logger = logging.getLogger(__name__)


def _on_root_dir_selected(dialog, response_id, root_dir_panel):
    """Called after a directory is chosen via the RootDirChooserDialog"""
    open_dialog = dialog
    # if response is "ACCEPT" (the button "Open" has been clicked)
    if response_id == Gtk.ResponseType.OK:
        filename = open_dialog.get_filename()
        logger.info(f'User selected dir: {filename}')
        root_dir_panel.update_root(filename)
    # if response is "CANCEL" (the button "Cancel" has been clicked)
    elif response_id == Gtk.ResponseType.CANCEL:
        logger.debug("Cancelled: RootDirChooserDialog")
    elif response_id == Gtk.ResponseType.CLOSE:
        logger.debug("Closed: RootDirChooserDialog")
    elif response_id == Gtk.ResponseType.DELETE_EVENT:
        logger.debug("Deleted: RootDirChooserDialog")
    else:
        logger.error(f'Unrecognized response: {response_id}')
    # destroy the FileChooserDialog
    dialog.destroy()


class RootDirChooserDialog(Gtk.FileChooserDialog):
    def __init__(self, title, parent, current_dir):
        Gtk.FileChooserDialog.__init__(self, title=title, parent=parent, action=Gtk.FileChooserAction.SELECT_FOLDER)
        self.add_button(Gtk.STOCK_CANCEL, Gtk.ResponseType.CANCEL)
        self.add_button(Gtk.STOCK_OPEN, Gtk.ResponseType.OK)

        if current_dir is not None:
            self.set_current_folder(current_dir)

    def __call__(self):
        resp = self.run()
        self.hide()

        file_name = self.get_filename()

        d = self.get_current_folder()
        if d:
            self.set_current_folder(d)

        if resp == Gtk.ResponseType.OK:
            return file_name
        else:
            return None


class RootDirPanel:
    def __init__(self, parent_diff_tree):
        self.parent_diff_tree = parent_diff_tree
        self.content_box = Gtk.Box(spacing=6, orientation=Gtk.Orientation.HORIZONTAL)
        if parent_diff_tree.sizegroups is not None and parent_diff_tree.sizegroups.get('root_paths') is not None:
            parent_diff_tree.sizegroups['root_paths'].add_widget(self.content_box)
        # TODO: make Label editable (maybe switch it to an Entry) on click
        self.label = Gtk.Label(label='')
        self.update_root()
        self.label.set_justify(Gtk.Justification.LEFT)
        self.label.set_xalign(0)
        self.label.set_line_wrap(True)
        self.content_box.pack_start(self.label, expand=True, fill=True, padding=0)

        if self.parent_diff_tree.editable:
            self.change_btn = Gtk.Button(label='Change...')
            self.content_box.pack_start(self.change_btn, expand=False, fill=False, padding=0)
            self.change_btn.connect("clicked", self.on_change_btn_clicked, self.parent_diff_tree)

        self.parent_diff_tree.parent_win.connect(TOGGLE_UI_ENABLEMENT, self.set_enable_user_input)

    def on_change_btn_clicked(self, widget, diff_tree):
        # create a RootDirChooserDialog to open:
        # the arguments are: title of the window, parent_window, action,
        # (buttons, response)
        open_dialog = RootDirChooserDialog(title="Pick a directory", parent=diff_tree.parent_win, current_dir=diff_tree.root_path)

        # not only local files can be selected in the file selector
        open_dialog.set_local_only(False)
        # dialog always on top of the textview window
        open_dialog.set_modal(True)
        # connect the dialog with the callback function open_response_cb()
        open_dialog.connect("response", _on_root_dir_selected, self)
        # show the dialog
        open_dialog.show()

    def set_enable_user_input(self, window, enable):
        self.change_btn.set_sensitive(enable)

    def update_root(self, new_root_path=None):
        if new_root_path is None:
            # Use existing root path
            new_root_path = self.parent_diff_tree.root_path

        # If root is no longer valid for any reason, go back in the tree until it becomes valid
        # TODO: maybe just display an error indicator instead
        # TODO: disabled for now because it causes a race condition at startup
        #new_root_path = file_util.get_valid_or_ancestor(new_root_path)

        # This setter will automatically detect whether the path has changed, and handle any UI updates
        # and signal emissions appropriately:
        self.parent_diff_tree.root_path = new_root_path
        # Update root label:
        self.label.set_markup(f'<b>Tree Root:</b> <i>{new_root_path}</i>')
