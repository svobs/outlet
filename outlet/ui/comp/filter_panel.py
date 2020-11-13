import logging

from gi.overrides import Pango

from ui.dialog.base_dialog import BaseDialog

import gi
gi.require_version("Gtk", "3.0")
from gi.repository import Gtk

logger = logging.getLogger(__name__)


#    CLASS TreeFilterPanel
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class TreeFilterPanel:

    def __init__(self, parent_win, controller, tree_id):
        self.parent_win: BaseDialog = parent_win
        self.con = controller
        assert type(tree_id) == str
        self.tree_id: str = tree_id
        self.cacheman = self.con.cacheman
        self.content_box = Gtk.Box(spacing=0, orientation=Gtk.Orientation.HORIZONTAL)
        self._ui_enabled = True

        # TODO: search history

        # A text entry for filtering
        self.search_entry = Gtk.Entry()
        self.search_entry.set_placeholder_text("Filter by name")
        self.search_entry.connect("changed", self.refresh_results)
        self.content_box.pack_start(self.search_entry, True, True, 0)

        # Add a checkbox for controlling subtree display
        self.subtree_checkbox = Gtk.CheckButton(label="Show subtrees")
        self.subtree_checkbox.connect("toggled", self.refresh_results)
        self.content_box.pack_start(self.subtree_checkbox, False, False, 0)

        self.trashed_checkbox = Gtk.CheckButton(label="Trashed")
        self.trashed_checkbox.connect("toggled", self.refresh_results)
        self.content_box.pack_start(self.trashed_checkbox, False, False, 0)

        self.shared_by_me_checkbox = Gtk.CheckButton(label="Shared by me")
        self.shared_by_me_checkbox.connect("toggled", self.refresh_results)
        self.content_box.pack_start(self.shared_by_me_checkbox, False, False, 0)

        self.shared_with_me_checkbox = Gtk.CheckButton(label="Shared with me")
        self.shared_with_me_checkbox.connect("toggled", self.refresh_results)
        self.content_box.pack_start(self.shared_with_me_checkbox, False, False, 0)

        # FIXME: add toggle buttons to GDrive status bar: Show Trashed, Show Shared By Me, Show Shared With Me

        # TODO: close box

    def refresh_results(self, _widget = None):
        # Apply filtering to results
        search_query = self.search_entry.get_text().lower()
        show_subtrees_of_matches = self.subtree_checkbox.get_active()
        # if search_query == "":
        #     self.tree_store.foreach(self.reset_row, True)
        #     if self.EXPAND_BY_DEFAULT:
        #         self.treeview.expand_all()
        #     else:
        #         self.treeview.collapse_all()
        # else:
        #     self.tree_store.foreach(self.reset_row, False)
        #     self.tree_store.foreach(self.show_matches, search_query, show_subtrees_of_matches)
        #     self.treeview.expand_all()
        # self.filter.refilter()

    def reset_row(self, model, path, iter, make_visible):
        # Reset some row attributes independent of row hierarchy
        self.tree_store.set_value(iter, self.COL_WEIGHT, Pango.Weight.NORMAL)
        self.tree_store.set_value(iter, self.COL_VISIBLE, make_visible)

    def make_path_visible(self, model, iter):
        # Make a row and its ancestors visible
        while iter:
            self.tree_store.set_value(iter, self.COL_VISIBLE, True)
            iter = model.iter_parent(iter)

    def make_subtree_visible(self, model, iter):
        # Make descendants of a row visible
        for i in range(model.iter_n_children(iter)):
            subtree = model.iter_nth_child(iter, i)
            if model.get_value(subtree, self.COL_VISIBLE):
                # Subtree already visible
                continue
            self.tree_store.set_value(subtree, self.COL_VISIBLE, True)
            self.make_subtree_visible(model, subtree)

    def show_matches(self, model, path, iter, search_query, show_subtrees_of_matches):
        text = model.get_value(iter, self.COL_TEXT).lower()
        if search_query in text:
            # Highlight direct match with bold
            self.tree_store.set_value(iter, self.COL_WEIGHT, Pango.Weight.BOLD)
            # Propagate visibility change up
            self.make_path_visible(model, iter)
            if show_subtrees_of_matches:
                # Propagate visibility change down
                self.make_subtree_visible(model, iter)
            return
