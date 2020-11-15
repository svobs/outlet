import logging

from gi.overrides import Pango
from pydispatch import dispatcher

from constants import H_PAD, HOLDOFF_TIME_MS, ICON_FOLDER_TREE, ICON_MATCH_CASE, ICON_REFRESH, ICON_SHARED, ICON_TRASHED
from ui import actions
from ui.dialog.base_dialog import BaseDialog

import gi

from ui.tree.filter_criteria import BoolOption, FilterCriteria
from util.holdoff_timer import HoldOffTimer

gi.require_version("Gtk", "3.0")
from gi.repository import Gtk

logger = logging.getLogger(__name__)


#    CLASS TreeFilterPanel
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class TreeFilterPanel:

    def __init__(self, parent_win, controller):
        self.parent_win: BaseDialog = parent_win
        self.con = controller
        self.tree_id: str = self.con.tree_id
        self.cacheman = self.con.cacheman
        self.content_box = Gtk.Box(spacing=0, orientation=Gtk.Orientation.HORIZONTAL)
        self._ui_enabled = True

        self._config_write_timer = HoldOffTimer(holdoff_time_ms=HOLDOFF_TIME_MS, task_func=self._write_to_config)

        # TODO: search history

        # A text entry for filtering
        self.search_entry = Gtk.Entry()
        self.search_entry.set_has_frame(True)
        self.search_entry.set_placeholder_text("Filter by name")
        self.search_entry.connect("changed", self.refresh_results)
        self.content_box.pack_start(self.search_entry, True, True, 0)

        self.toolbar = Gtk.Toolbar()
        self.toolbar.set_style(Gtk.ToolbarStyle.ICONS)
        self.toolbar.set_orientation(Gtk.Orientation.HORIZONTAL)
        self.toolbar.set_border_width(5)
        self.content_box.pack_end(self.toolbar, expand=False, fill=True, padding=H_PAD)
        # See icon size enum at: https://developer.gnome.org/gtk3/stable/gtk3-Themeable-Stock-Images.html
        # self.toolbar.set_icon_size(Gtk.IconSize.SMALL_TOOLBAR)  # 16px
        logger.debug(f'ICON SIZE: {self.toolbar.get_icon_size()}')

        self.show_ancestors_btn = self._add_toolbar_toggle_btn('Show ancestors of matches', ICON_FOLDER_TREE, self.refresh_results)
        self.match_case_btn = self._add_toolbar_toggle_btn('Match case', ICON_MATCH_CASE, self.refresh_results)
        self.is_trashed_btn = self._add_toolbar_toggle_btn('Is trashed', ICON_TRASHED, self.refresh_results)
        self.is_shared_btn = self._add_toolbar_toggle_btn('Is shared', ICON_SHARED, self.refresh_results)

        self.toolbar.show_all()

        filter_criteria: FilterCriteria = self.con.treeview_meta.filter_criteria
        if filter_criteria:
            if filter_criteria.search_query:
                self.search_entry.set_text(filter_criteria.search_query)
            if not filter_criteria.ignore_case:
                self.match_case_btn.set_active(True)
            if filter_criteria.is_trashed == BoolOption.TRUE:
                self.is_trashed_btn.set_active(True)
            if filter_criteria.is_shared == BoolOption.TRUE:
                self.is_shared_btn.set_active(True)
            if filter_criteria.show_subtrees_of_matches:
                self.show_ancestors_btn.set_active(True)

        # TODO: close box

    def _add_toolbar_toggle_btn(self, entry_label: str, icon_name: str, on_clicked):
        btn = Gtk.ToggleToolButton()
        icon = Gtk.Image()
        icon.set_from_file(self.parent_win.app.assets.get_path(icon_name))
        btn.set_icon_widget(icon)
        btn.set_tooltip_text(entry_label)
        btn.connect('clicked', on_clicked)
        self.toolbar.insert(btn, -1)
        return btn

    def _write_to_config(self):
        self.con.treeview_meta.write_filter_criteria_to_config()

    def _get_filter_criteria_from_ui(self) -> FilterCriteria:
        search_query = self.search_entry.get_text()
        filter_criteria = FilterCriteria(search_query=search_query)

        filter_criteria.ignore_case = not self.match_case_btn.get_active()

        filter_criteria.show_subtrees_of_matches = self.show_ancestors_btn.get_active()

        if self.is_trashed_btn.get_active():
            filter_criteria.is_trashed = BoolOption.TRUE

        if self.is_shared_btn.get_active():
            filter_criteria.is_shared = BoolOption.TRUE

        return filter_criteria

    def refresh_results(self, widget=None):
        # Apply filtering to results
        filter_criteria: FilterCriteria = self._get_filter_criteria_from_ui()

        dispatcher.send(signal=actions.FILTER_UI_TREE, sender=self.tree_id, filter_criteria=filter_criteria)

        self._config_write_timer.start_or_delay()

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
