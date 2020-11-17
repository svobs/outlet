import logging
from typing import Optional

from gi.overrides import Pango
from pydispatch import dispatcher

from constants import H_PAD, HOLDOFF_TIME_MS, ICON_FOLDER_TREE, ICON_IS_NOT_SHARED, ICON_IS_NOT_TRASHED, ICON_MATCH_CASE, ICON_REFRESH, \
    ICON_IS_SHARED, ICON_IS_TRASHED, \
    TREE_TYPE_GDRIVE
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
        # no icon for now
        # pixbuf = self.parent_win.app.assets.get_icon(ICON_FOLDER_TREE)
        # self.search_entry.set_icon_from_pixbuf(Gtk.EntryIconPosition.PRIMARY, pixbuf)
        self.content_box.pack_start(self.search_entry, True, True, 0)

        self.toolbar = Gtk.Toolbar()
        self.toolbar.set_style(Gtk.ToolbarStyle.ICONS)
        self.toolbar.set_orientation(Gtk.Orientation.HORIZONTAL)
        self.toolbar.set_border_width(0)
        self.content_box.pack_end(self.toolbar, expand=False, fill=True, padding=0)
        # See icon size enum at: https://developer.gnome.org/gtk3/stable/gtk3-Themeable-Stock-Images.html
        # self.toolbar.set_icon_size(Gtk.IconSize.SMALL_TOOLBAR)  # 16px
        logger.debug(f'ICON SIZE: {self.toolbar.get_icon_size()}')

        self.supports_shared_status = self.con.get_root_identifier().tree_type == TREE_TYPE_GDRIVE

        self.show_ancestors_btn = self._add_toolbar_toggle_btn('Show ancestors of matches', ICON_FOLDER_TREE, self.refresh_results)
        self.match_case_btn = self._add_toolbar_toggle_btn('Match case', ICON_MATCH_CASE, self.refresh_results)
        self.is_trashed_btn = self._add_toolbar_toggle_btn('Is trashed', ICON_IS_TRASHED, self.refresh_results)
        if self.supports_shared_status:
            self.is_shared_btn = self._add_toolbar_toggle_btn('Is shared', ICON_IS_SHARED, self.refresh_results)

        filter_criteria: FilterCriteria = self.con.treeview_meta.filter_criteria
        if filter_criteria:
            if not self.supports_shared_status:
                if filter_criteria.is_shared != BoolOption.NOT_SPECIFIED:
                    logger.info(f'Overriding previous filter for is_shared ({filter_criteria.is_shared}) because tree does not support shared status')
                    # Override this. Since we're missing the button, having anything but NOT_SPECIFIED can result in unexpected behavior
                    filter_criteria.is_shared = BoolOption.NOT_SPECIFIED

            if filter_criteria.search_query:
                self.search_entry.set_text(filter_criteria.search_query)

            if not filter_criteria.ignore_case:
                self.match_case_btn.set_active(True)

            self._update_trashed_btn(filter_criteria)

            self._update_shared_btn(filter_criteria)

            if filter_criteria.show_subtrees_of_matches:
                self.show_ancestors_btn.set_active(True)

        self.toolbar.show_all()

        # TODO: close box

    def _update_trashed_btn(self, filter_criteria):
        if filter_criteria.is_trashed == BoolOption.NOT_SPECIFIED:
            self._set_icon(self.is_trashed_btn, ICON_IS_TRASHED)
            self.is_trashed_btn.set_active(False)
        elif filter_criteria.is_trashed == BoolOption.TRUE:
            self._set_icon(self.is_trashed_btn, ICON_IS_TRASHED)
            self.is_trashed_btn.set_active(True)
        elif filter_criteria.is_trashed == BoolOption.FALSE:
            self._set_icon(self.is_trashed_btn, ICON_IS_NOT_TRASHED)
            self.is_trashed_btn.set_active(True)
        else:
            assert False

        self.toolbar.show_all()

    def _update_shared_btn(self, filter_criteria):
        if self.supports_shared_status:
            if filter_criteria.is_shared == BoolOption.NOT_SPECIFIED:
                self._set_icon(self.is_shared_btn, ICON_IS_SHARED)
                self.is_shared_btn.set_active(False)
            elif filter_criteria.is_shared == BoolOption.TRUE:
                self._set_icon(self.is_shared_btn, ICON_IS_SHARED)
                self.is_shared_btn.set_active(True)
            elif filter_criteria.is_shared == BoolOption.FALSE:
                self._set_icon(self.is_shared_btn, ICON_IS_NOT_SHARED)
                self.is_shared_btn.set_active(True)
            else:
                assert False

            self.toolbar.show_all()

    def _add_toolbar_toggle_btn(self, entry_label: str, icon_name: str, on_clicked) -> Gtk.ToggleToolButton:
        btn = Gtk.ToggleToolButton()
        self._set_icon(btn, icon_name)
        btn.set_tooltip_text(entry_label)
        btn.connect('clicked', on_clicked)
        self.toolbar.insert(btn, -1)
        return btn

    def _set_icon(self, btn, icon_name):
        logger.debug(f'Setting icon to "{icon_name}"')
        icon = Gtk.Image()
        icon.set_from_file(self.parent_win.app.assets.get_path(icon_name))
        btn.set_icon_widget(icon)

    def _write_to_config(self):
        self.con.treeview_meta.write_filter_criteria_to_config()

    def refresh_results(self, widget=None):
        override_listener = False

        search_query = self.search_entry.get_text()
        filter_criteria = FilterCriteria(search_query=search_query)

        filter_criteria.ignore_case = not self.match_case_btn.get_active()

        filter_criteria.show_subtrees_of_matches = self.show_ancestors_btn.get_active()

        # The tri-state buttons do not contain enough information to be derived from the UI, and must be inferred by a combination
        # of prev state and user action:
        prev_filter_criteria: Optional[FilterCriteria] = self.con.treeview_meta.filter_criteria
        if prev_filter_criteria:
            filter_criteria.is_trashed = prev_filter_criteria.is_trashed
            filter_criteria.is_shared = prev_filter_criteria.is_shared

        if widget == self.is_trashed_btn:
            logger.debug('IsTrashed button clicked')
            override_listener = True
            prev_state: BoolOption = filter_criteria.is_trashed
            if prev_state == BoolOption.NOT_SPECIFIED:
                filter_criteria.is_trashed = BoolOption.TRUE
            elif prev_state == BoolOption.TRUE:
                filter_criteria.is_trashed = BoolOption.FALSE
            elif prev_state == BoolOption.FALSE:
                filter_criteria.is_trashed = BoolOption.NOT_SPECIFIED
            self._update_trashed_btn(filter_criteria)

        elif self.supports_shared_status and widget == self.is_shared_btn:
            logger.debug('IsShared button clicked')
            override_listener = True
            prev_state: BoolOption = filter_criteria.is_shared
            if prev_state == BoolOption.NOT_SPECIFIED:
                filter_criteria.is_shared = BoolOption.TRUE
            elif prev_state == BoolOption.TRUE:
                filter_criteria.is_shared = BoolOption.FALSE
            elif prev_state == BoolOption.FALSE:
                filter_criteria.is_shared = BoolOption.NOT_SPECIFIED
            self._update_shared_btn(filter_criteria)

        dispatcher.send(signal=actions.FILTER_UI_TREE, sender=self.tree_id, filter_criteria=filter_criteria)
        self._config_write_timer.start_or_delay()

        return override_listener

    def reset_row(self, model, path, iter, make_visible):
        # Reset some row attributes independent of row hierarchy
        self.tree_store.set_value(iter, self.COL_WEIGHT, Pango.Weight.NORMAL)
        self.tree_store.set_value(iter, self.COL_VISIBLE, make_visible)

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
