import logging

import gi
from gi.overrides import GLib
from pydispatch import dispatcher

from constants import FILTER_APPLY_DELAY_MS, IconId
from logging_constants import SUPER_DEBUG_ENABLED
from model.disp_tree.filter_criteria import FilterCriteria, Ternary
from signal_constants import Signal
from fe.gtk.dialog.base_dialog import BaseDialog
from util.has_lifecycle import HasLifecycle
from util.holdoff_timer import HoldOffTimer

gi.require_version("Gtk", "3.0")
from gi.repository import Gtk

logger = logging.getLogger(__name__)


class TreeFilterPanel(HasLifecycle):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS TreeFilterPanel
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """

    def __init__(self, parent_win, controller):
        HasLifecycle.__init__(self)
        self.parent_win: BaseDialog = parent_win
        self.con = controller
        self.content_box = Gtk.Box(spacing=0, orientation=Gtk.Orientation.HORIZONTAL)
        self._ui_enabled = True

        self._apply_filter_timer = HoldOffTimer(holdoff_time_ms=FILTER_APPLY_DELAY_MS, task_func=self._apply_filter_criteria)

        # A text entry for filtering
        self.search_entry = Gtk.Entry()
        self.search_entry.set_has_frame(True)
        self.search_entry.set_placeholder_text("Filter by name")
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
        # logger.debug(f'ICON SIZE: {self.toolbar.get_icon_size()}')

        # FIXME: put this into _redraw_panel()
        # self.supports_shared_status = self.con.get_root_spid().tree_type == TreeType.GDRIVE
        self.supports_shared_status = True

        self.show_ancestors_btn = self._add_toolbar_toggle_btn('Show ancestors of matches', IconId.ICON_FOLDER_TREE)
        self.match_case_btn = self._add_toolbar_toggle_btn('Match case', IconId.ICON_MATCH_CASE)
        self.is_trashed_btn = self._add_toolbar_toggle_btn('Is trashed', IconId.ICON_IS_TRASHED)
        if self.supports_shared_status:
            self.is_shared_btn = self._add_toolbar_toggle_btn('Is shared', IconId.ICON_IS_SHARED)

        filter_criteria: FilterCriteria = self.parent_win.backend.get_filter_criteria(self.con.tree_id)
        if not self.supports_shared_status:
            if filter_criteria.is_shared != Ternary.NOT_SPECIFIED:
                logger.info(f'[{self.con.tree_id}] Overriding previous filter for is_shared ({filter_criteria.is_shared}) '
                            f'because tree does not support shared status')
                # Override this. Since we're missing the button, having anything but NOT_SPECIFIED can result in unexpected behavior
                filter_criteria.is_shared = Ternary.NOT_SPECIFIED

        if filter_criteria.search_query:
            self.search_entry.set_text(filter_criteria.search_query)

        if not filter_criteria.ignore_case:
            self.match_case_btn.set_active(True)

        self._update_trashed_btn(filter_criteria)

        self._update_shared_btn(filter_criteria)

        if filter_criteria.show_ancestors_of_matches:
            self.show_ancestors_btn.set_active(True)

        self._latest_filter_criteria: FilterCriteria = filter_criteria

        # Wait until everything has been initialized before connecting listeners
        self.search_entry.connect("changed", self.update_filter_criteria)

        self.show_ancestors_btn.connect('clicked', self.update_filter_criteria)
        self.match_case_btn.connect('clicked', self.update_filter_criteria)
        self.is_trashed_btn.connect('clicked', self.update_filter_criteria)
        if self.supports_shared_status:
            self.is_shared_btn.connect('clicked', self.update_filter_criteria)

        self.start()

    def start(self):
        HasLifecycle.start(self)
        self.connect_dispatch_listener(signal=Signal.DISPLAY_TREE_CHANGED, receiver=self._on_display_tree_changed_filterpanel)
        logger.debug(f'[{self.con.tree_id}] Filter panel started')

    def _on_display_tree_changed_filterpanel(self, sender, tree):
        """Callback for Signal.DISPLAY_TREE_CHANGED"""
        if sender != self.con.tree_id:
            return

        logger.debug(f'[{sender}] Received signal "{Signal.DISPLAY_TREE_CHANGED.name}" with new root: {tree.get_root_spid()}')

        # Send the new tree directly to _redraw_panel(). Do not allow it to fall back to querying the controller for the tree,
        # because that would be a race condition:
        GLib.idle_add(self._redraw_panel, tree)

    def _redraw_panel(self, new_tree=None):
        # TODO: reset panel for new display tree

        pass
        # TODO: toggle filter panel on/off

    def _update_trashed_btn(self, filter_criteria):
        if filter_criteria.is_trashed == Ternary.NOT_SPECIFIED:
            self.is_trashed_btn.set_active(False)
            self._set_icon(self.is_trashed_btn, IconId.ICON_IS_TRASHED)
        elif filter_criteria.is_trashed == Ternary.TRUE:
            self.is_trashed_btn.set_active(True)
            self._set_icon(self.is_trashed_btn, IconId.ICON_IS_TRASHED)
        elif filter_criteria.is_trashed == Ternary.FALSE:
            self.is_trashed_btn.set_active(True)
            self._set_icon(self.is_trashed_btn, IconId.ICON_IS_NOT_TRASHED)
        else:
            assert False

        self.toolbar.show_all()
        logger.debug(f'[{self.con.tree_id}] Updated IsTrashed button with new state: {filter_criteria.is_trashed}')

    def _update_shared_btn(self, filter_criteria):
        if self.supports_shared_status:
            if filter_criteria.is_shared == Ternary.NOT_SPECIFIED:
                self._set_icon(self.is_shared_btn, IconId.ICON_IS_SHARED)
                self.is_shared_btn.set_active(False)
            elif filter_criteria.is_shared == Ternary.TRUE:
                self._set_icon(self.is_shared_btn, IconId.ICON_IS_SHARED)
                self.is_shared_btn.set_active(True)
            elif filter_criteria.is_shared == Ternary.FALSE:
                self._set_icon(self.is_shared_btn, IconId.ICON_IS_NOT_SHARED)
                self.is_shared_btn.set_active(True)
            else:
                assert False

            self.toolbar.show_all()
            logger.debug(f'[{self.con.tree_id}] Updated IsShared button with new state: {filter_criteria.is_shared}')

    def _add_toolbar_toggle_btn(self, entry_label: str, icon_id: IconId) -> Gtk.ToggleToolButton:
        btn = Gtk.ToggleToolButton()
        self._set_icon(btn, icon_id)
        btn.set_tooltip_text(entry_label)
        self.toolbar.insert(btn, -1)
        return btn

    def _set_icon(self, btn, icon_id: IconId):
        if SUPER_DEBUG_ENABLED:
            logger.debug(f'[{self.con.tree_id}] Setting icon to "{icon_id.name}"')
        icon = Gtk.Image()
        icon.set_from_file(self.parent_win.app.assets.get_path(icon_id))
        btn.set_icon_widget(icon)

    def _apply_filter_criteria(self):
        filter_criteria = self._latest_filter_criteria
        if filter_criteria:
            dispatcher.send(signal=Signal.FILTER_UI_TREE, sender=self.con.tree_id, filter_criteria=filter_criteria)

    def update_filter_criteria(self, widget=None):
        logger.debug(f'[{self.con.tree_id}] Updating filter criteria')

        search_query = self.search_entry.get_text()
        filter_criteria = FilterCriteria(search_query=search_query)

        filter_criteria.ignore_case = not self.match_case_btn.get_active()

        filter_criteria.show_ancestors_of_matches = self.show_ancestors_btn.get_active()

        # The tri-state buttons do not contain enough information to be derived from the UI, and must be inferred by a combination
        # of prev state and user action:
        filter_criteria.is_trashed = self._latest_filter_criteria.is_trashed
        filter_criteria.is_shared = self._latest_filter_criteria.is_shared

        if widget == self.is_trashed_btn:
            logger.debug(f'[{self.con.tree_id}] IsTrashed button clicked')
            prev_state: Ternary = filter_criteria.is_trashed
            if prev_state == Ternary.NOT_SPECIFIED:
                filter_criteria.is_trashed = Ternary.TRUE
            elif prev_state == Ternary.TRUE:
                filter_criteria.is_trashed = Ternary.FALSE
            elif prev_state == Ternary.FALSE:
                filter_criteria.is_trashed = Ternary.NOT_SPECIFIED
            self._update_trashed_btn(filter_criteria)

        elif self.supports_shared_status and widget == self.is_shared_btn:
            logger.debug(f'[{self.con.tree_id}] IsShared button clicked')
            prev_state: Ternary = filter_criteria.is_shared
            if prev_state == Ternary.NOT_SPECIFIED:
                filter_criteria.is_shared = Ternary.TRUE
            elif prev_state == Ternary.TRUE:
                filter_criteria.is_shared = Ternary.FALSE
            elif prev_state == Ternary.FALSE:
                filter_criteria.is_shared = Ternary.NOT_SPECIFIED
            self._update_shared_btn(filter_criteria)

        self._latest_filter_criteria = filter_criteria
        self._apply_filter_timer.start_or_delay()
