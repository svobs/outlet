import logging
from typing import Optional

import gi

import constants
from constants import TreeDisplayMode
from model.display_id import Identifier
from model.fmeta_tree import FMetaTree
from model.subtree_snapshot import SubtreeSnapshot

gi.require_version("Gtk", "3.0")
from gi.repository import Gtk

from model.fmeta import Category
from ui.tree.lazy_display_strategy import LazyDisplayStrategy
from ui.tree.fmeta_action_handlers import FMetaTreeActionHandlers
from ui.tree.gdrive_action_handlers import GDriveActionHandlers
from ui.tree.fmeta_change_strategy import FMetaChangeTreeStrategy
from ui.comp.root_dir_panel import RootDirPanel

from ui.tree.controller import TreePanelController
from ui.tree.treeview_meta import TreeViewMeta
from ui.tree.display_store import DisplayStore
import ui.assets

logger = logging.getLogger(__name__)

"""
🡻🡻🡻 ① Static internal functinos 🡻🡻🡻
"""


def _build_status_bar():
    info_bar_container = Gtk.Box(spacing=6, orientation=Gtk.Orientation.HORIZONTAL)
    info_bar = Gtk.Label(label='')
    info_bar.set_justify(Gtk.Justification.LEFT)
    info_bar.set_line_wrap(True)
    info_bar_container.add(info_bar)
    return info_bar, info_bar_container


def _build_content_box(root_dir_panel, tree_view, status_bar_container):
    content_box = Gtk.Box(spacing=6, orientation=Gtk.Orientation.VERTICAL)

    content_box.pack_start(root_dir_panel, False, False, 5)

    # Tree will take up all the excess space
    tree_view.set_vexpand(True)
    tree_view.set_hexpand(False)
    tree_scroller = Gtk.ScrolledWindow()
    # No horizontal scrolling - only vertical
    tree_scroller.set_policy(Gtk.PolicyType.NEVER, Gtk.PolicyType.AUTOMATIC)
    tree_scroller.add(tree_view)
    # child, expand, fill, padding
    content_box.pack_start(tree_scroller, False, True, 5)

    content_box.pack_start(status_bar_container, False, True, 5)

    return content_box


def _compare_data(model, row1, row2, args):
    """
    Comparison function, for use in model sort by column.
    """
    treeview_meta = args[0]
    compare_field_func = args[1]

    sort_column, _ = model.get_sort_column_id()

    try:
        value1 = compare_field_func(model[row1][treeview_meta.col_num_data])
        value2 = compare_field_func(model[row2][treeview_meta.col_num_data])
        if not value1 or not value2:
            # This appears to achieve satisfactory behavior
            # (preserving previous column sort order for directories)
            return 0
        if value1 < value2:
            return -1
        elif value1 == value2:
            return 0
        else:
            return 1
    except AttributeError:
        # One of the objects is missing the attribute. No worry.
        return 0


def _build_treeview(display_store: DisplayStore):
    """ Builds the GTK3 treeview widget"""
    model: Gtk.TreeStore = display_store.model
    treeview_meta: TreeViewMeta = display_store.treeview_meta

    treeview = Gtk.TreeView(model=model)
    treeview.set_level_indentation(treeview_meta.extra_indent)
    treeview.set_show_expanders(True)
    treeview.set_property('enable_grid_lines', True)
    treeview.set_property('enable_tree_lines', True)
    treeview.set_fixed_height_mode(True)
    treeview.set_vscroll_policy(Gtk.ScrollablePolicy.NATURAL)
    # Allow click+drag to select multiple items.
    # May want to disable if using drag+drop
    treeview.set_rubber_banding(True)

    # 0 Checkbox + Icon + Name
    # See: https://stackoverflow.com/questions/27745585/show-icon-or-color-in-gtk-treeview-tree
    px_column = Gtk.TreeViewColumn(treeview_meta.col_names[treeview_meta.col_num_name])
    px_column.set_sizing(Gtk.TreeViewColumnSizing.FIXED)

    if treeview_meta.has_checkboxes:
        renderer = Gtk.CellRendererToggle()
        renderer.connect("toggled", display_store.on_cell_checkbox_toggled)
        renderer.set_fixed_size(width=-1, height=treeview_meta.row_height)
        px_column.pack_start(renderer, False)
        px_column.add_attribute(renderer, 'active', treeview_meta.col_num_checked)
        px_column.add_attribute(renderer, 'inconsistent', treeview_meta.col_num_inconsistent)

    px_renderer = Gtk.CellRendererPixbuf()
    px_renderer.set_fixed_size(width=-1, height=treeview_meta.row_height)
    px_column.pack_start(px_renderer, False)

    str_renderer = Gtk.CellRendererText()
    str_renderer.set_fixed_height_from_font(1)
    str_renderer.set_fixed_size(width=-1, height=treeview_meta.row_height)
    str_renderer.set_property('width-chars', 15)
    px_column.pack_start(str_renderer, False)

    # set data connector function/method

    # For displaying icons
    def get_tree_cell_pixbuf(col, cell, model, iter, user_data):
        cell.set_property('pixbuf', ui.assets.get_icon(model.get_value(iter, treeview_meta.col_num_icon)))

    px_column.set_cell_data_func(px_renderer, get_tree_cell_pixbuf)

    # For displaying text next to icon
    def get_tree_cell_text(col, cell, model, iter, user_data):
        cell.set_property('text', model.get_value(iter, treeview_meta.col_num_name))

    px_column.set_cell_data_func(str_renderer, get_tree_cell_text)

    px_column.set_min_width(50)
    px_column.set_expand(True)
    px_column.set_resizable(True)
    px_column.set_reorderable(True)
    px_column.set_sort_column_id(treeview_meta.col_num_name)
    treeview.append_column(px_column)

    if not treeview_meta.use_dir_tree:
        # DIRECTORY COLUMN
        renderer = Gtk.CellRendererText()
        renderer.set_fixed_height_from_font(1)
        renderer.set_fixed_size(width=-1, height=treeview_meta.row_height)
        renderer.set_property('width-chars', 20)
        column = Gtk.TreeViewColumn(treeview_meta.col_names[treeview_meta.col_num_dir], renderer, text=treeview_meta.col_num_dir)
        column.set_sort_column_id(treeview_meta.col_num_dir)

        column.set_sizing(Gtk.TreeViewColumnSizing.FIXED)
        column.set_min_width(50)
        # column.set_max_width(300)
        column.set_expand(True)
        column.set_resizable(True)
        column.set_reorderable(True)
        column.set_fixed_height_from_font(1)
        treeview.append_column(column)

    # SIZE COLUMN
    renderer = Gtk.CellRendererText()
    renderer.set_fixed_size(width=-1, height=treeview_meta.row_height)
    renderer.set_fixed_height_from_font(1)
    # renderer.set_property('width-chars', 15)
    column = Gtk.TreeViewColumn(treeview_meta.col_names[treeview_meta.col_num_size], renderer, text=treeview_meta.col_num_size)
    column.set_sort_column_id(treeview_meta.col_num_size)

    column.set_sizing(Gtk.TreeViewColumnSizing.FIXED)
    #  column.set_fixed_width(50)
    column.set_min_width(90)
    # column.set_max_width(300)
    column.set_expand(False)
    column.set_resizable(True)
    column.set_reorderable(True)
    treeview.append_column(column)

    # Need the original file sizes (in bytes) here, not the formatted one
    model.set_sort_func(treeview_meta.col_num_size, _compare_data, (treeview_meta, lambda f: f.size_bytes))

    # MODIFICATION TS COLUMN
    renderer = Gtk.CellRendererText()
    renderer.set_property('width-chars', 8)
    renderer.set_fixed_size(width=-1, height=treeview_meta.row_height)
    renderer.set_fixed_height_from_font(1)
    column = Gtk.TreeViewColumn(treeview_meta.col_names[treeview_meta.col_num_modification_ts], renderer, text=treeview_meta.col_num_modification_ts)
    column.set_sort_column_id(treeview_meta.col_num_modification_ts)

    column.set_sizing(Gtk.TreeViewColumnSizing.FIXED)
    # column.set_fixed_width(50)
    column.set_min_width(50)
    # column.set_max_width(300)
    column.set_expand(False)
    column.set_resizable(True)
    column.set_reorderable(True)
    treeview.append_column(column)

    model.set_sort_func(treeview_meta.col_num_modification_ts, _compare_data, (treeview_meta, lambda f: f.modify_ts))

    if treeview_meta.show_change_ts:
        # METADATA CHANGE TS COLUMN
        renderer = Gtk.CellRendererText()
        renderer.set_property('width-chars', 8)
        renderer.set_fixed_size(width=-1, height=treeview_meta.row_height)
        renderer.set_fixed_height_from_font(1)
        column = Gtk.TreeViewColumn(treeview_meta.col_names[treeview_meta.col_num_change_ts], renderer, text=treeview_meta.col_num_change_ts)
        column.set_sort_column_id(treeview_meta.col_num_change_ts)

        column.set_sizing(Gtk.TreeViewColumnSizing.FIXED)
        # column.set_fixed_width(50)
        column.set_min_width(50)
        # column.set_max_width(300)
        column.set_expand(False)
        column.set_resizable(True)
        column.set_reorderable(True)
        treeview.append_column(column)

        model.set_sort_func(treeview_meta.col_num_change_ts, _compare_data, (treeview_meta, lambda f: f.change_ts))

    select = treeview.get_selection()
    select.set_mode(treeview_meta.selection_mode)
    return treeview


def is_ignored_func(data_node):
    return data_node.category == Category.Ignored


"""
🡻🡻🡻 ② Generic case 🡻🡻🡻
"""


class TreeFactory:
    def __init__(self,
                 parent_win,
                 tree_id: str,
                 root: Optional[Identifier] = None,
                 tree: Optional[SubtreeSnapshot] = None
                 ):
        self.parent_win = parent_win

        self.root: Optional[Identifier] = root
        self.tree: Optional[SubtreeSnapshot] = tree
        """Choose one: tree or root"""

        self.tree_id: str = tree_id
        self.has_checkboxes: bool = False
        self.can_change_root: bool = False
        self.lazy_load: bool = True
        self.allow_multiple_selection: bool = False
        self.display_persisted: bool = False
        self.tree_display_mode: TreeDisplayMode = TreeDisplayMode.ONE_TREE_ALL_ITEMS

    def build(self):
        """Builds a single instance of a tree panel, and configures all its components as specified."""
        logger.debug(f'Building controller for tree: {self.tree_id}')

        if self.allow_multiple_selection:
            gtk_selection_mode = Gtk.SelectionMode.MULTIPLE
        else:
            gtk_selection_mode = Gtk.SelectionMode.SINGLE

        treeview_meta = TreeViewMeta(config=self.parent_win.config,
                                     tree_id=self.tree_id,
                                     has_checkboxes=self.has_checkboxes,
                                     can_change_root=self.can_change_root,
                                     tree_display_mode=TreeDisplayMode.ONE_TREE_ALL_ITEMS,
                                     lazy_load=self.lazy_load,
                                     selection_mode=gtk_selection_mode,
                                     is_display_persisted=self.display_persisted,
                                     is_ignored_func=is_ignored_func)

        display_store = DisplayStore(treeview_meta)

        # The controller holds all the components in memory. Important for listeners especially,
        # since they rely on weak references.
        controller = TreePanelController(self.parent_win, display_store, treeview_meta)

        if self.root:
            tree_type = self.root.tree_type
            controller.set_tree(root=self.root)
        elif self.tree:
            tree_type = self.tree.tree_type
            controller.set_tree(tree=self.tree)
        else:
            raise RuntimeError('"root" and "tree" are both empty!')

        if tree_type == constants.OBJ_TYPE_GDRIVE:
            display_strategy = LazyDisplayStrategy(config=self.parent_win.config)
            action_handlers = GDriveActionHandlers(config=self.parent_win.config)
        elif tree_type == constants.OBJ_TYPE_LOCAL_DISK:
            display_strategy = FMetaChangeTreeStrategy(config=self.parent_win.config)
            action_handlers = FMetaTreeActionHandlers(config=self.parent_win.config)
        else:
            raise RuntimeError(f'Unsupported tree type: {tree_type}')
        controller.display_strategy = display_strategy
        display_strategy.con = controller
        controller.action_handlers = action_handlers
        action_handlers.con = controller

        controller.tree_view = _build_treeview(display_store)
        controller.root_dir_panel = RootDirPanel(parent_win=self.parent_win,
                                                 tree_id=treeview_meta.tree_id,
                                                 current_root=controller.get_root_identifier(),
                                                 can_change_root=treeview_meta.can_change_root)

        controller.status_bar, status_bar_container = _build_status_bar()
        controller.content_box = _build_content_box(controller.root_dir_panel.content_box, controller.tree_view, status_bar_container)

        # Line up the following between trees if we are displaying side-by-side trees:
        if hasattr('parent_win', 'sizegroups'):
            if self.parent_win.sizegroups.get('tree_status'):
                self.parent_win.sizegroups['tree_status'].add_widget(status_bar_container)
            if self.parent_win.sizegroups.get('root_paths'):
                self.parent_win.sizegroups['root_paths'].add_widget(controller.root_dir_panel.content_box)

        controller.init()
        return controller


"""
🡻🡻🡻 ③ Specialized cases 🡻🡻🡻
"""


def build_gdrive(parent_win,
                 tree_id,
                 tree: SubtreeSnapshot):
    """Builds a tree panel for browsing a Google Drive tree, using lazy loading."""

    factory = TreeFactory(parent_win=parent_win, tree=tree, tree_id=tree_id)
    factory.allow_multiple_selection = False
    factory.display_persisted = False
    factory.has_checkboxes = False
    factory.can_change_root = False
    return factory.build()


def build_category_file_tree(parent_win,
                             tree_id: str,
                             root: Identifier = None,
                             tree: SubtreeSnapshot = None):

    factory = TreeFactory(parent_win=parent_win, root=root, tree=tree, tree_id=tree_id)
    factory.has_checkboxes = False  # not initially
    factory.can_change_root = True
    factory.allow_multiple_selection = True
    factory.display_persisted = True
    return factory.build()


def build_static_category_file_tree(parent_win, tree_id: str, tree: FMetaTree):
    # Whole tree is provided here
    factory = TreeFactory(parent_win=parent_win, tree=tree, tree_id=tree_id)
    factory.has_checkboxes = False
    factory.can_change_root = False
    factory.allow_multiple_selection = False
    factory.display_persisted = False
    factory.tree_display_mode = TreeDisplayMode.CHANGES_ONE_TREE_PER_CATEGORY
    factory.lazy_load = False
    return factory.build()

