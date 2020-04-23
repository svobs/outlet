import logging

import ui.assets
from fmeta.fmeta import Category
from ui.diff_tree.fmeta_action_handlers import FMetaTreeActionHandlers
from ui.diff_tree.gdrive_action_handlers import GDriveActionHandlers
from ui.tree.fmeta_change_strategy import FMetaChangeTreeStrategy
from ui.tree.action_bridge import TreeActionBridge
from ui.root_dir_panel import RootDirPanel

import gi

from ui.tree.controller import TreePanelController
from ui.tree.display_meta import TreeDisplayMeta
from ui.tree.display_store import DisplayStore
from ui.tree.lazy_load_strategy import LazyLoadStrategy

gi.require_version("Gtk", "3.0")
from gi.repository import Gtk

logger = logging.getLogger(__name__)


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
    display_meta = args[0]
    compare_field_func = args[1]

    sort_column, _ = model.get_sort_column_id()

    try:
        value1 = compare_field_func(model[row1][display_meta.col_num_data])
        value2 = compare_field_func(model[row2][display_meta.col_num_data])
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


def _build_treeview(display_store):
    """ Builds the GTK3 treeview widget"""
    model = display_store.model
    display_meta = display_store.display_meta

    treeview = Gtk.TreeView(model=model)
    treeview.set_level_indentation(display_meta.extra_indent)
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
    px_column = Gtk.TreeViewColumn(display_meta.col_names[display_meta.col_num_name])
    px_column.set_sizing(Gtk.TreeViewColumnSizing.FIXED)

    if display_meta.editable:
        renderer = Gtk.CellRendererToggle()
        renderer.connect("toggled", display_store.on_cell_checkbox_toggled)
        renderer.set_fixed_size(width=-1, height=display_meta.row_height)
        px_column.pack_start(renderer, False)
        px_column.add_attribute(renderer, 'active', display_meta.col_num_checked)
        px_column.add_attribute(renderer, 'inconsistent', display_meta.col_num_inconsistent)

    px_renderer = Gtk.CellRendererPixbuf()
    px_renderer.set_fixed_size(width=-1, height=display_meta.row_height)
    px_column.pack_start(px_renderer, False)

    str_renderer = Gtk.CellRendererText()
    str_renderer.set_fixed_height_from_font(1)
    str_renderer.set_fixed_size(width=-1, height=display_meta.row_height)
    str_renderer.set_property('width-chars', 15)
    px_column.pack_start(str_renderer, False)

    # set data connector function/method

    # For displaying icons
    def get_tree_cell_pixbuf(col, cell, model, iter, user_data):
        cell.set_property('pixbuf', ui.assets.get_icon(model.get_value(iter, display_meta.col_num_icon)))
    px_column.set_cell_data_func(px_renderer, get_tree_cell_pixbuf)

    # For displaying text next to icon
    def get_tree_cell_text(col, cell, model, iter, user_data):
        cell.set_property('text', model.get_value(iter, display_meta.col_num_name))
    px_column.set_cell_data_func(str_renderer, get_tree_cell_text)

    px_column.set_min_width(50)
    px_column.set_expand(True)
    px_column.set_resizable(True)
    px_column.set_reorderable(True)
    px_column.set_sort_column_id(display_meta.col_num_name)
    treeview.append_column(px_column)

    if not display_meta.use_dir_tree:
        # DIRECTORY COLUMN
        renderer = Gtk.CellRendererText()
        renderer.set_fixed_height_from_font(1)
        renderer.set_fixed_size(width=-1, height=display_meta.row_height)
        renderer.set_property('width-chars', 20)
        column = Gtk.TreeViewColumn(display_meta.col_names[display_meta.col_num_dir], renderer, text=display_meta.col_num_dir)
        column.set_sort_column_id(display_meta.col_num_dir)

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
    renderer.set_fixed_size(width=-1, height=display_meta.row_height)
    renderer.set_fixed_height_from_font(1)
    # renderer.set_property('width-chars', 15)
    column = Gtk.TreeViewColumn(display_meta.col_names[display_meta.col_num_size], renderer, text=display_meta.col_num_size)
    column.set_sort_column_id(display_meta.col_num_size)

    column.set_sizing(Gtk.TreeViewColumnSizing.FIXED)
    #  column.set_fixed_width(50)
    column.set_min_width(90)
    # column.set_max_width(300)
    column.set_expand(False)
    column.set_resizable(True)
    column.set_reorderable(True)
    treeview.append_column(column)

    # Need the original file sizes (in bytes) here, not the formatted one
    model.set_sort_func(display_meta.col_num_size, _compare_data, (display_meta, lambda f: f.size_bytes))

    # MODIFICATION TS COLUMN
    renderer = Gtk.CellRendererText()
    renderer.set_property('width-chars', 8)
    renderer.set_fixed_size(width=-1, height=display_meta.row_height)
    renderer.set_fixed_height_from_font(1)
    column = Gtk.TreeViewColumn(display_meta.col_names[display_meta.col_num_modification_ts], renderer, text=display_meta.col_num_modification_ts)
    column.set_sort_column_id(display_meta.col_num_modification_ts)

    column.set_sizing(Gtk.TreeViewColumnSizing.FIXED)
    # column.set_fixed_width(50)
    column.set_min_width(50)
    # column.set_max_width(300)
    column.set_expand(False)
    column.set_resizable(True)
    column.set_reorderable(True)
    treeview.append_column(column)

    model.set_sort_func(display_meta.col_num_modification_ts, _compare_data, (display_meta, lambda f: f.modify_ts))

    if display_meta.show_change_ts:
        # METADATA CHANGE TS COLUMN
        renderer = Gtk.CellRendererText()
        renderer.set_property('width-chars', 8)
        renderer.set_fixed_size(width=-1, height=display_meta.row_height)
        renderer.set_fixed_height_from_font(1)
        column = Gtk.TreeViewColumn(display_meta.col_names[display_meta.col_num_change_ts], renderer, text=display_meta.col_num_change_ts)
        column.set_sort_column_id(display_meta.col_num_change_ts)

        column.set_sizing(Gtk.TreeViewColumnSizing.FIXED)
        # column.set_fixed_width(50)
        column.set_min_width(50)
        # column.set_max_width(300)
        column.set_expand(False)
        column.set_resizable(True)
        column.set_reorderable(True)
        treeview.append_column(column)

        model.set_sort_func(display_meta.col_num_change_ts, _compare_data, (display_meta, lambda f: f.change_ts))

    select = treeview.get_selection()
    select.set_mode(display_meta.selection_mode)
    return treeview


def is_ignored_func(data_node):
    return data_node.category == Category.Ignored


def build(parent_win, data_store, display_meta, display_strategy, action_handlers):
    """Builds a single instance of a tree panel, and configures all its components as specified."""
    logger.debug(f'Building controller for tree: {data_store.tree_id}')

    display_store = DisplayStore(display_meta, data_store)

    # The controller holds all the components in memory. Important for listeners especially,
    # since they rely on weak references.
    controller = TreePanelController(data_store, display_store, display_meta)
    controller.tree_view = _build_treeview(display_store)
    controller.root_dir_panel = RootDirPanel(parent_win=parent_win, tree_id=data_store.tree_id,
                                             current_root=data_store.get_root_path(), editable=display_meta.editable)
    controller.display_strategy = display_strategy
    display_strategy.con = controller
    controller.action_handlers = action_handlers
    action_handlers.con = controller

    controller.status_bar, status_bar_container = _build_status_bar()
    controller.content_box = _build_content_box(controller.root_dir_panel.content_box, controller.tree_view, status_bar_container)

    # Line up the following between trees if we are displaying side-by-side trees:
    if hasattr('parent_win', 'sizegroups'):
        if parent_win.sizegroups.get('tree_status'):
            parent_win.sizegroups['tree_status'].add_widget(status_bar_container)
        if parent_win.sizegroups.get('root_paths'):
            parent_win.sizegroups['root_paths'].add_widget(controller.root_dir_panel.content_box)

    controller.init()
    return controller


def build_gdrive(parent_win, data_store):
    """Builds a tree panel for browsing a Google Drive tree, using lazy loading."""
    display_meta = TreeDisplayMeta(config=parent_win.config, tree_id=data_store.tree_id, editable=False,
                                   selection_mode=Gtk.SelectionMode.SINGLE,
                                   is_display_persisted=True, is_ignored_func=is_ignored_func)

    display_strategy = LazyLoadStrategy()
    action_handlers = GDriveActionHandlers()

    return build(parent_win=parent_win, data_store=data_store, display_meta=display_meta, display_strategy=display_strategy, action_handlers=action_handlers)


def build_bulk_load_file_tree(parent_win, data_store):
    display_meta = TreeDisplayMeta(config=parent_win.config, tree_id=data_store.tree_id, editable=True,
                                   selection_mode=Gtk.SelectionMode.MULTIPLE,
                                   is_display_persisted=True, is_ignored_func=is_ignored_func)

    display_strategy = FMetaChangeTreeStrategy()
    action_handlers = FMetaTreeActionHandlers()

    con = build(parent_win=parent_win, data_store=data_store, display_meta=display_meta, display_strategy=display_strategy, action_handlers=action_handlers)
    return con


def build_static_file_tree(parent_win, data_store):
    display_meta = TreeDisplayMeta(config=parent_win.config, tree_id=data_store.tree_id, editable=False,
                                   selection_mode=Gtk.SelectionMode.SINGLE,
                                   is_display_persisted=False, is_ignored_func=is_ignored_func)

    display_strategy = FMetaChangeTreeStrategy()
    action_handlers = FMetaTreeActionHandlers()

    con = build(parent_win=parent_win, data_store=data_store, display_meta=display_meta, display_strategy=display_strategy, action_handlers=action_handlers)
    return con
