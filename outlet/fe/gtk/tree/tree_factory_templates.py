import logging

from constants import V_PAD

import gi

from fe.gtk.tree.multi_drag_tree_view import MultiDragTreeView

gi.require_version("Gtk", "3.0")
from gi.repository import Gtk
from gi.repository.Gtk import TreeView

logger = logging.getLogger(__name__)


def build_status_bar():
    info_bar_container = Gtk.Box(spacing=6, orientation=Gtk.Orientation.HORIZONTAL)
    info_bar = Gtk.Label(label='')
    info_bar.set_justify(Gtk.Justification.LEFT)
    info_bar.set_line_wrap(True)
    info_bar_container.add(info_bar)
    return info_bar, info_bar_container


def build_content_box(root_dir_panel, filter_panel, tree_view, status_bar_container):
    vbox = Gtk.Box(spacing=0, orientation=Gtk.Orientation.VERTICAL)

    vbox.pack_start(root_dir_panel, False, False, 0)

    vbox.pack_start(filter_panel, False, False, V_PAD)

    tree_scroller = Gtk.ScrolledWindow()
    # No horizontal scrolling - only vertical
    tree_scroller.set_policy(Gtk.PolicyType.NEVER, Gtk.PolicyType.AUTOMATIC)
    tree_scroller.add(tree_view)
    # child, expand, fill, padding
    vbox.pack_start(tree_scroller, False, True, 0)

    vbox.pack_start(status_bar_container, False, True, V_PAD)

    return vbox


def add_checkbox_icon_name_column(treeview: TreeView, display_store, assets):
    # COLUMN: Checkbox + Icon + Name
    # See: https://stackoverflow.com/questions/27745585/show-icon-or-color-in-gtk-treeview-tree
    treeview_meta = display_store.treeview_meta
    px_column = Gtk.TreeViewColumn(treeview_meta.col_names[treeview_meta.col_num_name])
    px_column.set_sizing(Gtk.TreeViewColumnSizing.FIXED)

    if treeview_meta.has_checkboxes:
        checkbox_renderer = Gtk.CellRendererToggle()
        checkbox_renderer.connect("toggled", display_store.on_cell_checkbox_toggled)
        checkbox_renderer.set_fixed_size(width=-1, height=treeview_meta.row_height)
        px_column.pack_start(checkbox_renderer, False)
        px_column.add_attribute(checkbox_renderer, 'active', treeview_meta.col_num_checked)
        px_column.add_attribute(checkbox_renderer, 'inconsistent', treeview_meta.col_num_inconsistent)

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
        icon_id = model.get_value(iter, treeview_meta.col_num_icon)
        if icon_id is not None:
            icon_id = int(icon_id)
        cell.set_property('pixbuf', assets.get_icon(icon_id))

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


def add_directory_column(treeview, treeview_meta):
    """DIRECTORY COLUMN"""
    if treeview_meta.use_dir_tree:
        return

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


def add_size_column(treeview, treeview_meta, model):
    """SIZE COLUMN"""
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
    model.set_sort_func(treeview_meta.col_num_size, _compare_data, (treeview_meta, lambda f: f.get_size_bytes()))


def add_etc_column(treeview, treeview_meta, model):
    """ETC COLUMN"""
    renderer = Gtk.CellRendererText()
    renderer.set_fixed_size(width=-1, height=treeview_meta.row_height)
    renderer.set_fixed_height_from_font(1)
    # renderer.set_property('width-chars', 15)
    column = Gtk.TreeViewColumn(treeview_meta.col_names[treeview_meta.col_num_etc], renderer, text=treeview_meta.col_num_etc)
    # column.set_sort_column_id(treeview_meta.col_num_etc)

    column.set_sizing(Gtk.TreeViewColumnSizing.FIXED)
    #  column.set_fixed_width(50)
    column.set_min_width(90)
    # column.set_max_width(300)
    column.set_expand(False)
    column.set_resizable(True)
    column.set_reorderable(True)
    treeview.append_column(column)


def add_modify_ts_column(treeview, treeview_meta, model):
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


def add_change_ts_column(treeview, treeview_meta, model):
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


def build_treeview(display_store, assets) -> Gtk.TreeView:
    """ Builds the GTK3 treeview widget"""
    model: Gtk.TreeStore = display_store.model
    treeview_meta = display_store.treeview_meta

    treeview = MultiDragTreeView(model=model)
    treeview.set_level_indentation(treeview_meta.extra_indent)
    treeview.set_show_expanders(True)
    treeview.set_property('enable_grid_lines', True)
    treeview.set_property('enable_tree_lines', True)
    treeview.set_fixed_height_mode(True)
    treeview.set_vscroll_policy(Gtk.ScrollablePolicy.NATURAL)
    # Allow click+drag to select multiple items.
    # May want to disable if using drag+drop
    treeview.set_rubber_banding(True)

    # Some guy on a message board says it's important to set this to true:
    treeview.set_can_focus(True)

    # Search-as-you-type: navigates to an item in the list which starts with the text entered.
    # Currently we will just use the default built-in search box
    treeview.set_enable_search(True)
    treeview.set_search_column(treeview_meta.col_num_name)

    # Search for "TREE_VIEW_COLUMNS":

    add_checkbox_icon_name_column(treeview, display_store, assets)
    add_directory_column(treeview, treeview_meta)
    add_size_column(treeview, treeview_meta, model)
    add_etc_column(treeview, treeview_meta, model)
    add_modify_ts_column(treeview, treeview_meta, model)
    add_change_ts_column(treeview, treeview_meta, model)

    # Selection mode (single, multi):
    select = treeview.get_selection()
    select.set_mode(treeview_meta.selection_mode)

    # Tree will take up all the excess space
    treeview.set_vexpand(True)
    treeview.set_hexpand(False)
    return treeview


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


def replace_widget(old, new):
    parent = old.get_parent()

    children = parent.get_children()

    for child in children:
        if child == old:
            parent.remove(old)
            parent.add(new)

    new.show()
    parent.show()
