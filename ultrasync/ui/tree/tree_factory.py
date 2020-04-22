import logging

import ui.assets
from ui.root_dir_panel import RootDirPanel

import gi
gi.require_version("Gtk", "3.0")
from gi.repository import GLib, Gtk

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
    renderer.set_property('width-chars', 10)
    column = Gtk.TreeViewColumn(display_meta.col_names[display_meta.col_num_size], renderer, text=display_meta.col_num_size)
    column.set_sort_column_id(display_meta.col_num_size)

    column.set_sizing(Gtk.TreeViewColumnSizing.FIXED)
    #  column.set_fixed_width(50)
    column.set_min_width(50)
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

    return treeview


def build_all(parent_win, store, display_store):
    root_dir_panel = RootDirPanel(parent_win, store, display_store.display_meta.editable)

    tree_view = _build_treeview(display_store)

    status_bar, status_bar_container = _build_status_bar()
    content_box = _build_content_box(root_dir_panel.content_box, tree_view, status_bar_container)

    # Line up the following between trees if we are displaying side-by-side trees:
    if hasattr('parent_win', 'sizegroups'):
        if parent_win.sizegroups.get('tree_status'):
            parent_win.sizegroups['tree_status'].add_widget(status_bar_container)
        if parent_win.sizegroups.get('root_paths'):
            parent_win.sizegroups['root_paths'].add_widget(root_dir_panel.content_box)

    return tree_view, status_bar, content_box
