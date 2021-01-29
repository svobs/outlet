import logging

import gi

from constants import SUPER_DEBUG, TreeDisplayMode
from ui.gtk.comp.filter_panel import TreeFilterPanel
from ui.gtk.comp.root_dir_panel import RootDirPanel
from ui.gtk.dialog.base_dialog import BaseDialog
from ui.gtk.tree import tree_factory_templates
from ui.gtk.tree.controller import TreePanelController
from ui.gtk.tree.treeview_meta import TreeViewMeta

gi.require_version("Gtk", "3.0")
from gi.repository import Gtk

logger = logging.getLogger(__name__)


def is_ignored_func(data_node) -> bool:
    # not currently used
    return False


class TreeFactory:
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS TreeFactory
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """

    def __init__(self, parent_win: BaseDialog, tree):
        self.parent_win = parent_win

        if not tree:
            raise RuntimeError('Tree not provided!')

        self.tree = tree

        self.can_modify_tree = False
        self.has_checkboxes: bool = False
        self.can_change_root: bool = False
        self.lazy_load: bool = True
        self.allow_multiple_selection: bool = False
        self.display_persisted: bool = False
        self.tree_display_mode: TreeDisplayMode = TreeDisplayMode.ONE_TREE_ALL_ITEMS

    def build(self):
        """Builds a single instance of a tree panel, and configures all its components as specified."""
        logger.debug(f'[{self.tree.tree_id}] Building controller for tree')

        # 1. Logical Stuff

        if self.allow_multiple_selection:
            gtk_selection_mode = Gtk.SelectionMode.MULTIPLE
        else:
            gtk_selection_mode = Gtk.SelectionMode.SINGLE

        treeview_meta = TreeViewMeta(backend=self.parent_win.backend,
                                     tree_id=self.tree.tree_id,
                                     can_modify_tree=self.can_modify_tree,
                                     has_checkboxes=self.has_checkboxes,
                                     can_change_root=self.can_change_root,
                                     tree_display_mode=self.tree_display_mode,
                                     lazy_load=self.lazy_load,
                                     selection_mode=gtk_selection_mode,
                                     is_display_persisted=self.display_persisted,
                                     is_ignored_func=is_ignored_func)

        # The controller holds all the components in memory. Important for listeners especially,
        # since they rely on weak references.
        controller = TreePanelController(self.parent_win, self.tree, treeview_meta)

        # 2. GTK3 stuff

        controller.tree_view = tree_factory_templates.build_treeview(controller.display_store, controller.parent_win.app.assets)

        controller.root_dir_panel = RootDirPanel(parent_win=self.parent_win,
                                                 controller=controller,
                                                 can_change_root=treeview_meta.can_change_root)

        controller.filter_panel = TreeFilterPanel(parent_win=self.parent_win,
                                                  controller=controller)

        controller.status_bar, controller.status_bar_container = tree_factory_templates.build_status_bar()
        controller.content_box = tree_factory_templates.build_content_box(controller.root_dir_panel.content_box, controller.filter_panel.content_box,
                                                                          controller.tree_view, controller.status_bar_container)

        # Line up the following between trees if we are displaying side-by-side trees:
        if hasattr('parent_win', 'sizegroups'):
            if self.parent_win.sizegroups.get('tree_status'):
                self.parent_win.sizegroups['tree_status'].add_widget(controller.status_bar_container)
            if self.parent_win.sizegroups.get('root_paths'):
                self.parent_win.sizegroups['root_paths'].add_widget(controller.root_dir_panel.content_box)
            if self.parent_win.sizegroups.get('filter_panel'):
                self.parent_win.sizegroups['filter_panel'].add_widget(controller.filter_panel.content_box)

        # 3. Start everything
        controller.start()

        # Even if the backend already loaded the tree, ask it to send a notification again so we know it's finished loading:
        self.parent_win.app.backend.start_subtree_load(self.tree.tree_id)
        return controller


"""
🡻🡻🡻 Specialized cases 🡻🡻🡻
"""


def build_gdrive_root_chooser(parent_win, tree):
    """Builds a tree panel for browsing a Google Drive tree, using lazy loading. For the GDrive root chooser dialog"""
    if SUPER_DEBUG:
        logger.debug(f'[{tree.tree_id}] Entered build_gdrive_root_chooser()')
    factory = TreeFactory(parent_win=parent_win, tree=tree)
    factory.allow_multiple_selection = False
    factory.can_modify_tree = False
    factory.display_persisted = False
    factory.has_checkboxes = False
    factory.can_change_root = True
    controller = factory.build()

    return controller


def build_editor_tree(parent_win, tree):
    if SUPER_DEBUG:
        logger.debug(f'[{tree.tree_id}] Entered build_editor_tree()')
    factory = TreeFactory(parent_win=parent_win, tree=tree)
    factory.has_checkboxes = False  # not initially
    factory.can_modify_tree = True
    factory.can_change_root = True
    factory.allow_multiple_selection = True
    factory.display_persisted = True
    controller = factory.build()

    return controller


def build_eager_load_change_tree(parent_win, tree):
    if SUPER_DEBUG:
        logger.debug(f'[{tree.tree_id}] Entered build_eager_load_change_tree()')
    # Whole tree is provided here. For Merge Preview dialog
    factory = TreeFactory(parent_win=parent_win, tree=tree)
    factory.has_checkboxes = False
    factory.can_change_root = False
    factory.can_modify_tree = False
    factory.allow_multiple_selection = False
    factory.display_persisted = False
    factory.tree_display_mode = TreeDisplayMode.CHANGES_ONE_TREE_PER_CATEGORY
    factory.lazy_load = False
    return factory.build()
