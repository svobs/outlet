import logging
from typing import Optional

from constants import SUPER_DEBUG, TreeDisplayMode
from model.node.node import Node
from model.node_identifier import NodeIdentifier, SinglePathNodeIdentifier
from model.display_tree.display_tree import DisplayTree
from ui.comp.filter_panel import TreeFilterPanel
from ui.dialog.base_dialog import BaseDialog
from ui.tree import tree_factory_templates
from ui.tree.tree_actions import TreeActions
from ui.tree.ui_listeners import TreeUiListeners

from ui.tree.display_mutator import DisplayMutator
from ui.comp.root_dir_panel import RootDirPanel

from ui.tree.controller import TreePanelController
from ui.tree.treeview_meta import TreeViewMeta
from ui.tree.display_store import DisplayStore

import gi

from util.root_path_meta import RootPathMeta

gi.require_version("Gtk", "3.0")
from gi.repository import Gtk

logger = logging.getLogger(__name__)

"""
🡻🡻🡻 ① Static internal functinos 🡻🡻🡻
"""


def is_ignored_func(data_node: Node) -> bool:
    # not currently used
    return False


"""
🡻🡻🡻 ② Generic case 🡻🡻🡻
"""


class TreeFactory:
    def __init__(self,
                 parent_win: BaseDialog,
                 tree_id: str,
                 root_path_meta: Optional[RootPathMeta] = None,
                 tree: Optional[DisplayTree] = None
                 ):
        self.parent_win = parent_win

        if root_path_meta:
            self.root_path_meta = root_path_meta
        elif tree:
            self.root_path_meta = RootPathMeta(tree.get_root_identifier(), is_found=True)
        else:
            raise RuntimeError('Params "root_path_meta" and "tree" cannot both be empty!')
        self.tree: Optional[DisplayTree] = tree
        """Choose one: tree or root"""

        self.tree_id: str = tree_id
        self.can_modify_tree = False
        self.has_checkboxes: bool = False
        self.can_change_root: bool = False
        self.lazy_load: bool = True
        self.allow_multiple_selection: bool = False
        self.display_persisted: bool = False
        self.tree_display_mode: TreeDisplayMode = TreeDisplayMode.ONE_TREE_ALL_ITEMS

    def build(self):
        """Builds a single instance of a tree panel, and configures all its components as specified."""
        logger.debug(f'[{self.tree_id}] Building controller for tree')

        if self.allow_multiple_selection:
            gtk_selection_mode = Gtk.SelectionMode.MULTIPLE
        else:
            gtk_selection_mode = Gtk.SelectionMode.SINGLE

        treeview_meta = TreeViewMeta(config=self.parent_win.config,
                                     tree_id=self.tree_id,
                                     can_modify_tree=self.can_modify_tree,
                                     has_checkboxes=self.has_checkboxes,
                                     can_change_root=self.can_change_root,
                                     tree_display_mode=self.tree_display_mode,
                                     lazy_load=self.lazy_load,
                                     selection_mode=gtk_selection_mode,
                                     is_display_persisted=self.display_persisted,
                                     is_ignored_func=is_ignored_func)

        treeview_meta.read_filter_criteria_from_config()

        # The controller holds all the components in memory. Important for listeners especially,
        # since they rely on weak references.
        controller = TreePanelController(self.parent_win, treeview_meta)

        controller.display_store = DisplayStore(controller)

        if self.tree:
            # Prefer tree over root
            controller.set_tree(tree=self.tree)
            already_loaded = True
        elif self.root_path_meta:
            controller.set_tree(root=self.root_path_meta.root)
            already_loaded = False
        else:
            raise RuntimeError('"root_path_meta" and "tree" are both empty!')

        controller.display_mutator = DisplayMutator(config=self.parent_win.config, controller=controller)

        controller.tree_ui_listeners = TreeUiListeners(config=self.parent_win.config, controller=controller)

        controller.tree_actions = TreeActions(controller=controller)

        assets = self.parent_win.app.assets
        controller.tree_view = tree_factory_templates.build_treeview(controller.display_store, assets)

        controller.root_dir_panel = RootDirPanel(parent_win=self.parent_win,
                                                 controller=controller,
                                                 current_root_meta=self.root_path_meta,
                                                 can_change_root=treeview_meta.can_change_root,
                                                 is_loaded=already_loaded)

        controller.filter_panel = TreeFilterPanel(parent_win=self.parent_win,
                                                  controller=controller)

        controller.status_bar, status_bar_container = tree_factory_templates.build_status_bar()
        controller.content_box = tree_factory_templates.build_content_box(controller.root_dir_panel.content_box, controller.filter_panel.content_box,
                                                                          controller.tree_view, status_bar_container)

        # Line up the following between trees if we are displaying side-by-side trees:
        if hasattr('parent_win', 'sizegroups'):
            if self.parent_win.sizegroups.get('tree_status'):
                self.parent_win.sizegroups['tree_status'].add_widget(status_bar_container)
            if self.parent_win.sizegroups.get('root_paths'):
                self.parent_win.sizegroups['root_paths'].add_widget(controller.root_dir_panel.content_box)
            if self.parent_win.sizegroups.get('filter_panel'):
                self.parent_win.sizegroups['filter_panel'].add_widget(controller.filter_panel.content_box)

        controller.init()
        return controller


"""
🡻🡻🡻 ③ Specialized cases 🡻🡻🡻
"""


def build_gdrive_root_chooser(parent_win, tree_id, tree: DisplayTree):
    """Builds a tree panel for browsing a Google Drive tree, using lazy loading. For the GDrive root chooser dialog"""
    if SUPER_DEBUG:
        logger.debug(f'[{tree_id}] Entered build_gdrive_root_chooser()')
    factory = TreeFactory(parent_win=parent_win, tree=tree, tree_id=tree_id)
    factory.allow_multiple_selection = False
    factory.can_modify_tree = False
    factory.display_persisted = False
    factory.has_checkboxes = False
    factory.can_change_root = True
    return factory.build()


def build_editor_tree(parent_win,
                      tree_id: str,
                      root_path_meta: RootPathMeta = None,
                      tree: DisplayTree = None):
    if SUPER_DEBUG:
        logger.debug(f'[{tree_id}] Entered build_editor_tree()')
    factory = TreeFactory(parent_win=parent_win, root_path_meta=root_path_meta, tree=tree, tree_id=tree_id)
    factory.has_checkboxes = False  # not initially
    factory.can_modify_tree = True
    factory.can_change_root = True
    factory.allow_multiple_selection = True
    factory.display_persisted = True
    return factory.build()


def build_static_category_file_tree(parent_win, tree_id: str, tree: DisplayTree):
    if SUPER_DEBUG:
        logger.debug(f'[{tree_id}] Entered build_static_category_file_tree()')
    # Whole tree is provided here. For Merge Preview dialog
    factory = TreeFactory(parent_win=parent_win, tree=tree, tree_id=tree_id)
    factory.has_checkboxes = False
    factory.can_change_root = False
    factory.can_modify_tree = False
    factory.allow_multiple_selection = False
    factory.display_persisted = False
    factory.tree_display_mode = TreeDisplayMode.CHANGES_ONE_TREE_PER_CATEGORY
    factory.lazy_load = False
    return factory.build()
