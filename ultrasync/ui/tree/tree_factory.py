import logging
from typing import Optional, Tuple

import gi

import constants
from constants import TreeDisplayMode
from model.display_id import Identifier
from model.fmeta_tree import FMetaTree
from model.subtree_snapshot import SubtreeSnapshot
from ui.tree import tree_factory_templates

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

logger = logging.getLogger(__name__)

"""
🡻🡻🡻 ① Static internal functinos 🡻🡻🡻
"""


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

        controller.tree_view = tree_factory_templates.build_treeview(display_store)
        controller.root_dir_panel = RootDirPanel(parent_win=self.parent_win,
                                                 tree_id=treeview_meta.tree_id,
                                                 current_root=controller.get_root_identifier(),
                                                 can_change_root=treeview_meta.can_change_root)

        controller.status_bar, status_bar_container = tree_factory_templates.build_status_bar()
        controller.content_box = tree_factory_templates.build_content_box(controller.root_dir_panel.content_box, controller.tree_view, status_bar_container)

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

