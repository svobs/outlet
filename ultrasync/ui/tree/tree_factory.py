import logging
from typing import Optional, Tuple

import gi

from constants import TreeDisplayMode
from model.display_node import DisplayNode
from model.node_identifier import NodeIdentifier
from model.subtree_snapshot import SubtreeSnapshot
from ui.dialog.base_dialog import BaseDialog
from ui.tree import tree_factory_templates
from ui.tree.tree_actions import TreeActions
from ui.tree.ui_listeners import TreeUiListeners

gi.require_version("Gtk", "3.0")
from gi.repository import Gtk

from model.fmeta import Category
from ui.tree.display_mutator import DisplayMutator
from ui.comp.root_dir_panel import RootDirPanel

from ui.tree.controller import TreePanelController
from ui.tree.treeview_meta import TreeViewMeta
from ui.tree.display_store import DisplayStore

logger = logging.getLogger(__name__)

"""
🡻🡻🡻 ① Static internal functinos 🡻🡻🡻
"""


def is_ignored_func(data_node: DisplayNode) -> bool:
    return data_node.category == Category.Ignored


"""
🡻🡻🡻 ② Generic case 🡻🡻🡻
"""


class TreeFactory:
    def __init__(self,
                 parent_win: BaseDialog,
                 tree_id: str,
                 root: Optional[NodeIdentifier] = None,
                 tree: Optional[SubtreeSnapshot] = None
                 ):
        self.parent_win = parent_win

        self.root: Optional[NodeIdentifier] = root
        self.tree: Optional[SubtreeSnapshot] = tree
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
        logger.debug(f'Building controller for tree: {self.tree_id}')

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

        display_store = DisplayStore(treeview_meta)

        # The controller holds all the components in memory. Important for listeners especially,
        # since they rely on weak references.
        controller = TreePanelController(self.parent_win, display_store, treeview_meta)

        if self.root:
            controller.set_tree(root=self.root)
            already_loaded = False
        elif self.tree:
            controller.set_tree(tree=self.tree)
            already_loaded = True
        else:
            raise RuntimeError('"root" and "tree" are both empty!')

        controller.display_mutator = DisplayMutator(config=self.parent_win.config, controller=controller)

        controller.user_input_listeners = TreeUiListeners(config=self.parent_win.config, controller=controller)

        controller.tree_actions = TreeActions(controller=controller)

        assets = self.parent_win.application.assets
        controller.tree_view = tree_factory_templates.build_treeview(display_store, assets)

        controller.root_dir_panel = RootDirPanel(parent_win=self.parent_win,
                                                 controller=controller,
                                                 tree_id=treeview_meta.tree_id,
                                                 current_root=controller.get_root_identifier(),
                                                 can_change_root=treeview_meta.can_change_root,
                                                 is_loaded=already_loaded)

        # FIXME: add toggle buttons to GDrive status bar: Show Trashed, Show Shared By Me, Show Shared With Me

        controller.status_bar, status_bar_container = tree_factory_templates.build_status_bar()
        controller.content_box = tree_factory_templates.build_content_box(controller.root_dir_panel.content_box, controller.tree_view,
                                                                          status_bar_container)

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


def build_gdrive_root_chooser(parent_win, tree_id, tree: SubtreeSnapshot):
    """Builds a tree panel for browsing a Google Drive tree, using lazy loading. For the GDrive root chooser dialog"""

    factory = TreeFactory(parent_win=parent_win, tree=tree, tree_id=tree_id)
    factory.allow_multiple_selection = False
    factory.can_modify_tree = False
    factory.display_persisted = False
    factory.has_checkboxes = False
    factory.can_change_root = False
    return factory.build()


def build_editor_tree(parent_win,
                      tree_id: str,
                      root: NodeIdentifier = None,
                      tree: SubtreeSnapshot = None):
    factory = TreeFactory(parent_win=parent_win, root=root, tree=tree, tree_id=tree_id)
    factory.has_checkboxes = False  # not initially
    factory.can_modify_tree = True
    factory.can_change_root = True
    factory.allow_multiple_selection = True
    factory.display_persisted = True
    return factory.build()


def build_static_category_file_tree(parent_win, tree_id: str, tree: SubtreeSnapshot):
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
