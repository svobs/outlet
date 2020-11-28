import logging
import os
import re
import subprocess
from typing import List

from pydispatch import dispatcher

from constants import TreeDisplayMode
from model.display_tree.category import CategoryDisplayTree
from model.node.gdrive_node import GDriveFile
from model.node.local_disk_node import LocalNode
from model.node.node import Node, SPIDNodePair
from model.user_op import UserOp, UserOpType
from store.gdrive.client import GDriveClient
from ui import actions
from util import file_util
from util.has_lifecycle import HasLifecycle

import gi
gi.require_version("Gtk", "3.0")
from gi.repository import Gtk

logger = logging.getLogger(__name__)

DATE_REGEX = r'^[\d]{4}(\-[\d]{2})?(-[\d]{2})?'
OPEN = 1
SHOW = 2


# CLASS TreeActions
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class TreeActions(HasLifecycle):
    def __init__(self, controller):
        HasLifecycle.__init__(self)
        self.con = controller
        self.download_dir = file_util.get_resource_path(self.con.config.get('download_dir'))
        self.post_download_action = OPEN

    def start(self):
        logger.debug(f'[{self.con.tree_id}] TreeActions start')
        HasLifecycle.start(self)
        self.connect_dispatch_listener(signal=actions.CALL_EXIFTOOL, sender=self.con.tree_id, receiver=self._call_exiftool)
        self.connect_dispatch_listener(signal=actions.CALL_EXIFTOOL_LIST, sender=self.con.tree_id, receiver=self._call_exiftool_list)
        self.connect_dispatch_listener(signal=actions.SHOW_IN_NAUTILUS, sender=self.con.tree_id, receiver=self._show_in_nautilus)
        self.connect_dispatch_listener(signal=actions.CALL_XDG_OPEN, sender=self.con.tree_id, receiver=self._call_xdg_open)
        self.connect_dispatch_listener(signal=actions.DOWNLOAD_FROM_GDRIVE, sender=self.con.tree_id, receiver=self._download_file_from_gdrive)
        self.connect_dispatch_listener(signal=actions.DELETE_SINGLE_FILE, sender=self.con.tree_id, receiver=self._delete_single_file)
        self.connect_dispatch_listener(signal=actions.DELETE_SUBTREE, sender=self.con.tree_id, receiver=self._delete_subtree)
        self.connect_dispatch_listener(signal=actions.SET_ROWS_CHECKED, sender=self.con.tree_id, receiver=self._check_rows)
        self.connect_dispatch_listener(signal=actions.SET_ROWS_UNCHECKED, sender=self.con.tree_id, receiver=self._uncheck_rows)
        self.connect_dispatch_listener(signal=actions.DIFF_ONE_SIDE_RESULT, sender=self.con.tree_id, receiver=self._receive_diff_result)

    # ACTIONS begin
    # ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

    def _call_exiftool_list(self, sender, node_list: List[LocalNode]):

        def call_exiftool():
            for item in node_list:
                self._call_exiftool(sender, item.get_single_path())

        dispatcher.send(signal=actions.ENQUEUE_UI_TASK, sender=sender, task_func=call_exiftool)

    def _call_exiftool(self, sender, full_path):
        """See "Misc EXIF Tool Notes" in README.md
        """
        if not os.path.exists(full_path):
            self.con.parent_win.show_error_msg(f'Cannot manipulate dir', f'Dir not found: {full_path}')
            return
        if not os.path.isdir(full_path):
            self.con.parent_win.show_error_msg(f'Cannot manipulate dir', f'Not a dir: {full_path}')
            return
        dir_name = os.path.basename(full_path)
        tokens = dir_name.split(' ', 1)
        comment_to_set = None
        if len(tokens) > 1:
            assert not len(tokens) > 2, f'Length of tokens is {len(tokens)}: "{full_path}"'
            comment_to_set = tokens[1]
        date_to_set = tokens[0]
        if not re.fullmatch(DATE_REGEX + '$', date_to_set):
            raise RuntimeError(f'Unexpected date pattern: {tokens[0]}')
        if len(date_to_set) == 10:
            # good, whole date. Just to be sure, replace all dashes with colons
            pass
        elif len(date_to_set) == 7:
            # only year + month found. Add default day
            date_to_set += ':01'
        elif len(date_to_set) == 4:
            # only year found. Add default day
            date_to_set += ':01:01'
        date_to_set = date_to_set.replace('-', ':')

        logger.info(f'[{self.con.tree_id}] Calling exiftool for: {full_path}')
        args = ["exiftool", f'-AllDates="{date_to_set} 12:00:00"']
        if comment_to_set:
            args.append(f'-Comment="{comment_to_set}"')
        args.append(full_path)
        subprocess.run(args)

        list_original_files = [f.path for f in os.scandir(full_path) if not f.is_dir() and f.path.endswith('.jpg_original')]
        for file in list_original_files:
            logger.debug(f'[{self.con.tree_id}] Removing file: {file}')
            os.remove(file)

    def _download_file_from_gdrive(self, sender, node: GDriveFile):
        os.makedirs(name=self.download_dir, exist_ok=True)
        dest_file = os.path.join(self.download_dir, node.name)

        gdrive_client: GDriveClient = self.con.app.cacheman.get_gdrive_client()
        try:
            gdrive_client.download_file(node.goog_id, dest_file)
            if self.post_download_action == OPEN:
                self._call_xdg_open(sender, dest_file)
            elif self.post_download_action == SHOW:
                self._show_in_nautilus(sender, dest_file)
        except Exception as err:
            self.con.parent_win.show_error_msg('Download failed', repr(err))
            raise

    def _call_xdg_open(self, sender, full_path: str):
        if os.path.exists(full_path):
            logger.info(f'[{self.con.tree_id}] Calling xdg-open for: {full_path}')
            subprocess.run(["xdg-open", full_path])
        else:
            self.con.parent_win.show_error_msg(f'Cannot open file', f'File not found: {full_path}')

    def _show_in_nautilus(self, sender, full_path):
        if os.path.exists(full_path):
            logger.info(f'[{self.con.tree_id}] Opening in Nautilus: {full_path}')
            subprocess.run(["nautilus", "--browser", full_path])
        else:
            self.con.parent_win.show_error_msg('Cannot open file in Nautilus', f'File not found: {full_path}')

    def _delete_single_file(self, sender, node: Node):
        self._delete_subtree(sender, node)

    def _get_subtree_for_node(self, subtree_root: Node) -> List[Node]:
        assert subtree_root.is_dir(), f'Expected a dir: {subtree_root}'

        subtree_files, subtree_dirs = self.con.app.backend.cacheman.get_all_files_and_dirs_for_subtree(subtree_root.node_identifier)
        return subtree_files + subtree_dirs

    def _delete_subtree(self, sender, node: Node = None, node_list: List[Node] = None):
        if not node_list and node:
            node_list = [node]
        logger.debug(f'[{self.con.tree_id}] Setting up delete for {len(node_list)} nodes')

        # don't worry about overlapping trees; the cacheman will sort everything out
        batch_uid = self.con.app.backend.uid_generator.next_uid()
        op_list = []
        for node_to_delete in node_list:
            if isinstance(node_to_delete, SPIDNodePair):
                node_to_delete = node_to_delete.node

            if node_to_delete.is_dir():
                # Expand dir nodes. ChangeManager will not remove non-empty dirs
                expanded_node_list = self._get_subtree_for_node(node_to_delete)
                for node in expanded_node_list:
                    # somewhere in this returned list is the subtree root. Need to check so we don't include a duplicate:
                    if node.uid != node_to_delete.uid:
                        op_list.append(UserOp(op_uid=self.con.app.backend.uid_generator.next_uid(), batch_uid=batch_uid,
                                              op_type=UserOpType.RM, src_node=node))

            op_list.append(UserOp(op_uid=self.con.app.backend.uid_generator.next_uid(), batch_uid=batch_uid,
                                  op_type=UserOpType.RM, src_node=node_to_delete))

        self.con.parent_win.app.backend.cacheman.enqueue_op_list(op_list)

    def _check_rows(self, sender, tree_paths: List[Gtk.TreePath] = None):
        for tree_path in tree_paths:
            self.con.display_store.set_row_checked(tree_path, True)

    def _uncheck_rows(self, sender, tree_paths: List[Gtk.TreePath] = None):
        for tree_path in tree_paths:
            self.con.display_store.set_row_checked(tree_path, False)

    def _receive_diff_result(self, sender: str, new_tree: CategoryDisplayTree):
        self.con.reload(new_tree=new_tree, tree_display_mode=TreeDisplayMode.CHANGES_ONE_TREE_PER_CATEGORY, show_checkboxes=True)
