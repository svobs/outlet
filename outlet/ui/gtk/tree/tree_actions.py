import logging
import os
import re
import subprocess
from typing import List

from pydispatch import dispatcher

from constants import DATE_REGEX, OPEN, SHOW
from model.node.gdrive_node import GDriveFile
from model.node.node import Node, SPIDNodePair
from signal_constants import Signal
from util.has_lifecycle import HasLifecycle

import gi
gi.require_version("Gtk", "3.0")
from gi.repository import Gtk

logger = logging.getLogger(__name__)


class TreeActions(HasLifecycle):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS TreeActions
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, controller):
        HasLifecycle.__init__(self)
        self.con = controller
        self.post_download_action = OPEN

    def start(self):
        logger.debug(f'[{self.con.tree_id}] TreeActions start')
        HasLifecycle.start(self)
        self.connect_dispatch_listener(signal=Signal.CALL_EXIFTOOL, sender=self.con.tree_id, receiver=self._call_exiftool)
        self.connect_dispatch_listener(signal=Signal.CALL_EXIFTOOL_LIST, sender=self.con.tree_id, receiver=self._call_exiftool_list)
        self.connect_dispatch_listener(signal=Signal.SHOW_IN_NAUTILUS, sender=self.con.tree_id, receiver=self._show_in_nautilus)
        self.connect_dispatch_listener(signal=Signal.CALL_XDG_OPEN, sender=self.con.tree_id, receiver=self._call_xdg_open)
        self.connect_dispatch_listener(signal=Signal.DOWNLOAD_FROM_GDRIVE, sender=self.con.tree_id, receiver=self._download_file_from_gdrive)
        self.connect_dispatch_listener(signal=Signal.DOWNLOAD_FROM_GDRIVE_DONE, receiver=self._on_gdrive_download_done)
        self.connect_dispatch_listener(signal=Signal.DELETE_SINGLE_FILE, sender=self.con.tree_id, receiver=self._delete_single_file)
        self.connect_dispatch_listener(signal=Signal.DELETE_SUBTREE, sender=self.con.tree_id, receiver=self._delete_subtree)
        self.connect_dispatch_listener(signal=Signal.SET_ROWS_CHECKED, sender=self.con.tree_id, receiver=self._check_rows)
        self.connect_dispatch_listener(signal=Signal.SET_ROWS_UNCHECKED, sender=self.con.tree_id, receiver=self._uncheck_rows)

    # ACTIONS begin
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def _call_exiftool_list(self, sender, sn_list: List[SPIDNodePair]):

        def call_exiftool():
            for sn in sn_list:
                self._call_exiftool(sender, sn.spid.get_single_path())

        dispatcher.send(signal=Signal.ENQUEUE_UI_TASK, sender=sender, task_func=call_exiftool)

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
        self.con.app.backend.download_file_from_gdrive(node.device_uid, node.uid, sender)

    def _on_gdrive_download_done(self, sender, filename: str):
        if sender == self.con.tree_id:
            if self.post_download_action == OPEN:
                self._call_xdg_open(sender, filename)
            elif self.post_download_action == SHOW:
                self._show_in_nautilus(sender, filename)

    def _call_xdg_open(self, sender, full_path: str):
        if os.path.exists(full_path):
            logger.info(f'[{self.con.tree_id}] Calling xdg-open for: {full_path}')
            subprocess.run(["xdg-open", full_path])
        else:
            self.con.parent_win.show_error_msg(f'Cannot open file', f'File not found: {full_path}')

    def _show_in_nautilus(self, sender, full_path):
        if os.path.exists(full_path):
            logger.info(f'[{self.con.tree_id}] Opening in Nautilus: {full_path}')
            # FIXME: this occasionally gets a catastrophic error which corrupts the whole UI!
            subprocess.run(["nautilus", "--browser", full_path])
        else:
            self.con.parent_win.show_error_msg('Cannot open file in Nautilus', f'File not found: {full_path}')

    def _delete_single_file(self, sender, node: Node):
        self._delete_subtree(sender, [node])

    def _delete_subtree(self, sender, node_list: List[Node]):
        if not node_list:
            return
        device_uid = node_list[0].device_uid
        node_uid_list = [n.uid for n in node_list]
        self.con.app.backend.delete_subtree(device_uid, node_uid_list)

    def _check_rows(self, sender, tree_paths: List[Gtk.TreePath] = None):
        for tree_path in tree_paths:
            self.con.display_store.set_row_checked(tree_path, True)

    def _uncheck_rows(self, sender, tree_paths: List[Gtk.TreePath] = None):
        for tree_path in tree_paths:
            self.con.display_store.set_row_checked(tree_path, False)
