import os
import logging
import re
from typing import List
import subprocess
from pydispatch import dispatcher

import file_util
from command.command_builder import CommandBuilder
from gdrive.client import GDriveClient
from model.category import Category
from model.display_node import DisplayNode
from model.goog_node import GoogFile
from ui import actions

logger = logging.getLogger(__name__)

DATE_REGEX = r'^[\d]{4}(\-[\d]{2})?(-[\d]{2})?'
OPEN = 1
SHOW = 2


# CLASS TreeActions
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class TreeActions:
    def __init__(self, controller):
        self.con = controller
        self.download_dir = file_util.get_resource_path(self.con.config.get('download_dir'))
        self.post_download_action = OPEN

    def init(self):
        dispatcher.connect(signal=actions.CALL_EXIFTOOL, sender=self.con.tree_id, receiver=self._call_exiftool)
        dispatcher.connect(signal=actions.CALL_EXIFTOOL_LIST, sender=self.con.tree_id, receiver=self._call_exiftool_list)
        dispatcher.connect(signal=actions.SHOW_IN_NAUTILUS, sender=self.con.tree_id, receiver=self._show_in_nautilus)
        dispatcher.connect(signal=actions.CALL_XDG_OPEN, sender=self.con.tree_id, receiver=self._call_xdg_open)
        dispatcher.connect(signal=actions.EXPAND_ALL, sender=self.con.tree_id, receiver=self._expand_all)
        dispatcher.connect(signal=actions.DOWNLOAD_FROM_GDRIVE, sender=self.con.tree_id, receiver=self._download_file_from_gdrive)
        dispatcher.connect(signal=actions.DELETE_SINGLE_FILE, sender=self.con.tree_id, receiver=self._delete_single_file)
        dispatcher.connect(signal=actions.DELETE_SUBTREE, sender=self.con.tree_id, receiver=self._delete_subtree)

    # ACTIONS begin
    # ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

    def _call_exiftool_list(self, sender, node_list: List[DisplayNode]):
        for item in node_list:
            self._call_exiftool(sender, item.full_path)

    def _call_exiftool(self, sender, full_path):
        """exiftool -AllDates="2001:01:01 12:00:00" *
        exiftool -Comment="Hawaii" {target_dir}
        find . -name "*jpg_original" -exec rm -fv {} \;
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

        logger.info(f'Calling exiftool for: {full_path}')
        args = ["exiftool", f'-AllDates="{date_to_set} 12:00:00"']
        if comment_to_set:
            args.append(f'-Comment="{comment_to_set}"')
        args.append(full_path)
        subprocess.run(args)

        list_original_files = [f.path for f in os.scandir(full_path) if not f.is_dir() and f.path.endswith('.jpg_original')]
        for file in list_original_files:
            logger.debug(f'Removing file: {file}')
            os.remove(file)

    def _download_file_from_gdrive(self, sender, node: GoogFile):
        gdrive_client = GDriveClient(self.con.config)

        os.makedirs(name=self.download_dir, exist_ok=True)
        dest_file = os.path.join(self.download_dir, node.name)
        try:
            gdrive_client.download_file(node.goog_id, dest_file)
            if self.post_download_action == OPEN:
                self._call_xdg_open(sender, dest_file)
            elif self.post_download_action == SHOW:
                self._show_in_nautilus(sender, dest_file)
        except Exception as err:
            self.con.parent_win.show_error_msg('Download failed', repr(err))
            raise

    def _call_xdg_open(self, sender, full_path):
        if os.path.exists(full_path):
            logger.info(f'Calling xdg-open for: {full_path}')
            subprocess.run(["xdg-open", full_path])
        else:
            self.con.parent_win.show_error_msg(f'Cannot open file', f'File not found: {full_path}')

    def _show_in_nautilus(self, sender, full_path):
        if os.path.exists(full_path):
            logger.info(f'Opening in Nautilus: {full_path}')
            subprocess.run(["nautilus", "--browser", full_path])
        else:
            self.con.parent_win.show_error_msg('Cannot open file in Nautilus', f'File not found: {full_path}')

    def _expand_all(self, sender, tree_path):
        self.con.display_mutator.expand_all(tree_path)

    def _delete_single_file(self, sender, node: DisplayNode):
        self._delete_subtree(sender, node)

    def _delete_subtree(self, sender, node: DisplayNode = None, node_list: List[DisplayNode] = None):
        if not node_list and node:
            node_list = [node]
        logger.debug(f'Setting up delete for {len(node_list)} nodes')

        file_dict = {}
        for node_to_delete in node_list:
            if node_to_delete.is_dir():
                # Add all its descendants. Assume that we came from a display tree which may not have all its children.
                # Need to look things up in the central cache. We will focus on deleting files, and will delete empty parent dirs as needed.
                subtree_file_list, dir_list = self.con.parent_win.application.cache_manager.get_all_files_and_dirs_for_subtree(
                    node_to_delete.node_identifier)
                # FIXME: we need to incorporate directories into delete. This means building a tree and changing everything...
                for file in subtree_file_list:
                    # mark for deletion
                    file.node_identifier.category = Category.Deleted
            else:
                node_to_delete.node_identifier.category = Category.Deleted
                subtree_file_list = [node_to_delete]

            # remove duplicates
            for f in subtree_file_list:
                file_dict[f.uid] = f

        total_list = list(file_dict.values())
        builder = CommandBuilder(self.con.parent_win.application)
        command_batch = builder.build_command_batch(delete_list=total_list)
        # This should fire listeners which ultimately populate the tree:
        self.con.parent_win.application.cache_manager.add_command_batch(command_batch)
