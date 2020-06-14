import os
import logging
import re
from typing import List
import subprocess
from pydispatch import dispatcher

import file_util
from gdrive.client import GDriveClient
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
            self._call_exiftool(item.full_path)

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
        subprocess.check_call(args)

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
            subprocess.check_call(["xdg-open", full_path])
        else:
            self.con.parent_win.show_error_msg(f'Cannot open file', f'File not found: {full_path}')

    def _show_in_nautilus(self, sender, full_path):
        if os.path.exists(full_path):
            logger.info(f'Opening in Nautilus: {full_path}')
            subprocess.check_call(["nautilus", "--browser", full_path])
        else:
            self.con.parent_win.show_error_msg('Cannot open file in Nautilus', f'File not found: {full_path}')

    def _expand_all(self, sender, tree_path):
        self.con.display_mutator.expand_all(tree_path)

    def _delete_single_file(self, sender, node: DisplayNode):
        logger.debug(f'Deleting single file: {node.node_identifier}')
        # # "Left tree" here is the source tree, and "right tree" is the dst tree:
        # change_maker = ChangeMaker(left_tree=self.con.get_tree(), right_tree=self.con.get_tree(),
        #                            application=self.con.parent_win.application)
        # change_maker.copy_nodes_left_to_right(self._drag_data.nodes, dest_node)
        # builder = CommandBuilder(self.con.parent_win.application)
        # command_plan = builder.build_command_plan(change_tree=change_maker.change_tree_right)
        # # This should fire listeners which ultimately populate the tree:
        # self.con.parent_win.application.command_executor.enqueue(command_plan)

    def _delete_subtree(self, sender, node: DisplayNode):
        pass
