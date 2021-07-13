import logging
from typing import Dict, List

import util.format
from backend.display_tree.active_tree_meta import ActiveDisplayTreeMeta
from backend.display_tree.change_tree import ChangeTree
from backend.display_tree.filter_state import FilterState
from constants import TreeType
from model.device import Device
from model.node.container_node import CategoryNode, RootTypeNode
from model.node.directory_stats import DirectoryStats
from model.node_identifier import GUID
from model.uid import UID
from model.user_op import DISPLAYED_USER_OP_TYPES

logger = logging.getLogger(__name__)


class TreeSummarizer:

    @staticmethod
    def _get_tree_type_for_device_uid(device_uid: UID, device_list: List[Device]) -> TreeType:
        for device in device_list:
            if device.uid == device_uid:
                return device.tree_type
        raise RuntimeError(f'Could not find device with UID: {device_uid}')

    @staticmethod
    def build_tree_summary(tree_meta: ActiveDisplayTreeMeta, device_list: List[Device]):
        tree_id = tree_meta.tree_id
        root_sn = tree_meta.root_sn

        uses_uid_key = False
        # Do not use filtered stats at all for now:
        is_filtered = False
        if tree_meta.dir_stats_unfiltered_by_guid:
            dir_stats_dict = tree_meta.dir_stats_unfiltered_by_guid
        else:
            # this will only happen for first-order trees pulling directly from the cache:
            uses_uid_key = True
            dir_stats_dict = tree_meta.dir_stats_unfiltered_by_uid

        if tree_meta.change_tree:
            logger.debug(f'[{tree_id}] This is a ChangeTree: it will provide the summary')
            return TreeSummarizer._build_change_tree_summary(tree_meta.change_tree, tree_meta.filter_state, dir_stats_dict, device_list)

        if not tree_meta.root_exists or not root_sn or not root_sn.node:
            logger.debug(f'[{tree_id}] No summary (tree does not exist)')
            return 'Tree does not exist'

        if uses_uid_key:
            key = root_sn.spid.node_uid
        else:
            key = root_sn.spid.guid
        root_stats = dir_stats_dict.get(key, None)
        if not root_stats:
            logger.error(f'Contents of dir_stats_dict: {dir_stats_dict}')
            raise RuntimeError(f'[{tree_id}] (is_filtered={is_filtered}) No stats found for root node with GUID: {root_sn.spid.guid}')

        tree_type = TreeSummarizer._get_tree_type_for_device_uid(root_sn.spid.device_uid, device_list)

        if tree_type == TreeType.GDRIVE:
            logger.debug(f'[{tree_id}] Generating summary for GDrive tree: {root_sn.spid}')
            return TreeSummarizer._build_summary(root_stats, is_filtered, 'folder')
        else:
            assert tree_type == TreeType.LOCAL_DISK
            logger.debug(f'[{tree_id}] Generating summary for LocalDisk tree: {root_sn.spid}')
            return TreeSummarizer._build_summary(root_stats, is_filtered, 'dir')

    @staticmethod
    def _build_summary(stat: DirectoryStats, is_filtered: bool, dir_str: str) -> str:
        """For Order 1 trees (local master or gdrive master)"""

        total_bytes = stat.get_size_bytes() + stat.trashed_bytes
        size_hf = util.format.humanfriendlier_size(total_bytes)
        filter_pre = ''
        if is_filtered:
            filter_pre = 'Showing: '

        trashed_str = ''
        if stat.trashed_bytes or stat.trashed_file_count or stat.trashed_dir_count:
            trashed_size_hf = util.format.humanfriendlier_size(stat.trashed_bytes)
            file_s = '' if stat.trashed_file_count == 1 else 's'
            dir_s = '' if stat.trashed_dir_count == 1 else 's'
            trashed_str = f' (including {trashed_size_hf} in {stat.trashed_file_count:n} file{file_s} & {stat.trashed_dir_count:n} {dir_str}{dir_s} trashed)'

        total_files = stat.file_count + stat.trashed_file_count
        total_dirs = stat.dir_count + stat.trashed_dir_count
        file_s = '' if total_files == 1 else 's'
        dir_s = '' if total_dirs == 1 else 's'
        return f'{filter_pre}{size_hf} total in {total_files:n} file{file_s} & {total_dirs:n} {dir_str}{dir_s}{trashed_str}'

    @staticmethod
    def _build_simple_summary(dir_stats: DirectoryStats, dir_str) -> str:
        if not dir_stats or (not dir_stats.file_count and not dir_stats.dir_count):
            return '0 items'
        size = util.format.humanfriendlier_size(dir_stats.get_size_bytes())
        file_s = '' if dir_stats.file_count == 1 else 's'
        dir_s = '' if dir_stats.dir_count == 1 else 's'
        return f'{size} in {dir_stats.file_count:n} file{file_s} and {dir_stats.dir_count:n} {dir_str}{dir_s}'

    # ChangeTree
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    @staticmethod
    def _make_cat_map():
        cm = {}
        for op_type, disp_str in DISPLAYED_USER_OP_TYPES.items():
            cm[op_type] = f'{disp_str}: 0'
        return cm

    @staticmethod
    def _build_cat_summaries_str(cat_map) -> str:
        cat_summaries = []
        for op_type in DISPLAYED_USER_OP_TYPES.keys():
            summary = cat_map.get(op_type, None)
            if summary:
                cat_summaries.append(summary)
        return ', '.join(cat_summaries)

    @staticmethod
    def _build_summary_cat_map(tree: ChangeTree, spid, filter_state: FilterState, dir_stats_dict: Dict[GUID, DirectoryStats]):
        include_empty_op_types = False
        cat_count = 0
        if include_empty_op_types:
            cat_map = TreeSummarizer._make_cat_map()
        else:
            cat_map = {}
        for cat_sn in tree.get_child_list_for_spid(spid):
            cat_count += 1
            assert isinstance(cat_sn.node, CategoryNode), f'Not a CategoryNode: {cat_sn.node}'
            cat_stats = dir_stats_dict.get(cat_sn.spid.guid, None)
            if not cat_stats:
                raise RuntimeError(f'[{tree.tree_id}] (is_filtered={filter_state.has_criteria()}) No stats found for cat node {cat_sn.spid}')
            summary = TreeSummarizer._build_simple_summary(cat_stats, 'dir')
            cat_map[cat_sn.spid.op_type] = f'{cat_sn.node.name}: {summary}'
        if cat_count:
            return cat_map
        else:
            return None

    @staticmethod
    def _build_change_tree_summary(tree: ChangeTree, filter_state: FilterState, dir_stats_dict: Dict[GUID, DirectoryStats], device_list: List[Device])\
            -> str:
        # TODO: do we want to create a different summary for filtered views?
        if tree.show_whole_forest:
            # need to preserve ordering...
            type_summaries = []
            device_map = {}
            cat_count = 0
            for device_sn in tree.get_child_list_for_spid(tree.get_root_spid()):
                assert isinstance(device_sn.node, RootTypeNode), f'For {device_sn}'
                cat_map = TreeSummarizer._build_summary_cat_map(tree, device_sn.spid, filter_state, dir_stats_dict)
                if cat_map:
                    cat_count += 1
                    device_map[device_sn.spid.device_uid] = cat_map
            if cat_count == 0:
                return 'Contents are identical'
            for device in device_list:
                cat_map = device_map.get(device.uid, None)
                if cat_map:
                    type_summaries.append(f'{device.friendly_name}: {TreeSummarizer._build_cat_summaries_str(cat_map)}')
            return '; '.join(type_summaries)
        else:
            cat_map = TreeSummarizer._build_summary_cat_map(tree, tree.get_root_spid(), filter_state, dir_stats_dict)
            if not cat_map:
                return 'Contents are identical'
            return TreeSummarizer._build_cat_summaries_str(cat_map)
