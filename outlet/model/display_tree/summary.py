import logging
from typing import Dict

import util.format
from backend.store.tree.active_tree_meta import ActiveDisplayTreeMeta
from backend.store.tree.change_tree import ChangeTree
from backend.store.tree.filter_state import FilterState
from constants import TREE_TYPE_GDRIVE, TREE_TYPE_LOCAL_DISK
from model.node.container_node import CategoryNode, RootTypeNode
from model.node.directory_stats import DirectoryStats
from model.node.node import Node
from model.uid import UID
from model.user_op import DISPLAYED_USER_OP_TYPES

logger = logging.getLogger(__name__)


class TreeSummarizer:

    @staticmethod
    def build_tree_summary(tree_id: str, root_node: Node, tree_meta: ActiveDisplayTreeMeta):
        if tree_meta.filter_state.has_criteria():
            is_filtered = True
            dir_stats = tree_meta.filter_state.get_dir_stats()
        else:
            is_filtered = False
            dir_stats = tree_meta.dir_stats_unfiltered

        if tree_meta.change_tree:
            logger.debug(f'[{tree_id}] This is a ChangeTree: it will provide the summary')
            return TreeSummarizer._build_change_tree_summary(tree_meta.change_tree, tree_meta.filter_state, dir_stats)

        if not root_node:
            logger.debug(f'[{tree_id}] No summary (tree does not exist)')
            return 'Tree does not exist'

        root_stats = dir_stats.get(root_node.uid, None)
        if not root_stats:
            raise RuntimeError(f'[{tree_id}] (is_filtered={is_filtered}) No stats found for root node {root_node}')

        if root_node.get_tree_type() == TREE_TYPE_GDRIVE:
            logger.debug(f'[{tree_id}] Generating summary for GDrive tree: {root_node.node_identifier}')
            return TreeSummarizer._build_summary(root_stats, is_filtered, 'folder')
        else:
            assert root_node.get_tree_type() == TREE_TYPE_LOCAL_DISK
            logger.debug(f'[{tree_id}] Generating summary for LocalDisk tree: {root_node.node_identifier}')
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
        return f'{size} in {dir_stats.file_count:n} files and {dir_stats.dir_count:n} {dir_str}'

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
    def _build_summary_cat_map(tree: ChangeTree, uid, filter_state: FilterState, dir_stats_map: Dict[UID, DirectoryStats]):
        include_empty_op_types = False
        cat_count = 0
        if include_empty_op_types:
            cat_map = TreeSummarizer._make_cat_map()
        else:
            cat_map = {}
        for cat_node in tree.get_child_list_for_uid(uid):
            cat_count += 1
            assert isinstance(cat_node, CategoryNode), f'Not a CategoryNode: {cat_node}'
            cat_stats = dir_stats_map.get(cat_node.uid, None)
            if not cat_stats:
                raise RuntimeError(f'[{tree.tree_id}] (is_filtered={filter_state.has_criteria()}) No stats found for cat node {cat_node}')
            summary = TreeSummarizer._build_simple_summary(cat_stats, 'dir')
            cat_map[cat_node.op_type] = f'{cat_node.name}: {summary}'
        if cat_count:
            return cat_map
        else:
            return None

    @staticmethod
    def _build_change_tree_summary(tree: ChangeTree, filter_state: FilterState, dir_stats_map: Dict[UID, DirectoryStats]) -> str:
        # FIXME: add support for filters
        if tree.show_whole_forest:
            # need to preserve ordering...
            type_summaries = []
            type_map = {}
            cat_count = 0
            for child in tree.get_child_list(tree.get_root_node()):
                assert isinstance(child, RootTypeNode), f'For {child}'
                cat_map = TreeSummarizer._build_summary_cat_map(tree, child.uid, filter_state, dir_stats_map)
                if cat_map:
                    cat_count += 1
                    type_map[child.node_identifier.tree_type] = cat_map
            if cat_count == 0:
                return 'Contents are identical'
            for tree_type, tree_type_name in (TREE_TYPE_LOCAL_DISK, 'Local Disk'), (TREE_TYPE_GDRIVE, 'Google Drive'):
                cat_map = type_map.get(tree_type, None)
                if cat_map:
                    type_summaries.append(f'{tree_type_name}: {TreeSummarizer._build_cat_summaries_str(cat_map)}')
            return '; '.join(type_summaries)
        else:
            cat_map = TreeSummarizer._build_summary_cat_map(tree.get_root_node().uid)
            if not cat_map:
                return 'Contents are identical'
            return TreeSummarizer._build_cat_summaries_str(cat_map)
