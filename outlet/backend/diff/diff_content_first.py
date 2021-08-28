"""Content-first diff. See diff function below."""
import collections
import logging
import time
from typing import Callable, DefaultDict, Dict, Iterable, List, Optional, Tuple

from backend.diff.change_maker import ChangeMaker, OneSide, SPIDNodePair
from backend.display_tree.change_tree import ChangeTree
from backend.tree_store.local import content_hasher
from constants import TreeType
from model.user_op import UserOpType
from util.stopwatch_sec import Stopwatch

logger = logging.getLogger(__name__)


class OneSideSourceMeta:
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS OneSideSourceMeta

    Just a storage struct for a lot of internally needed junk
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self):
        self.md5_dict: DefaultDict[str, List[SPIDNodePair]] = collections.defaultdict(lambda: list())
        self.path_dict: DefaultDict[str, List[SPIDNodePair]] = collections.defaultdict(lambda: list())


class DiffState:
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS DiffState

    Data structure for holding intermediate state as we conduct the diff.
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, compare_paths_also: bool, src_meta_s: OneSideSourceMeta, src_meta_r: OneSideSourceMeta):
        self.compare_paths_also: bool = compare_paths_also

        # Information about each side's source tree
        self.src_meta_s: OneSideSourceMeta = src_meta_s
        self.src_meta_r: OneSideSourceMeta = src_meta_r

        # List of items which do not have a matching md5 on the other side. We will compare these by path.
        # Note: each list within this list contains duplicates (nodes with the same md5)
        self.sn_list_only_s: List[SPIDNodePair] = []
        self.sn_list_only_r: List[SPIDNodePair] = []

        self.count_add_delete_pairs = 0
        self.count_move_pairs = 0
        self.count_update_pairs = 0


class ContentFirstDiffer(ChangeMaker):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS ContentFirstDiffer
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, backend, left_tree_sn: SPIDNodePair, right_tree_sn: SPIDNodePair, tree_id_left: str, tree_id_right: str,
                 tree_id_left_src: str, tree_id_right_src: str):
        super().__init__(backend, left_tree_sn, right_tree_sn, tree_id_left_src, tree_id_right_src, tree_id_left, tree_id_right)

    def build_one_side_src_meta(self, side: OneSide) -> OneSideSourceMeta:
        """Let's build a path dict while we are building the MD5 dict. Performance gain expected to be small for local trees and moderate
        for GDrive trees"""
        sw = Stopwatch()
        meta: OneSideSourceMeta = OneSideSourceMeta()
        duplicate_path_skipped_sn_list: List[SPIDNodePair] = []

        def on_file_found(sn: SPIDNodePair):
            if not sn.node.md5 and not content_hasher.try_calculating_signatures(sn.node):
                # TODO: handle GDrive objects which don't have MD5s
                logger.warning(f'Unable to calculate signature for file, skipping: {sn.spid}')
                on_file_found.count_skipped_no_md5 += 1
                return
                
            path = sn.spid.get_single_path()
            if path in meta.path_dict:
                on_file_found.count_duplicates += 1
                duplicate_path_skipped_sn_list.append(sn)
            else:
                meta.path_dict[path].append(sn)
                meta.md5_dict[sn.node.md5].append(sn)
                on_file_found.count_file_nodes += 1

        on_file_found.count_file_nodes = 0
        on_file_found.count_duplicates = 0
        on_file_found.count_skipped_no_md5 = 0

        self.visit_each_sn_for_subtree(side.root_sn, on_file_found, side.tree_id_src)

        # Do this only after we have collected all our MD5s:
        for dup_sn in duplicate_path_skipped_sn_list:
            prev_path_sn = meta.path_dict[dup_sn.spid.get_single_path()][-1]
            prev_md5_sn_list = meta.md5_dict[dup_sn.node.md5]
            if prev_md5_sn_list:
                prev_md5_sn = prev_md5_sn_list[-1]
                logger.warning(f'[{side.tree_id}] Skipping node with duplicate path & duplicate MD5: (spid={dup_sn.spid}, MD5={dup_sn.node.md5}): '
                               f' prev node with same path: (spid={prev_path_sn.spid}, MD5={prev_path_sn.node.md5});'
                               f' prev node with same MD5: (spid={prev_md5_sn.spid}, MD5={prev_md5_sn.node.md5})')
            else:
                # this seems like a worse error, if we are dropping the MD5
                logger.error(f'[{side.tree_id}] Skipping node with duplicate path but unique MD5: (spid={dup_sn.spid}, MD5={dup_sn.node.md5}): '
                             f' prev node with same path: (spid={prev_path_sn.spid}, MD5={prev_path_sn.node.md5})')

        logger.info(f'[{side.tree_id}] {sw} Found {len(meta.md5_dict)} MD5s & {len(meta.path_dict)} paths for {on_file_found.count_file_nodes} file '
                    f'nodes (including {on_file_found.count_duplicates} skipped due to duplicate paths '
                    f'& {on_file_found.count_skipped_no_md5} skipped due to missing MD5)')
        return meta

    def _match_relative_paths_for_same_md5(self, lefts: List[SPIDNodePair], rights: List[SPIDNodePair], pairing_func: Callable) \
            -> Tuple[Iterable[SPIDNodePair], Iterable[SPIDNodePair]]:
        """This returns a set of pairs for items whose MD5s match but whose relative paths *do not* match.
        All elements in both the 'lefts' and 'rights' parameter lists are expected to contain the same MD5."""
        # it's not efficient to use this method if one side has no nodes:
        assert lefts and rights, f'_match_relative_paths_for_same_md5(): this method should not be used when one side has no paths!'

        # Put all left identifiers in dict. Key is relative path
        relpath_sn_dict_s: Dict[str, SPIDNodePair] = {}
        for sn_s in lefts:
            relpath_s = self.left_side.derive_relative_path(sn_s.spid)
            if relpath_s in relpath_sn_dict_s:
                # GDrive permits this unfortunately. But if node is a complete duplicate, do we really care?
                assert sn_s.spid.tree_type == TreeType.GDRIVE, f'Duplicate node with same MD5 and same location but not GDrive: {sn_s.spid}'
                logger.warning(f'Found node with same MD5 and same location; will ignore for diff: {sn_s.spid}')
            else:
                relpath_sn_dict_s[relpath_s] = sn_s

        sn_list_only_r: List[SPIDNodePair] = []
        for sn_r in rights:
            relpath_r: str = self.right_side.derive_relative_path(sn_r.spid)
            if relpath_r in relpath_sn_dict_s:
                # discard left. silently discard right
                relpath_sn_dict_s.pop(relpath_r, None)
            else:
                # no match
                sn_list_only_r.append(sn_r)

        # Pair as many as possible, choosing arbitrary items for each pairing (these will be treated as renames/moves):
        while len(relpath_sn_dict_s) > 0 and len(sn_list_only_r) > 0:
            pairing_func(relpath_sn_dict_s.popitem()[1], sn_list_only_r.pop())

        assert not (len(relpath_sn_dict_s) > 0 and len(sn_list_only_r) > 0), f'Both sides have remaining values! At most one side should.'

        return relpath_sn_dict_s.values(), sn_list_only_r

    def _compare_nodes_for_single_md5(self, md5: str, state: DiffState):
        # Set of items on S with same MD5:
        sn_list_s: List[SPIDNodePair] = state.src_meta_s.md5_dict[md5]
        for sn in sn_list_s:
            assert sn.node.md5 == md5, f'Expected MD5={md5} in {sn}'

        # Set of items on R with same MD5:
        sn_list_r: List[SPIDNodePair] = state.src_meta_r.md5_dict[md5]
        for sn in sn_list_r:
            assert sn.node.md5 == md5, f'Expected MD5={md5} in {sn}'

        logger.debug(f'[MD5={md5}] Found {len(sn_list_s)} S nodes and {len(sn_list_r)} R nodes with this MD5')

        if not sn_list_s:
            # Easy case: Content is only present on RIGHT side
            logger.debug(f'[MD5={md5}] R only: {[sn.spid for sn in sn_list_r]}')
            state.sn_list_only_r = state.sn_list_only_r + sn_list_r
            return

        if not sn_list_r:
            # Easy case: Content is only present on LEFT side
            logger.debug(f'[MD5={md5}] S only: {[sn.spid for sn in sn_list_s]}')
            state.sn_list_only_s = state.sn_list_only_s + sn_list_s
            return

        # Content is present on BOTH sides but paths may be different...

        if not state.compare_paths_also:
            # This MD5 has at least one copy on each side. If we don't care about paths, we also don't care about copies. Just return.
            return

        """About compare_paths_also==True:
        If we do this, we care about what the files are named, where they are located, and how many
        duplicates exist. When it comes to determining the direction of renamed files, we simply don't
        have enough info to be conclusive, but as this is a secondary concern (our first is ensuring
        we have *any* matching signatures (see above)) I am ok with making a best guess and letting the
        user make the final call via the UI. Here we can choose either to use the modification times
        (newer is assumed to be the rename destination), or for each side to assume it is the destination
        (similar to how we handle missing signatures above)"""

        def create_mv_ops(_sn_s: SPIDNodePair, _sn_r: SPIDNodePair):
            # MOVED: the file already exists in each tree, so just do a rename within the tree
            # (it is possible that the trees are on different disks, so keep performance in mind)
            logger.debug(f'[MD5={md5}] Creating MV pair: S={_sn_s.spid} R={_sn_r.spid}')
            self.append_mv_op_r_to_r(_sn_s, _sn_r)

            self.append_mv_op_s_to_s(_sn_s, _sn_r)
            state.count_move_pairs += 1

        remaining_list_s, remaining_list_r = self._match_relative_paths_for_same_md5(sn_list_s, sn_list_r, pairing_func=create_mv_ops)

        # Can't create MV pairs if one side has more nodes:
        if remaining_list_s:
            for remaining_left in remaining_list_s:
                logger.debug(f'[MD5={md5}] Orphan on left: {remaining_left}')
                state.sn_list_only_s.append(remaining_left)
        if remaining_list_r:
            for remaining_right in remaining_list_r:
                logger.debug(f'[MD5={md5}] Orphan on right: {remaining_right}')
                state.sn_list_only_r.append(remaining_right)

    def diff(self, compare_paths_also=False) -> Tuple[ChangeTree, ChangeTree]:
        """Use this method if we mostly care about having the same unique files *somewhere* in
           each tree (in other words, we care about file contents, and care less about where each
           file is placed). If a file is found with the same signature on both sides but with
           different paths, it is assumed to be renamed/moved.

           Rough algorithm for categorization:
           1. Is file an ignored type? -> IGNORED
           2. For all unique signatures:
           2a. File's signature and path exists on both sides? --> NONE
           2b. File's signature is found on both sides but path is different? --> MOVED
           2c. All files not matched in (2) are orphans.
           3. For all orphans:
           3a. File's path is same on both sides but signature is different? --> UPDATED
           3b. File's signature and path are unique to target side --> DELETED
           3c. File's signature and path are unique to opposite side --> ADDED
           """
        logger.info(f'Diffing files by MD5 (compare_paths_also={compare_paths_also})')

        # the set of MD5s already processed
        md5_set_stopwatch = Stopwatch()

        state = DiffState(compare_paths_also, self.build_one_side_src_meta(self.left_side), self.build_one_side_src_meta(self.right_side))

        md5_union_set = state.src_meta_s.md5_dict.keys() | state.src_meta_r.md5_dict.keys()
        logger.debug(f'{md5_set_stopwatch} Found {len(md5_union_set)} combined MD5s')

        """Compares the two trees, and populates the change sets of both. The order of 'left' and which is 'right'
         is not important, because the changes are computed from each tree's perspective (e.g. a file which is in
         Left but not Right will be determined to be an 'added' file from the perspective of Left but a 'deleted'
         file from the perspective of Right)"""

        sw = Stopwatch()
        for md5 in md5_union_set:
            # Yield to other tasks in the CPython thread (e.g. progress bar):
            time.sleep(0)

            self._compare_nodes_for_single_md5(md5, state)

        # At this point, all MV ops will have been completed. Just checking for updates, adds, and deletes
        logger.debug(f'{sw} Finished first pass of MD5 set (1/3): Unique MD5s: Left={len(state.sn_list_only_s)} Right={len(state.sn_list_only_r)}')

        sw = Stopwatch()
        # Iterate over all the nodes on the left side which have MD5s not present on the right
        for sn_s in state.sn_list_only_s:
            # We already examined files with same signature on each side.
            # So now examine each unmatched signature and see if the paths match. If so, we have UPDATED files.
            if compare_paths_also and self._create_update_pair_for_left_sn(state, sn_s):
                continue
            else:
                # Op pair: (CP left->right, RM from left):
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(f'Left has file with no matching MD5 or path on right: adding CP op on right, RM op on left): '
                                 f'{sn_s.spid} md5={sn_s.node.md5}')

                self.append_cp_op_s_to_r(sn_s)
                self.left_side.add_node_and_new_op(UserOpType.RM, sn_s)

                state.count_add_delete_pairs += 1
        logger.info(f'{sw} Finished path comparison for left tree (2/3)')

        sw = Stopwatch()
        for sn_r in state.sn_list_only_r:
            if compare_paths_also:
                right_on_left_path: str = self.get_path_moved_to_left(sn_r.spid)
                if state.src_meta_s.path_dict.get(right_on_left_path):
                    # NODE UPDATED. Logically this has already been covered (above) since our iteration is symmetrical:
                    continue

            # Op pair: (CP left<-right, RM from right):
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f'Right has file with no matching MD5 or path on left: adding CP op on left, RM op on right: '
                             f'{sn_r.spid} md5={sn_r.node.md5}')

            self.append_cp_op_r_to_s(sn_r)
            self.right_side.add_node_and_new_op(UserOpType.RM, sn_r)
            state.count_add_delete_pairs += 1

        logger.info(f'{sw} Finished path comparison for right tree (3/3)')

        logger.info(f'Done with diff (pairs: '
                    f'CP/RM={state.count_add_delete_pairs} '
                    f'UP/UP={state.count_update_pairs} '
                    f'MV/MV={state.count_move_pairs})')

        return self.left_side.change_tree, self.right_side.change_tree

    def _create_update_pair_for_left_sn(self, state: DiffState, sn_s: SPIDNodePair) -> bool:
        left_on_right_path: str = self.get_path_moved_to_right(sn_s.spid)
        existing_sn_list_r: List[SPIDNodePair] = state.src_meta_r.path_dict.get(left_on_right_path)
        if existing_sn_list_r:
            # Path match: NODE UPDATED

            if len(existing_sn_list_r) > 1:
                assert self.right_side.root_identifier.tree_type == TreeType.GDRIVE, \
                    f'Should never see multiple nodes for same path ("{repr(left_on_right_path)}") for non-GDrive trees, ' \
                    f'but found: {existing_sn_list_r}'
                # GDrive creates a hard problem because it can allow nodes with the same name and path. Just pick first one for now.
                logger.warning(f'Found {len(existing_sn_list_r)} nodes in right tree at path "{repr(left_on_right_path)}"; picking the first')
                logger.debug(f'List={existing_sn_list_r}')

            existing_sn_r: SPIDNodePair = existing_sn_list_r[0]
            assert existing_sn_r.node.is_live(), f'non-existent nodes should have been pre-filtered: {existing_sn_r.node}'
            assert existing_sn_r.node.md5 != sn_s.node.md5, \
                f'Expected different MD5 for left node ({sn_s.node}) and right node ({existing_sn_r.node})'

            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f'Creating UP pair: S=({sn_s.spid} md5={sn_s.node.md5}) <-> R=({existing_sn_r.spid} md5={existing_sn_r.node.md5})')
            # Same path, different md5 -> Updated.
            # Remember, we don't know which direction is "correct" so we supply ops in both directions:
            self.append_up_op_r_to_s(sn_s, existing_sn_r)
            self.append_up_op_s_to_r(sn_s, existing_sn_r)
            state.count_update_pairs += 1
            return True

        # No match
        return False
