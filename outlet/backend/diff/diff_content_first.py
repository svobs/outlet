"""Content-first diff. See diff function below."""
import collections
import logging
import time
from typing import Callable, DefaultDict, Dict, List, Tuple

from backend.diff.change_maker import ChangeMaker, OneSide, SPIDNodePair
from backend.display_tree.change_tree import ChangeTree
from backend.tree_store.local import content_hasher
from constants import TreeType
from model.user_op import UserOpType
from util.stopwatch_sec import Stopwatch

logger = logging.getLogger(__name__)


class OneSideDiffMeta:
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS OneSideDiffMeta

    Just a storage struct for a lot of internally needed junk
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self):
        self.md5_dict: DefaultDict[str, List[SPIDNodePair]] = collections.defaultdict(lambda: list())
        self.path_dict: DefaultDict[str, List[SPIDNodePair]] = collections.defaultdict(lambda: list())


class ContentFirstDiffer(ChangeMaker):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS ContentFirstDiffer
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, backend, left_tree_sn: SPIDNodePair, right_tree_sn: SPIDNodePair, tree_id_left: str, tree_id_right: str,
                 tree_id_left_src: str, tree_id_right_src: str):
        super().__init__(backend, left_tree_sn, right_tree_sn, tree_id_left_src, tree_id_right_src, tree_id_left, tree_id_right)

    def generate_one_side_diff_meta(self, side: OneSide) -> OneSideDiffMeta:
        """Let's build a path dict while we are building the MD5 dict. Performance gain expected to be small for local trees and moderate
        for GDrive trees"""
        sw = Stopwatch()
        meta: OneSideDiffMeta = OneSideDiffMeta()

        def on_file_found(sn: SPIDNodePair):
            if not sn.node.md5 and not content_hasher.try_calculating_signatures(sn.node):
                logger.error(f'Unable to calculate signature for file! Skipping: {sn.node.get_single_path()}')
                return

            if sn.node.md5:
                meta.md5_dict[sn.node.md5].append(sn)
                path = sn.spid.get_single_path()
                if path in meta.path_dict:
                    logger.warning(f'Found additional node at path: "{sn.spid.get_single_path()}" (tree={side.root_sn.spid}).')
                    on_file_found.count_duplicate_paths += 1
                meta.path_dict[path].append(sn)
                on_file_found.count_nodes += 1
            else:
                logger.debug(f'No MD5 for node; skipping: {sn.spid.get_single_path()}')

        on_file_found.count_nodes = 0
        on_file_found.count_duplicate_paths = 0

        self.visit_each_sn_for_subtree(side.root_sn, on_file_found, side.tree_id_src)

        logger.info(f'{sw} Found {len(meta.md5_dict)} MD5s for {on_file_found.count_nodes} nodes (including '
                    f'{on_file_found.count_duplicate_paths} duplicate paths)')
        return meta

    def _process_nonmatching_relative_path_pairs(self, lefts: List[SPIDNodePair], rights: List[SPIDNodePair],
                                                 on_mismatched_pair: Callable, on_left_only: Callable, on_right_only: Callable) -> None:
        """This returns a set of pairs for items whose MD5s match but whose relative paths *do not* match.
        All elements in both the 'lefts' and 'rights' parameter lists are expected to contain the same MD5."""
        # Check for the trivial cases first:
        if not lefts:
            for right_sn in rights:
                on_right_only(right_sn)
            return
        if not rights:
            for left_sn in lefts:
                on_left_only(left_sn)
            return

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
            on_mismatched_pair(relpath_sn_dict_s.popitem()[1], sn_list_only_r.pop())

        assert not (len(relpath_sn_dict_s) > 0 and len(sn_list_only_r) > 0)

        # Remaining lefts are without a matching right
        while len(relpath_sn_dict_s) > 0:
            on_left_only(relpath_sn_dict_s.popitem()[1])

        # Remaining rights are without a matching left
        while len(sn_list_only_r) > 0:
            on_right_only(sn_list_only_r.pop())

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
        logger.info('Diffing files by MD5...')
        count_add_delete_pairs = 0
        count_moved_pairs = 0
        count_updated_pairs = 0

        # List of list of items which do not have a matching md5 on the other side.
        # We will compare these by path.
        # Note: each list within this list contains duplicates (nodes with the same md5)
        sn_list_only_s: List[SPIDNodePair] = []
        sn_list_only_r: List[SPIDNodePair] = []

        # the set of MD5s already processed
        md5_set_stopwatch = Stopwatch()

        meta_s: OneSideDiffMeta = self.generate_one_side_diff_meta(self.left_side)
        meta_r: OneSideDiffMeta = self.generate_one_side_diff_meta(self.right_side)

        md5_union_set = meta_s.md5_dict.keys() | meta_r.md5_dict.keys()
        logger.debug(f'{md5_set_stopwatch} Found {len(md5_union_set)} combined MD5s')

        """Compares the two trees, and populates the change sets of both. The order of 'left' and which is 'right'
         is not important, because the changes are computed from each tree's perspective (e.g. a file which is in
         Left but not Right will be determined to be an 'added' file from the perspective of Left but a 'deleted'
         file from the perspective of Right)"""

        sw = Stopwatch()
        for md5 in md5_union_set:
            # Grant just a tiny bit of time to other tasks in the CPython thread (e.g. progress bar):
            # TODO: investigate coroutines
            time.sleep(0.00001)

            # Set of items on S with same MD5:
            single_md5_sn_list_s: List[SPIDNodePair] = meta_s.md5_dict[md5]
            single_md5_sn_list_r: List[SPIDNodePair] = meta_r.md5_dict[md5]

            if not single_md5_sn_list_s:
                # Content is only present on RIGHT side
                sn_list_only_r += single_md5_sn_list_r
            elif not single_md5_sn_list_r:
                # Content is only present on LEFT side
                sn_list_only_s += single_md5_sn_list_s
            elif compare_paths_also:
                # Content is present on BOTH sides but paths may be different
                """If we do this, we care about what the files are named, where they are located, and how many
                duplicates exist. When it comes to determining the direction of renamed files, we simply don't
                have enough info to be conclusive, but as this is a secondary concern (our first is ensuring
                we have *any* matching signatures (see above)) I am ok with making a best guess and letting the
                user make the final call via the UI. Here we can choose either to use the modification times
                (newer is assumed to be the rename destination), or for each side to assume it is the destination
                (similar to how we handle missing signatures above)"""

                def on_mismatched_pair(_sn_s: SPIDNodePair, _sn_r: SPIDNodePair):
                    # MOVED: the file already exists in each tree, so just do a rename within the tree
                    # (it is possible that the trees are on different disks, so keep performance in mind)
                    self.append_mv_op_r_to_r(_sn_s, _sn_r)

                    self.append_mv_op_s_to_s(_sn_s, _sn_r)
                    on_mismatched_pair.count_moved_pairs += 1

                on_mismatched_pair.count_moved_pairs = 0

                def on_left_only(_sn_s: SPIDNodePair):
                    # There is an additional file with same signature on LEFT
                    sn_list_only_s.append(_sn_s)

                def on_right_only(_sn_r: SPIDNodePair):
                    # There is an additional file with same signature on RIGHT
                    sn_list_only_r.append(_sn_r)

                self._process_nonmatching_relative_path_pairs(single_md5_sn_list_s, single_md5_sn_list_r,
                                                              on_mismatched_pair=on_mismatched_pair, on_left_only=on_left_only,
                                                              on_right_only=on_right_only)
                count_moved_pairs += on_mismatched_pair.count_moved_pairs

        logger.debug(f'{sw} Finished first pass of MD5 set (1/3)')

        sw = Stopwatch()
        # List of (possibly non-unique) MD5s (but unique paths) on left side only (i.e. relative path not present on right):
        for sn_s in sn_list_only_s:
            # need to look up the actual node
            if compare_paths_also:
                # We already examined files with same signature on each side.
                # So now examine each unmatched signature and see if the paths match. If so, we have UPDATED files.
                left_on_right_path: str = self.get_path_moved_to_right(sn_s.spid)
                existing_sn_list_r: List[SPIDNodePair] = meta_r.path_dict.get(left_on_right_path)
                if existing_sn_list_r:
                    if len(existing_sn_list_r) > 1:
                        assert self.right_side.root_identifier.tree_type == TreeType.GDRIVE, \
                            f'Should never see multiple nodes for same path ("{left_on_right_path}") for this tree type, ' \
                            f'but found: {existing_sn_list_r}'
                        logger.debug(f'Found {len(existing_sn_list_r)} nodes at path "{left_on_right_path}"; picking the first one')
                    # GDrive creates a hard problem because it can allow nodes with the same name and path. Just pick first one for now.
                    # We can try to clean things up in the command executor.
                    existing_sn_r: SPIDNodePair = existing_sn_list_r[0]
                    assert existing_sn_r.node.is_live(), f'non-existent nodes should have been pre-filtered: {existing_sn_r.node}'
                    assert existing_sn_r.node.md5 != sn_s.node.md5, \
                        f'Expected different MD5 for left node ({sn_s.node}) and right node ({existing_sn_r})'
                    # UPDATED
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.debug(f'File updated: {sn_s.node.md5} <- "{sn_s.spid.get_single_path()}" -> {existing_sn_r.node.md5}')
                    # Same path, different md5 -> Updated.
                    # Remember, we don't know which direction is "correct" so we supply ops in both directions:
                    self.append_up_op_r_to_s(existing_sn_r, sn_s)
                    self.append_up_op_s_to_r(existing_sn_r, sn_s)
                    count_updated_pairs += 1
                    continue
                # No match? fall through

            # ADDED on right + DELETED on left
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f'Left has new file: "{sn_s.spid.get_single_path()}"')
            self.append_cp_op_s_to_r(sn_s)

            # Dead node walking:
            self.left_side.add_op(UserOpType.RM, sn_s)
            count_add_delete_pairs += 1
        logger.info(f'{sw} Finished path comparison for left tree (2/3)')

        sw = Stopwatch()
        for sn_r in sn_list_only_r:
            if compare_paths_also:
                right_on_left_path: str = self.get_path_moved_to_left(sn_r.spid)
                if meta_s.path_dict.get(right_on_left_path):
                    # UPDATED. Logically this has already been covered (above) since our iteration is symmetrical:
                    continue
            # DUPLICATE ADDED on right + DELETED on left
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f'Right has new file: "{sn_r.spid.get_single_path()}"')
            self.append_cp_op_r_to_s(sn_r)

            # Dead node walking:
            self.right_side.add_op(UserOpType.RM, sn_r)
            count_add_delete_pairs += 1

        logger.info(f'{sw} Finished path comparison for right tree (3/3)')

        logger.info(f'Done with diff (pairs: '
                    f'add/del={count_add_delete_pairs} '
                    f'upd={count_updated_pairs} '
                    f'moved={count_moved_pairs})')
        logger.info(f'{sw} Finished path comparison for right tree')

        return self.left_side.change_tree, self.right_side.change_tree
