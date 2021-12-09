import logging
import threading
from collections import deque
from typing import Deque, Set

from backend.display_tree.active_tree_manager import ActiveTreeManager
from backend.display_tree.active_tree_meta import ActiveDisplayTreeMeta
from constants import CONFIG_DELIMITER, ROWS_OF_INTEREST_SAVE_HOLDOFF_TIME_MS, SUPER_DEBUG_ENABLED, TreeID
from model.display_tree.build_struct import RowsOfInterest
from model.node.node import SPIDNodePair
from model.node_identifier import GUID
from util.holdoff_timer import HoldOffTimer
from util.stopwatch_sec import Stopwatch

logger = logging.getLogger(__name__)


class RowStateTracking:
    """
    Expanded & selected row state tracking
    """

    def __init__(self, backend, active_tree_manager: ActiveTreeManager):
        self.backend = backend
        self._active_tree_manager = active_tree_manager

        self._rows_of_interest_save_timer = HoldOffTimer(holdoff_time_ms=ROWS_OF_INTEREST_SAVE_HOLDOFF_TIME_MS,
                                                         task_func=self.save_all_rows_of_interest)
        self._rows_of_interest_to_save_tree_id_set: Set[TreeID] = set()
        self._tree_id_set_lock = threading.Lock()

        # Register hook for saving all rows of interest when any tree is deregistered
        # make sure any updates are written out first:
        self._active_tree_manager.on_deregister_tree_hook = self.save_all_rows_of_interest

    def get_rows_of_interest(self, tree_id: TreeID) -> RowsOfInterest:
        logger.debug(f'[{tree_id}] Getting rows of interest')

        meta = self._active_tree_manager.get_active_display_tree_meta(tree_id)
        if not meta:
            raise RuntimeError(f'get_rows_of_interest(): DisplayTree not registered: {tree_id}')

        rows_of_interest = RowsOfInterest()
        rows_of_interest.expanded = meta.expanded_row_set
        rows_of_interest.selected = meta.selected_row_set
        if SUPER_DEBUG_ENABLED:
            logger.debug(f'[{tree_id}] get_rows_of_interest(): Returning: expanded={meta.expanded_row_set}, selected={meta.selected_row_set}')
        else:
            logger.debug(f'[{tree_id}] get_rows_of_interest(): Returning {len(meta.expanded_row_set)} expanded & {len(meta.selected_row_set)} selected')

        return rows_of_interest

    def load_rows_of_interest(self, tree_id: TreeID):
        logger.debug(f'[{tree_id}] Loading rows of interest')

        meta = self._active_tree_manager.get_active_display_tree_meta(tree_id)
        if not meta:
            raise RuntimeError(f'load_rows_of_interest(): DisplayTree not registered: {tree_id}')

        # NOTE: the purge process will actually end up populating the expanded_row_set in the display_tree_meta, but we will just overwrite it
        expanded_row_set = self._load_expanded_rows_from_config(meta.tree_id)
        selected_row_set = self._load_selected_rows_from_config(meta.tree_id)
        # rows_of_interest = self._purge_dead_rows(expanded_row_set, selected_row_set, meta)  # TODO
        meta.expanded_row_set = expanded_row_set
        meta.selected_row_set = selected_row_set

    def set_selected_rows(self, tree_id: TreeID, selected: Set[GUID], select_ts: int) -> bool:
        display_tree_meta: ActiveDisplayTreeMeta = self._active_tree_manager.get_active_display_tree_meta(tree_id)
        if not display_tree_meta:
            raise RuntimeError(f'Tree not found in memory: {tree_id}')

        if display_tree_meta.last_select_time_ms > select_ts:
            logger.info(f'[{tree_id}] Discarding request to set new rows selection; last select time for tree '
                        f'({display_tree_meta.last_select_time_ms}) is more recent than the request\'s ({select_ts})')
            return False

        logger.debug(f'[{tree_id}] Storing selection: {selected}')
        display_tree_meta.selected_row_set = selected
        display_tree_meta.last_select_time_ms = select_ts

        self._schedule_rows_of_interest_save(tree_id)
        return True

    def add_expanded_row(self, guid: GUID, tree_id: TreeID):
        """AKA expanding a row on the frontend"""
        display_tree_meta: ActiveDisplayTreeMeta = self._active_tree_manager.get_active_display_tree_meta(tree_id)
        if not display_tree_meta:
            raise RuntimeError(f'Tree not found in memory: {tree_id}')

        if display_tree_meta.root_sn.spid.guid == guid:
            logger.debug(f'[{tree_id}] add_expanded_row(): ignoring root: {guid}')
            return

        logger.debug(f'[{tree_id}] Adding row to expanded_row_set: {guid}')
        display_tree_meta.expanded_row_set.add(guid)

        self._schedule_rows_of_interest_save(tree_id)

    def remove_expanded_row(self, row_guid: GUID, tree_id: TreeID):
        """AKA collapsing a row on the frontend"""
        # TODO: change FE API to send descendants for removal also
        display_tree_meta: ActiveDisplayTreeMeta = self._active_tree_manager.get_active_display_tree_meta(tree_id)
        if not display_tree_meta:
            raise RuntimeError(f'Tree not found in memory: {tree_id}')

        if display_tree_meta.root_sn.spid.guid == row_guid:
            logger.debug(f'[{tree_id}] remove_expanded_row(): ignoring root: {row_guid}')
            return

        try:
            logger.debug(f'[{tree_id}] Removing row from expanded_row_set: {row_guid}')
            display_tree_meta.expanded_row_set.remove(row_guid)
        except Exception as err:
            # We don't care too much about this. Dirs get removed all the time without our knowledge
            logger.debug(f'Failed to remove expanded row {row_guid}: error={repr(err)}')
            return

        self._schedule_rows_of_interest_save(tree_id)

    def _schedule_rows_of_interest_save(self, tree_id: TreeID):
        with self._tree_id_set_lock:
            self._rows_of_interest_to_save_tree_id_set.add(tree_id)

        self._rows_of_interest_save_timer.start_or_delay()

    def save_all_rows_of_interest(self):
        with self._tree_id_set_lock:
            for tree_id in self._rows_of_interest_to_save_tree_id_set:
                display_tree_meta = self._active_tree_manager.get_active_display_tree_meta(tree_id)
                if tree_id:
                    self._save_selected_rows_to_config(display_tree_meta)
                    self._save_expanded_rows_to_config(display_tree_meta)
                else:
                    logger.error(f'[{tree_id}] Could not save rows of interest: tree appears to have been deregistered already')

            self._rows_of_interest_to_save_tree_id_set.clear()

    @staticmethod
    def _make_selected_rows_config_key(tree_id: TreeID) -> str:
        return f'ui_state.{tree_id}.selected_rows'

    @staticmethod
    def _make_expanded_rows_config_key(tree_id: TreeID) -> str:
        return f'ui_state.{tree_id}.expanded_rows'

    def _load_selected_rows_from_config(self, tree_id: TreeID) -> Set[GUID]:
        """Loads the Set of selected rows from app_config file"""
        logger.debug(f'[{tree_id}] Loading selected rows from app_config')
        try:
            selected_row_set: Set[GUID] = set()
            selected_rows_unparsed: str = self.backend.get_config(self._make_selected_rows_config_key(tree_id), default_val='', required=False)
            if selected_rows_unparsed:
                for guid in selected_rows_unparsed.split(CONFIG_DELIMITER):
                    selected_row_set.add(guid)
            return selected_row_set
        except RuntimeError as err:
            self.backend.report_exception(sender=tree_id, msg=f'Failed to load expanded rows from app_config', error=err)

    def _load_expanded_rows_from_config(self, tree_id: TreeID) -> Set[str]:
        """Loads the Set of expanded rows from config file"""
        logger.debug(f'[{tree_id}] Loading expanded rows from app_config')
        try:
            expanded_row_set: Set[str] = set()
            expanded_rows_str: str = self.backend.get_config(self._make_expanded_rows_config_key(tree_id), default_val='',
                                                             required=False)
            if expanded_rows_str:
                for guid in expanded_rows_str.split(CONFIG_DELIMITER):
                    expanded_row_set.add(guid)
            return expanded_row_set
        except RuntimeError:
            logger.exception(f'[{tree_id}] Failed to load expanded rows from app_config')

    def _save_selected_rows_to_config(self, display_tree_meta: ActiveDisplayTreeMeta):
        selected_rows_str: str = CONFIG_DELIMITER.join(str(guid) for guid in display_tree_meta.selected_row_set)
        self.backend.put_config(self._make_selected_rows_config_key(display_tree_meta.tree_id), selected_rows_str)

    def _save_expanded_rows_to_config(self, display_tree_meta: ActiveDisplayTreeMeta):
        expanded_rows_str: str = CONFIG_DELIMITER.join(str(uid) for uid in display_tree_meta.expanded_row_set)
        self.backend.put_config(self._make_expanded_rows_config_key(display_tree_meta.tree_id), expanded_rows_str)

    def _purge_dead_rows(self, expanded_cached: Set[GUID], selected_cached: Set[GUID], display_tree_meta: ActiveDisplayTreeMeta) -> RowsOfInterest:
        verified = RowsOfInterest()

        if not display_tree_meta.root_exists:
            verified.expanded = expanded_cached
            verified.selected = selected_cached
            return verified

        stopwatch = Stopwatch()

        processing_queue: Deque[SPIDNodePair] = deque()

        for sn in self.backend.get_child_list(parent_spid=display_tree_meta.state.root_sn.spid, tree_id=display_tree_meta.tree_id):
            processing_queue.append(sn)

        while len(processing_queue) > 0:
            sn: SPIDNodePair = processing_queue.popleft()
            guid = sn.spid.guid
            if guid in selected_cached:
                verified.selected.add(guid)

            if guid in expanded_cached:
                verified.expanded.add(guid)
                for sn in self.backend.get_child_list(parent_spid=sn.spid, tree_id=display_tree_meta.tree_id):
                    processing_queue.append(sn)

        logger.debug(f'[{display_tree_meta.tree_id}] {stopwatch} Verified {len(verified.expanded)} of {len(expanded_cached)} expanded rows '
                     f'and {len(verified.selected)} of {len(selected_cached)} selected')
        return verified
