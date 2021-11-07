import logging
from typing import Dict, List, Optional, Tuple

from pydispatch import dispatcher

from backend.tree_store.gdrive.path_list_computer import GDrivePathListComputer
from constants import GDRIVE_ROOT_UID, SUPER_DEBUG_ENABLED, TreeID
from backend.tree_store.gdrive.gdrive_whole_tree import GDriveWholeTree
from error import CacheNotFoundError
from model.node.gdrive_node import GDriveFile, GDriveFolder, GDriveNode
from model.node_identifier_factory import NodeIdentifierFactory
from model.uid import UID
from backend.tree_store.gdrive.master_gdrive_memory import GDriveMemoryStore
from backend.tree_store.gdrive.master_gdrive_op_load import GDriveDiskLoadOp
from backend.tree_store.gdrive.master_gdrive_op_write import GDriveWriteThroughOp
from backend.sqlite.gdrive_db import CurrentDownload, GDriveDatabase
from signal_constants import Signal
from util.has_lifecycle import HasLifecycle
from util.stopwatch_sec import Stopwatch

logger = logging.getLogger(__name__)


class GDriveDiskStore(HasLifecycle):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS GDriveDiskStore
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, backend, memstore: GDriveMemoryStore, device_uid: UID):
        HasLifecycle.__init__(self)
        self.backend = backend
        self.device_uid: UID = device_uid
        self._memstore: GDriveMemoryStore = memstore
        self._db: Optional[GDriveDatabase] = None
        self.needs_full_reload: bool = False
        self._path_list_computer: GDrivePathListComputer = None

    def start(self):
        logger.debug(f'Starting GDriveDiskStore')
        HasLifecycle.start(self)
        gdrive_db_path = self._get_gdrive_cache_path()
        self._db = GDriveDatabase(gdrive_db_path, self.backend, self.device_uid)
        self._path_list_computer = GDrivePathListComputer(get_node_for_uid_func=self._db.get_node_with_uid)

    def shutdown(self):
        HasLifecycle.shutdown(self)
        if self._db:
            self._db.close()
            self._db = None

    def _get_gdrive_cache_path(self) -> str:
        master_tree_root = NodeIdentifierFactory.get_root_constant_gdrive_spid(self.device_uid)
        try:
            cache_info = self.backend.cacheman.get_cache_info_for_subtree(master_tree_root, create_if_not_found=False)
        except CacheNotFoundError:
            logger.warning(f'Failed to find CacheInfo for GDrive device_uid: {self.device_uid}. Triggering a rebuild of the cache from the network.')
            cache_info = self.backend.cacheman.get_cache_info_for_subtree(master_tree_root, create_if_not_found=True)
            self.needs_full_reload = True

        return cache_info.cache_location

    def load_tree_from_cache(self, is_complete: bool, tree_id: TreeID) -> GDriveWholeTree:
        """
        Retrieves and reassembles (to the extent it was during the download) a partially or completely downloaded
        GDrive tree.
        """
        logger.debug(f'[{tree_id}] Loading GDrive tree from disk cache...')
        sw_total = Stopwatch()
        max_uid = GDRIVE_ROOT_UID + 1
        tree = GDriveWholeTree(self.backend, self.device_uid)
        invalidate_uids: Dict[UID, str] = {}

        # DIRs:
        sw = Stopwatch()
        folder_list: List[GDriveFolder] = self._db.get_gdrive_folder_object_list()

        dispatcher.send(Signal.SET_PROGRESS_TEXT, sender=tree_id, msg=f'Retrieved {len(folder_list):n} Google Drive folders')

        count_folders_loaded = 0
        for folder in folder_list:
            if folder.goog_id:
                uid = self.backend.cacheman.get_uid_for_goog_id(folder.device_uid, folder.goog_id, folder.uid)
                if folder.uid != uid:
                    # Duplicate entry with same goog_id. Here's a useful SQLite query:
                    # "SELECT goog_id, COUNT(*) c FROM gdrive_file GROUP BY goog_id HAVING c > 1;"
                    logger.warning(f'Skipping what appears to be a duplicate entry: goog_id="{folder.goog_id}", uid={folder.uid}')
                    invalidate_uids[folder.uid] = folder.goog_id
                    continue
            elif folder.uid != GDRIVE_ROOT_UID:
                raise RuntimeError(f'GDriveFolder is missing goog_id: {folder}')

            if tree.uid_dict.get(folder.uid, None):
                # This should never happen since we use UIDs as primary keys in our SQLite DB.
                raise RuntimeError(f'GDrive folder cache conflict for UID: {folder.uid} (1st: {tree.uid_dict[folder.uid]}; 2nd: {folder}')
            tree.uid_dict[folder.uid] = folder
            count_folders_loaded += 1

            if folder.uid >= max_uid:
                max_uid = folder.uid

        logger.debug(f'{sw} Loaded {count_folders_loaded} Google Drive folders')

        # FILES:
        sw = Stopwatch()
        file_list: List[GDriveFile] = self._db.get_gdrive_file_object_list()

        dispatcher.send(Signal.SET_PROGRESS_TEXT, sender=tree_id, msg=f'Retreived {len(file_list):n} Google Drive files')

        count_files_loaded = 0
        for file in file_list:
            if file.goog_id:
                uid = self.backend.cacheman.get_uid_for_goog_id(file.device_uid, file.goog_id, file.uid)
                if file.uid != uid:
                    # Duplicate entry with same goog_id. Here's a useful SQLite query:
                    # "SELECT goog_id, COUNT(*) c FROM gdrive_file GROUP BY goog_id HAVING c > 1;"
                    logger.warning(f'Skipping what appears to be a duplicate entry: goog_id="{file.goog_id}", uid={file.uid}')
                    invalidate_uids[file.uid] = file.goog_id
                    continue
            else:
                raise RuntimeError(f'GDriveFile is missing goog_id: {file}')

            if tree.uid_dict.get(file.uid, None):
                raise RuntimeError(f'GDrive cache conflict for UID: {file.uid} (1st: {tree.uid_dict[file.uid]}; 2nd: {file}')
            tree.uid_dict[file.uid] = file
            count_files_loaded += 1

            if file.uid >= max_uid:
                max_uid = file.uid

        logger.debug(f'{sw} Loaded {count_files_loaded} Google Drive files')

        if is_complete:
            # CHILD-PARENT MAPPINGS:
            sw = Stopwatch()
            id_parent_mappings = self._db.get_id_parent_mappings()
            mapping_count = len(id_parent_mappings)

            for mapping in id_parent_mappings:
                item_uid = mapping[0]
                if invalidate_uids.get(item_uid, None):
                    logger.warning(f'Skipping parent mappings for uid={item_uid}')
                else:
                    parent_uid = mapping[1]
                    if parent_uid:
                        tree.add_parent_mapping(UID(item_uid), UID(parent_uid))

                        if parent_uid >= max_uid:
                            max_uid = parent_uid

            logger.debug(f'{sw} Loaded {mapping_count} Google Drive file-folder mappings')

        logger.debug(f'{sw_total} Loaded {len(tree.uid_dict):n} items from {count_files_loaded:n} files and {count_folders_loaded:n} folders')

        self.backend.uid_generator.ensure_next_uid_greater_than(max_uid)
        return tree

    def execute_load_op(self, operation: GDriveDiskLoadOp):
        operation.load_from_diskstore(self._db)
        # No need to commit since we only did reads

    def execute_write_op(self, operation: GDriveWriteThroughOp):
        operation.update_diskstore(self._db)
        self._db.commit()

    def create_or_update_download(self, download: CurrentDownload, commit: bool = True):
        self._db.upsert_download(download, commit)

    def update_changes_download_start_token(self, new_page_token, commit: bool = True):
        self._db.update_changes_download_start_token(new_page_token, commit)

    def get_current_download(self, download_type: int):
        assert download_type
        for download in self._db.get_current_download_list():
            if download.download_type == download_type:
                return download
        return None

    def get_single_node_with_uid(self, uid: UID) -> Optional[GDriveNode]:
        """Note: this could be an expensive operation, since we are recursively looking up the node's parents in order to fill in paths."""
        if SUPER_DEBUG_ENABLED:
            logger.debug(f'Entered get_single_node_with_uid(): uid={uid}')
        return self._path_list_computer.recompute_path_list_for_uid(uid=uid)

    def insert_gdrive_files_and_parents(self, file_list: List[GDriveFile], parent_mappings: List[Tuple],
                                        current_download: CurrentDownload, commit: bool = True):
        self._db.insert_gdrive_files(file_list=file_list, commit=False)
        self._db.insert_id_parent_mappings(parent_mappings, commit=False)
        self._db.upsert_download(current_download, commit=commit)

    def insert_gdrive_folder_list_and_parents(self, folder_list: List[GDriveFolder], parent_mappings: List[Tuple],
                                              current_download: CurrentDownload, commit: bool = True):
        self._db.insert_gdrive_folder_list(folder_list=folder_list, commit=False)
        self._db.insert_id_parent_mappings(parent_mappings, commit=False)
        self._db.upsert_download(current_download, commit=commit)

    def insert_gdrive_folder_list(self, folder_list: List[GDriveFolder], overwrite=False, commit=True):
        self._db.insert_gdrive_folder_list(folder_list, overwrite, commit)

    def update_folder_fetched_status(self, commit=True):
        self._db.update_folder_fetched_status(commit)

    def get_id_parent_mappings(self) -> List[Tuple]:
        return self._db.get_id_parent_mappings()

    def insert_id_parent_mappings(self, id_parent_mappings: List[Tuple], overwrite=False, commit=True):
        self._db.insert_id_parent_mappings(id_parent_mappings, overwrite, commit)

    def delete_nodes(self, file_uid_list: List[UID], folder_uid_list: List[UID]):
        self._db.delete_nodes(file_uid_list, folder_uid_list)

    def get_child_list_for_parent_uid(self, node_uid: UID) -> List[GDriveNode]:
        return self._db.get_child_list_for_parent_uid(node_uid)
