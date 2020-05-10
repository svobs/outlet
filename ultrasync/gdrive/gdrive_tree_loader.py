import logging
import os
import time
import uuid
from queue import Queue

from constants import EXPLICITLY_TRASHED, IMPLICITLY_TRASHED, ROOT
from gdrive.client import GDriveClient
from index.sqlite.gdrive_db import GDriveDatabase
from model.gdrive_tree import GDriveWholeTree
from model.goog_node import GoogFile, GoogFolder
from ui import actions

logger = logging.getLogger(__name__)


#  Static functions
# ⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟

def build_path_trees_by_id(meta: GDriveWholeTree):
    # TODO DEAD CODE prob delete later
    path_dict = {}

    total_items = 0
    count_shared = 0
    count_explicit_trash = 0
    count_implicit_trash = 0
    count_no_md5 = 0
    count_path_conflicts = 0
    count_resolved_conflicts = 0

    for root_node in meta.roots:

        count_tree_items = 0
        count_tree_files = 0
        count_tree_dirs = 0
        logger.debug(f'Building tree for GDrive root: [{root_node.uid}] {root_node.name}')
        q = Queue()
        q.put((root_node, root_node.uid))

        while not q.empty():
            item, parent_id = q.get()
            path = os.path.join(parent_id, item.uid)
            existing = path_dict.get(path, None)
            if existing:
                if item.uid == existing.uid:
                    # dunno why these come through, but they seem to be harmless
                    count_resolved_conflicts += 1
                    continue
                else:
                    logger.error(f'Overwriting path "{path}":\n'
                                 f'OLD: {existing}\n'
                                 f'NEW: {item}')
                    count_path_conflicts += 1
            path_dict[path] = item
            # logger.debug(f'path="{path}" {item}')
            count_tree_items += 1
            total_items += 1

            # Collect stats
            if item.my_share:
                count_shared += 1
            if not item.is_dir() and not item.md5:
                count_no_md5 += 1

            if item.trashed == EXPLICITLY_TRASHED:
                count_explicit_trash += 1
            elif item.trashed == IMPLICITLY_TRASHED:
                count_implicit_trash += 1

            child_list = meta.get_children(item.uid)
            if item.is_dir():
                count_tree_dirs += 1
            else:
                count_tree_files += 1
                if child_list:
                    logger.error(f'Item is marked as a FILE but has children! [{root_node.uid}] {root_node.name}')

            if child_list:
                for child in child_list:
                    q.put((child, path))

        logger.debug(f'"{root_node.name}" contains {count_tree_items} nodes ({count_tree_files} files, {count_tree_dirs} dirs)')

    logger.info(f'Finished paths for {total_items} items under {len(meta.roots)} roots! Stats: shared_by_me={count_shared}, '
                f'no_md5={count_no_md5}, user_trashed={count_explicit_trash}, also_trashed={count_implicit_trash}, '
                f'path_conflicts={count_path_conflicts}, resolved={count_resolved_conflicts}')
    return path_dict


"""
▛▝▝▝▝▝▝▝▝▝▝▝▝▝▝▝▝▝▝▝▝▝▝▝▝▝▝▝▝▝▝▝▝▝ ▜
          Class GDriveTreeLoader
▙ ▖▖▖▖▖▖▖▖▖▖▖▖▖▖▖▖▖▖▖▖▖▖▖▖▖▖▖▖▖▖▖▖▖▟
"""


class GDriveTreeLoader:
    def __init__(self, config, cache_path, tree_id=None):
        self.config = config
        self.tree_id = tree_id
        self.tx_id = uuid.uuid1()
        self.cache_path = cache_path
        if cache_path:
            if not os.path.exists(cache_path):
                raise FileNotFoundError(cache_path)
            self.cache = GDriveDatabase(cache_path)
        else:
            self.cache = None
        self.gdrive_client = GDriveClient(self.config, tree_id)

    def load_all(self, invalidate_cache=False) -> GDriveWholeTree:
        try:
            if self.tree_id:
                logger.debug(f'Sending START_PROGRESS_INDETERMINATE for tree_id: {self.tree_id}')
                actions.get_dispatcher().send(actions.START_PROGRESS_INDETERMINATE, sender=self.tree_id, tx_id=self.tx_id)

            meta = GDriveWholeTree()

            meta.me = self.gdrive_client.get_about()
            cache_has_data = self.cache.has_gdrive_dirs() or self.cache.has_gdrive_files()

            # Load data from either cache or Google:
            if self.cache and cache_has_data and not invalidate_cache:
                if self.tree_id:
                    msg = 'Reading cache...'
                    actions.get_dispatcher().send(actions.SET_PROGRESS_TEXT, sender=self.tree_id, tx_id=self.tx_id, msg=msg)

                self.load_from_cache(meta)
            else:
                sync_ts = int(time.time())
                self.gdrive_client.download_directory_structure(meta, sync_ts)
                self.gdrive_client.download_all_file_meta(meta, sync_ts)
                for goog_dir in meta.first_parent_dict.values():
                    goog_dir.all_children_fetched = True

            # Save to cache if configured:
            if self.cache and (not cache_has_data or invalidate_cache):
                if self.tree_id:
                    msg = 'Saving to cache...'
                    actions.get_dispatcher().send(actions.SET_PROGRESS_TEXT, sender=self.tree_id, tx_id=self.tx_id, msg=msg)
                logger.debug('Saving to cache')
                self.save_to_cache(meta=meta, overwrite=True)

            return meta
        finally:
            if self.tree_id:
                logger.debug(f'Sending STOP_PROGRESS for tree_id: {self.tree_id}')
                actions.get_dispatcher().send(actions.STOP_PROGRESS, sender=self.tree_id, tx_id=self.tx_id)

    def save_to_cache(self, meta, overwrite):
        # Convert to tuples for insert into DB:
        dir_rows = []
        file_rows = []
        for parent_id, item_list in meta.first_parent_dict.items():
            for item in item_list:
                if item.is_dir():
                    dir_rows.append(item.make_tuple(parent_id))
                else:
                    file_rows.append(item.make_tuple(parent_id))

        for item in meta.roots:
            # Roots are stored with the other files and dirs:
            if item.is_dir():
                dir_rows.append(item.make_tuple(None))
            else:
                file_rows.append(item.make_tuple(None))

        self.cache.insert_gdrive_dirs(dir_rows, overwrite)

        self.cache.insert_gdrive_files(file_rows, overwrite)

        self.cache.insert_multiple_parent_mappings(meta.ids_with_multiple_parents, overwrite)

    def load_from_cache(self, meta: GDriveWholeTree):

        if not self.cache.has_gdrive_dirs() or not self.cache.has_gdrive_files():
            raise RuntimeError(f'Cache is corrupted: {self.cache_path}')

        # DIRs:
        dir_rows = self.cache.get_gdrive_dirs()
        actions.get_dispatcher().send(actions.SET_PROGRESS_TEXT, sender=self.tree_id, tx_id=self.tx_id, msg=f'Retreived {len(dir_rows):n} dirs')

        for item_id, item_name, parent_id, item_trashed, drive_id, my_share, sync_ts, all_children_fetched in dir_rows:
            item = GoogFolder(item_id=item_id, item_name=item_name,
                              trashed=item_trashed, drive_id=drive_id, my_share=my_share,
                              sync_ts=sync_ts, all_children_fetched=all_children_fetched)
            item.parent_ids = parent_id
            meta.add_item(item)

        # FILES:
        file_rows = self.cache.get_gdrive_files()
        actions.get_dispatcher().send(actions.SET_PROGRESS_TEXT, sender=self.tree_id, tx_id=self.tx_id, msg=f'Retreived {len(file_rows):n} files')
        for item_id, item_name, parent_id, item_trashed, size_bytes_str, md5, create_ts, modify_ts, owner_id, drive_id, \
                my_share, version, head_revision_id, sync_ts in file_rows:
            size_bytes = None if size_bytes_str is None else int(size_bytes_str)
            file_node = GoogFile(item_id=item_id, item_name=item_name,
                                 trashed=item_trashed, drive_id=drive_id, my_share=my_share, version=int(version),
                                 head_revision_id=head_revision_id, md5=md5,
                                 create_ts=int(create_ts), modify_ts=int(modify_ts), size_bytes=size_bytes,
                                 owner_id=owner_id, sync_ts=sync_ts)
            file_node.parent_ids = parent_id
            meta.add_item(file_node)

        # MISC:
        # meta.ids_with_multiple_parents = self.cache.get_multiple_parent_ids()

        # Finally, build the id tree:
        # meta.path_dict = build_path_trees_by_id(meta)

