import logging
import os
from queue import Queue
from gdrive.client import GDriveClient
from cache.sqlite.gdrive_db import GDriveDatabase
from model.gdrive import EXPLICITLY_TRASHED, GoogFolder, GoogFile, GDriveMeta, IMPLICITLY_TRASHED, NOT_TRASHED
from ui import actions

logger = logging.getLogger(__name__)


def build_trees(meta: GDriveMeta):
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
        logger.debug(f'Building tree for GDrive root: [{root_node.id}] {root_node.name}')
        q = Queue()
        q.put((root_node, '/'))

        while not q.empty():
            item, parent_path = q.get()
            path = os.path.join(parent_path, item.name)
            existing = path_dict.get(path, None)
            if existing:
                if existing.trashed == NOT_TRASHED and item.trashed != NOT_TRASHED:
                    count_resolved_conflicts += 1
                    continue
                elif item.trashed == NOT_TRASHED and existing.trashed != NOT_TRASHED:
                    count_resolved_conflicts += 1
                    # pass through
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
            if not item.is_dir():
                if item.shared:
                    count_shared += 1
                if not item.md5:
                    count_no_md5 += 1

            if item.trashed == EXPLICITLY_TRASHED:
                count_explicit_trash += 1
            elif item.trashed == IMPLICITLY_TRASHED:
                count_implicit_trash += 1

            child_list = meta.get_children(item.id)
            if item.is_dir():
                count_tree_dirs += 1
            else:
                count_tree_files += 1
                if child_list:
                    logger.error(f'Item is marked as a FILE but has children! [{root_node.id}] {root_node.name}')

            if child_list:
                for child in child_list:
                    q.put((child, path))

            # TODO: include multiple parent mappings!

        logger.debug(f'"{root_node.name}" contains {count_tree_items} nodes ({count_tree_files} files, {count_tree_dirs} dirs)')

    logger.info(f'Finished paths for {total_items} items under {len(meta.roots)} roots! Stats: shared_by_me={count_shared}, '
                f'no_md5={count_no_md5}, user_trashed={count_explicit_trash}, also_trashed={count_implicit_trash}, '
                f'path_conflicts={count_path_conflicts}, resolved={count_resolved_conflicts}')
    return path_dict


class GDriveTreeLoader:
    def __init__(self, config, cache_path, tree_id=None):
        self.config = config
        self.tree_id = tree_id
        if cache_path:
            self.cache = GDriveDatabase(cache_path)
        else:
            self.cache = None
        self.gdrive_client = GDriveClient(self.config, tree_id)

    def load_all(self, invalidate_cache=False):
        try:
            if self.tree_id:
                logger.debug(f'Sending START_PROGRESS_INDETERMINATE for tree_id: {self.tree_id}')
                actions.get_dispatcher().send(actions.START_PROGRESS_INDETERMINATE, sender=self.tree_id)

            meta = GDriveMeta()

            meta.me = self.gdrive_client.get_about()
            cache_has_data = self.cache.has_gdrive_dirs() or self.cache.has_gdrive_files()

            # Load data from either cache or Google:
            if self.cache and cache_has_data and not invalidate_cache:
                if self.tree_id:
                    msg = 'Reading cache...'
                    actions.get_dispatcher().send(actions.SET_PROGRESS_TEXT, sender=self.tree_id, msg=msg)

                self.load_from_cache(meta)
            else:
                self.gdrive_client.download_directory_structure(meta)
                self.gdrive_client.download_all_file_meta(meta)

            # Save to cache if configured:
            if self.cache and (not cache_has_data or invalidate_cache):
                if self.tree_id:
                    msg = 'Saving to cache...'
                    actions.get_dispatcher().send(actions.SET_PROGRESS_TEXT, sender=self.tree_id, msg=msg)
                self.save_to_cache(meta=meta, overwrite=True)

            # Finally, build the dir tree:
            #meta.path_dict = build_trees(meta)

            return meta
        finally:
            if self.tree_id:
                logger.debug(f'Sending STOP_PROGRESS for tree_id: {self.tree_id}')
                actions.get_dispatcher().send(actions.STOP_PROGRESS, sender=self.tree_id)

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

    def load_from_cache(self, meta):

        # DIRs:
        dir_rows = self.cache.get_gdrive_dirs()

        for item_id, item_name, parent_id, item_trashed, drive_id, my_share, sync_ts in dir_rows:
            meta.add_to_parent_dict(parent_id, GoogFolder(item_id=item_id, item_name=item_name,
                                                          trashed=item_trashed, drive_id=drive_id, my_share=my_share,
                                                          sync_ts=sync_ts))

        # FILES:
        file_rows = self.cache.get_gdrive_files()
        for item_id, item_name, parent_id, item_trashed, size_bytes_str, md5, create_ts, modify_ts, owner_id, drive_id, \
                my_share, version, head_revision_id, sync_ts in file_rows:
            size_bytes = None if size_bytes_str is None else int(size_bytes_str)
            file_node = GoogFile(item_id=item_id, item_name=item_name,
                                 trashed=item_trashed, drive_id=drive_id, my_share=my_share, version=int(version),
                                 head_revision_id=head_revision_id, md5=md5,
                                 create_ts=int(create_ts), modify_ts=int(modify_ts), size_bytes=size_bytes,
                                 owner_id=owner_id, sync_ts=sync_ts)
            meta.add_to_parent_dict(parent_id, file_node)

        # MISC:
        meta.ids_with_multiple_parents = self.cache.get_multiple_parent_ids()
