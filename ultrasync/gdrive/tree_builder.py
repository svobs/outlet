import logging
from treelib import Tree
import os
from queue import Queue
from database import MetaDatabase
from gdrive.client import GDriveClient
from gdrive.model import DirNode, FileNode, IntermediateMeta

logger = logging.getLogger(__name__)


def build_dir_trees(meta: IntermediateMeta):
    rows = []

    total = 0

    names = []
    for root in meta.roots:
        names.append(root.name)
    logger.debug(f'Root nodes: {names}')

    for root_node in meta.roots:
        tree_size = 0
        logger.debug(f'Building tree for GDrive root: [{root_node.id}] {root_node.name}')
        q = Queue()
        q.put((root_node, ''))

        while not q.empty():
            item, parent_path = q.get()
            path = os.path.join(parent_path, item.name)
            rows.append((item, path))
            # logger.debug(f'[{item.id}] {item.trash_status_str()} {path}/')
            tree_size += 1
            total += 1

            child_list = meta.first_parent_dict.get(item.id, None)
            if child_list:
                for child in child_list:
                    q.put((child, path))

        logger.debug(f'Root "{root_node.name}" has {tree_size} nodes')

    logger.debug(f'Finished with {total} items!')


class GDriveTreeBuilder:
    def __init__(self, config, cache_path, invalidate_cache=False):
        self.config = config
        self.gdrive_client = GDriveClient(self.config)
        self.invalidate_cache = invalidate_cache
        if cache_path:
            self.cache = MetaDatabase(cache_path)
        else:
            self.cache = None

    def build(self):
        # self.gdrive_client.get_about()
        # cache_has_data = self.cache.has_gdrive_dirs()
        #
        # # Load data from either cache or Google:
        # if self.cache and cache_has_data and not self.invalidate_cache:
        #     meta = self.load_dirs_from_cache()
        # else:
        #     meta = self.gdrive_client.download_directory_structure()
        #
        # # Save to cache if configured:
        # if self.cache and (not cache_has_data or self.invalidate_cache):
        #     self.save_dirs_to_cache(meta=meta, overwrite=True)
        #
        # # Finally, build the dir tree:
        # build_dir_trees(meta)

        meta = self.gdrive_client.download_all_file_meta()
        self.save_files_to_cache(meta=meta, overwrite=self.invalidate_cache)

    def save_files_to_cache(self, meta, overwrite):
        # Convert to tuples for insert into DB:
        file_rows = []
        for parent_id, item_list in meta.first_parent_dict.items():
            for item in item_list:
                if parent_id:
                    file_rows.append(item.make_tuple(parent_id))
                else:
                    raise RuntimeError(f'Found root in first_parent_dict: {item}')

        for item in meta.roots:
            logger.error(f'Found root in files: [{item.id}] name="{item.name}" version={item.version} {item.trashed.name}')
            # Insert anyway. Figure out later
            file_rows.append(item.make_tuple(None))

        self.cache.insert_gdrive_files(file_rows, meta.ids_with_multiple_parents, overwrite)

        return meta

    def load_files_from_cache(self):
        file_rows, ids_with_multiple_parents = self.cache.get_gdrive_files()

        meta = IntermediateMeta()

        for item_id, item_name, parent_id, item_trashed, original_filename, version, head_revision_id, md5, shared, created_ts, \
                modified_ts, size_bytes_str, owner_id in file_rows:
            size_bytes = None if size_bytes_str is None else int(size_bytes_str)
            file_node = FileNode(item_id=item_id, item_name=item_name, original_filename=original_filename,
                                 trashed_status=int(item_trashed), version=int(version),
                                 head_revision_id=head_revision_id, md5=md5, shared=shared,
                                 created_ts=int(created_ts), modified_ts=modified_ts, size_bytes=size_bytes,
                                 owner_id=owner_id)
            meta.add_to_parent_dict(parent_id, file_node)

        meta.ids_with_multiple_parents = ids_with_multiple_parents

        return meta

    def save_dirs_to_cache(self, meta, overwrite):
        # Convert to tuples for insert into DB:
        root_rows = []
        dir_rows = []
        for parent_id, item_list in meta.first_parent_dict.items():
            for item in item_list:
                if parent_id:
                    dir_rows.append((item.id, item.name, parent_id, item.trashed.value))
                else:
                    raise RuntimeError(f'Found root in first_parent_dict: {item}')

        for root in meta.roots:
            root_rows.append((root.id, root.name, root.trashed.value))

        self.cache.insert_gdrive_dirs(root_rows, dir_rows, meta.ids_with_multiple_parents, overwrite)

        return meta

    def load_dirs_from_cache(self):
        root_rows, dir_rows, ids_with_multiple_parents = self.cache.get_gdrive_dirs()

        meta = IntermediateMeta()

        for item_id, item_name, item_trashed in root_rows:
            meta.add_root(DirNode(item_id, item_name, trashed_status=int(item_trashed)))

        for item_id, item_name, parent_id, item_trashed in dir_rows:
            meta.add_to_parent_dict(parent_id, DirNode(item_id, item_name, trashed_status=int(item_trashed)))

        meta.ids_with_multiple_parents = ids_with_multiple_parents

        return meta
