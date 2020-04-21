import logging
from treelib import Tree
import os
from queue import Queue
from database import MetaDatabase
from gdrive.client import GDriveClient
from gdrive.model import GoogFolder, GoogFile, IntermediateMeta, Trashed
from fmeta.fmeta import FMetaTree, FMeta

logger = logging.getLogger(__name__)


def build_trees(meta: IntermediateMeta):
    trees = {}

    total_items = 0
    count_shared = 0
    count_explicit_trash = 0
    count_implicit_trash = 0
    count_no_md5 = 0

    for root_node in meta.roots:
        fmeta_tree = FMetaTree('/')
        assert trees.get(root_node.name, None) is None
        trees[root_node.name] = fmeta_tree

        count_tree_items = 0
        count_tree_files = 0
        count_tree_dirs = 0
        logger.debug(f'Building tree for GDrive root: [{root_node.id}] {root_node.name}')
        q = Queue()
        q.put((root_node, ''))

        while not q.empty():
            item, parent_path = q.get()
            path = os.path.join(parent_path, item.name)
            # if not item.is_dir():
            #     gmeta = GMeta(item) # TODO
            #     fmeta_tree.add(gmeta)
            # logger.debug(f'[{item.id}] {item.trash_status_str()} {path}/')
            count_tree_items += 1
            total_items += 1

            # Collect stats
            if not item.is_dir():
                if item.shared:
                    count_shared += 1
                if not item.md5:
                    count_no_md5 += 1

            if item.trashed == Trashed.EXPLICITLY_TRASHED.value:
                count_explicit_trash += 1
            elif item.trashed == Trashed.TRASHED.value:
                count_implicit_trash += 1

            child_list = meta.first_parent_dict.get(item.id, None)
            if item.is_dir():
                count_tree_dirs += 1
            else:
                count_tree_files += 1
                if child_list:
                    logger.error(f'Item is marked as a FILE but has children! [{root_node.id}] {root_node.name}')

            if child_list:
                for child in child_list:
                    q.put((child, path))

        logger.debug(f'Root "{root_node.name}" has {count_tree_items} nodes ({count_tree_files} files, {count_tree_dirs} dirs)')

    logger.debug(f'Finished with {total_items} items! Stats: shared={count_shared}, no_md5={count_no_md5}, '
                 f'trashed={count_explicit_trash}, also_trashed={count_implicit_trash}')


class GDriveTreeBuilder:
    def __init__(self, config, cache_path):
        self.config = config
        self.gdrive_client = GDriveClient(self.config)
        if cache_path:
            self.cache = MetaDatabase(cache_path)
        else:
            self.cache = None

    def build(self, invalidate_cache=False):
        self.gdrive_client.get_about()
        cache_has_data = self.cache.has_gdrive_dirs() or self.cache.has_gdrive_files()

        meta = IntermediateMeta()

        # Load data from either cache or Google:
        if self.cache and cache_has_data and not invalidate_cache:
            self.load_from_cache(meta)
        else:
            self.gdrive_client.download_directory_structure(meta)
            self.gdrive_client.download_all_file_meta(meta)

        # Save to cache if configured:
        if self.cache and (not cache_has_data or invalidate_cache):
            self.save_to_cache(meta=meta, overwrite=True)

        return meta
        # Finally, build the dir tree:
       # build_trees(meta)

    # TODO: filter by trashed status
    # TODO: filter by shared status
    # TODO: filter by different owner

    def save_to_cache(self, meta, overwrite):
        # Convert to tuples for insert into DB:
        dir_rows = []
        file_rows = []
        root_rows = []
        for parent_id, item_list in meta.first_parent_dict.items():
            for item in item_list:
                if item.is_dir():
                    dir_rows.append((item.id, item.name, parent_id, item.trashed.value))
                else:
                    file_rows.append(item.make_tuple(parent_id))

        for item in meta.roots:
            # Currently only parentless dirs are stored in this table. Parentless files are stored
            # with the other files
            root_rows.append((item.id, item.name, item.trashed.value))

        self.cache.insert_gdrive_dirs(root_rows, dir_rows, overwrite)

        self.cache.insert_gdrive_files(file_rows, overwrite)

        self.cache.insert_multiple_parent_mappings(meta.ids_with_multiple_parents, overwrite)

    def load_from_cache(self, meta):

        # DIRs:
        root_rows, dir_rows = self.cache.get_gdrive_dirs()

        for item_id, item_name, item_trashed in root_rows:
            meta.add_root(GoogFolder(item_id, item_name, trashed_status=int(item_trashed)))

        for item_id, item_name, parent_id, item_trashed in dir_rows:
            meta.add_to_parent_dict(parent_id, GoogFolder(item_id, item_name, trashed_status=int(item_trashed)))

        # FILES:
        file_rows = self.cache.get_gdrive_files()
        for item_id, item_name, parent_id, item_trashed, original_filename, version, head_revision_id, md5, shared, \
                created_ts, modified_ts, size_bytes_str, owner_id in file_rows:
            size_bytes = None if size_bytes_str is None else int(size_bytes_str)
            file_node = GoogFile(item_id=item_id, item_name=item_name, original_filename=original_filename,
                                 trashed_status=int(item_trashed), version=int(version),
                                 head_revision_id=head_revision_id, md5=md5, shared=shared,
                                 created_ts=int(created_ts), modified_ts=modified_ts, size_bytes=size_bytes,
                                 owner_id=owner_id)
            meta.add_to_parent_dict(parent_id, file_node)

        # MISC:
        meta.ids_with_multiple_parents = self.cache.get_multiple_parent_ids()
