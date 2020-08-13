from abc import ABC, abstractmethod
from typing import List, Optional, Tuple

from constants import GDRIVE_DOWNLOAD_STATE_GETTING_DIRS, GDRIVE_DOWNLOAD_STATE_GETTING_NON_DIRS, GDRIVE_DOWNLOAD_STATE_READY_TO_COMPILE
from index.sqlite.gdrive_db import CurrentDownload, GDriveDatabase
from index.uid.uid import UID
from model.node.gdrive_node import GDriveNode
from model.gdrive_whole_tree import GDriveWholeTree


# ABSTRACT CLASS MetaObserver
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class MetaObserver(ABC):
    """Observer interface, to be implemented with various strategies for processing downloaded Google Drive meta"""

    def __init__(self):
        pass

    @abstractmethod
    def meta_received(self, goog_node: GDriveNode, item):
        pass

    @abstractmethod
    def end_of_page(self, next_page_token: str):
        pass


# CLASS SimpleNodeCollector
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class SimpleNodeCollector(MetaObserver):
    """Just collects Google nodes in its internal list, to be retreived all at once when meta download is done"""

    def __init__(self):
        super().__init__()
        self.nodes: List[GDriveNode] = []
        self.raw_items = []

    def meta_received(self, goog_node: GDriveNode, item):
        self.nodes.append(goog_node)
        self.raw_items.append(item)

    def end_of_page(self, next_page_token: str):
        pass

    def __repr__(self):
        return f'SimpleNodeCollector(nodes={len(self.nodes)} raw_items={len(self.raw_items)}'


# CLASS FolderMetaPersister
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class FolderMetaPersister(MetaObserver):
    """Collect GDrive folder metas for mass insertion into database"""
    def __init__(self, tree: GDriveWholeTree, download: CurrentDownload, cache: GDriveDatabase):
        super().__init__()
        self.tree = tree
        assert download.current_state == GDRIVE_DOWNLOAD_STATE_GETTING_DIRS
        self.download: CurrentDownload = download
        self.cache: GDriveDatabase = cache
        self.dir_tuples: List[Tuple] = []
        self.id_parent_mappings: List[Tuple] = []

    def meta_received(self, goog_node, item):
        parent_google_ids = item.get('parents', [])
        self.tree.id_dict[goog_node.uid] = goog_node
        self.dir_tuples.append(goog_node.to_tuple())

        self.id_parent_mappings += parent_mappings_tuples(goog_node.uid, parent_google_ids, sync_ts=self.download.update_ts)

    def end_of_page(self, next_page_token):
        self.download.page_token = next_page_token
        if not next_page_token:
            # done
            assert self.download.current_state == GDRIVE_DOWNLOAD_STATE_GETTING_DIRS
            self.download.current_state = GDRIVE_DOWNLOAD_STATE_GETTING_NON_DIRS
            # fall through

        self.cache.insert_gdrive_folders_and_parents(dir_list=self.dir_tuples, parent_mappings=self.id_parent_mappings, current_download=self.download)

        if next_page_token:
            # Clear the buffers for reuse:
            self.dir_tuples = []
            self.id_parent_mappings = []


# CLASS FileMetaPersister
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class FileMetaPersister(MetaObserver):
    """Collect GDrive file metas for mass insertion into database"""
    def __init__(self, tree: GDriveWholeTree, download: CurrentDownload, cache: GDriveDatabase):
        super().__init__()
        self.tree = tree
        assert download.current_state == GDRIVE_DOWNLOAD_STATE_GETTING_NON_DIRS
        self.download: CurrentDownload = download
        self.cache: GDriveDatabase = cache
        self.file_tuples: List[Tuple] = []
        self.id_parent_mappings: List[Tuple] = []

    def meta_received(self, goog_node, item):
        parent_google_ids = item.get('parents', [])
        self.tree.id_dict[goog_node.uid] = goog_node
        self.file_tuples.append(goog_node.to_tuple())

        self.id_parent_mappings += parent_mappings_tuples(goog_node.uid, parent_google_ids, sync_ts=self.download.update_ts)

    def end_of_page(self, next_page_token):
        self.download.page_token = next_page_token
        if not next_page_token:
            # done
            assert self.download.current_state == GDRIVE_DOWNLOAD_STATE_GETTING_NON_DIRS
            self.download.current_state = GDRIVE_DOWNLOAD_STATE_READY_TO_COMPILE
            # fall through

        self.cache.insert_gdrive_files_and_parents(file_list=self.file_tuples, parent_mappings=self.id_parent_mappings,
                                                   current_download=self.download)

        if next_page_token:
            # Clear the buffers for reuse:
            self.file_tuples = []
            self.id_parent_mappings = []


def parent_mappings_tuples(item_uid: UID, parent_goog_ids: List[str], sync_ts: int) -> List[Tuple[UID, Optional[UID], str, int]]:
    tuples = []
    for parent_goog_id in parent_goog_ids:
        tuples.append((item_uid, None, parent_goog_id, sync_ts))
    return tuples
