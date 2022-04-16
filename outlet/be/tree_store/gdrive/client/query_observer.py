import time
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple
import logging

from constants import GDRIVE_DOWNLOAD_STATE_GETTING_DIRS, GDRIVE_DOWNLOAD_STATE_GETTING_NON_DIRS, GDRIVE_DOWNLOAD_STATE_READY_TO_COMPILE, \
    GDRIVE_ROOT_UID, MIME_TYPE_SHORTCUT
from be.tree_store.gdrive.gd_diskstore import GDriveDiskStore
from be.sqlite.gdrive_db import GDriveMetaDownload
from model.uid import UID
from model.node.gdrive_node import GDriveFile, GDriveFolder, GDriveNode
from be.tree_store.gdrive.gd_tree import GDriveWholeTree

logger = logging.getLogger(__name__)


class GDriveQueryObserver(ABC):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    ABSTRACT CLASS GDriveQueryObserver

    Observer interface, to be implemented with various strategies for processing downloaded Google Drive query results
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self):
        pass

    @abstractmethod
    def node_received(self, goog_node: GDriveNode, item):
        pass

    @abstractmethod
    def end_of_page(self, next_page_token: str):
        pass


class MetaCollector:
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS MetaCollector
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, cacheman):
        self.cacheman = cacheman

        self.shortcuts: Dict[str, GDriveNode] = {}
        """Dict of [goog_id -> node]"""

    def process(self, goog_node: GDriveNode, item: Dict[str, Any]):
        mime_type_string = item.get('mimeType', None)

        is_shortcut = mime_type_string == MIME_TYPE_SHORTCUT
        if is_shortcut:
            shortcut_details = item.get('shortcutDetails', None)
            if not shortcut_details:
                logger.error(f'Shortcut is missing shortcutDetails: id="{goog_node.uid}" name="{goog_node.name}"')
            else:
                target_id = shortcut_details.get('targetId')
                if not target_id:
                    logger.error(f'Shortcut is missing targetId: id="{goog_node.uid}" name="{goog_node.name}"')
                else:
                    logger.debug(f'Found shortcut: id="{goog_node.uid}" name="{goog_node.name}" -> target_id="{target_id}"')
                    self.shortcuts[goog_node.goog_id] = target_id


class SimpleNodeCollector(GDriveQueryObserver):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS SimpleNodeCollector

    Just collects Google nodes in its internal list, to be retreived all at once when meta download is done
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self):
        super().__init__()
        self.nodes: List[GDriveNode] = []
        self.raw_items = []

    def node_received(self, goog_node: GDriveNode, item):
        self.nodes.append(goog_node)
        self.raw_items.append(item)

    def end_of_page(self, next_page_token: str):
        pass

    def __repr__(self):
        return f'SimpleNodeCollector(nodes={len(self.nodes)} raw_items={len(self.raw_items)}'


class FolderMetaPersister(GDriveQueryObserver):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS FolderMetaPersister

    Collects GDrive folder metas for mass insertion into database
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, tree: GDriveWholeTree, download: GDriveMetaDownload, diskstore: GDriveDiskStore, cacheman):
        super().__init__()
        self.tree = tree
        assert download.current_state == GDRIVE_DOWNLOAD_STATE_GETTING_DIRS
        self.download: GDriveMetaDownload = download
        self.diskstore: GDriveDiskStore = diskstore

        self.meta_collector: MetaCollector = MetaCollector(cacheman)
        self.folder_list: List[GDriveFolder] = []
        self.id_parent_mappings: List[Tuple] = []

    def node_received(self, goog_node: GDriveFolder, item):
        parent_google_ids = item.get('parents', [])
        self.tree.uid_dict[goog_node.uid] = goog_node
        self.folder_list.append(goog_node)

        self.id_parent_mappings += parent_mappings_tuples(goog_node.uid, parent_google_ids, sync_ts=self.download.update_ts)

        self.meta_collector.process(goog_node, item)

    def end_of_page(self, next_page_token):
        self.download.page_token = next_page_token
        if not next_page_token:
            # done
            assert self.download.current_state == GDRIVE_DOWNLOAD_STATE_GETTING_DIRS
            self.download.current_state = GDRIVE_DOWNLOAD_STATE_GETTING_NON_DIRS

        # Insert all objects for the preceding page into the database:
        self.diskstore.insert_gdrive_folder_list_and_parents(folder_list=self.folder_list, parent_mappings=self.id_parent_mappings,
                                                             current_download=self.download, commit=True)

        if next_page_token:
            # Clear the buffers for reuse:
            self.folder_list = []
            self.id_parent_mappings = []

        # yield to other threads
        time.sleep(0)


class FileMetaPersister(GDriveQueryObserver):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS FileMetaPersister

    Collects GDrive file metas for mass insertion into database
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, tree: GDriveWholeTree, download: GDriveMetaDownload, diskstore: GDriveDiskStore, cacheman):
        super().__init__()
        self.tree = tree
        assert download.current_state == GDRIVE_DOWNLOAD_STATE_GETTING_NON_DIRS
        self.download: GDriveMetaDownload = download
        self.diskstore: GDriveDiskStore = diskstore

        self.meta_collector: MetaCollector = MetaCollector(cacheman)
        self.file_list: List[GDriveFile] = []
        self.id_parent_mappings: List[Tuple] = []

    def node_received(self, goog_node: GDriveFile, item):
        parent_google_ids = item.get('parents', [])
        self.tree.uid_dict[goog_node.uid] = goog_node
        self.file_list.append(goog_node)

        self.id_parent_mappings += parent_mappings_tuples(goog_node.uid, parent_google_ids, sync_ts=self.download.update_ts)

        self.meta_collector.process(goog_node, item)

    def end_of_page(self, next_page_token):
        self.download.page_token = next_page_token
        if not next_page_token:
            # done
            assert self.download.current_state == GDRIVE_DOWNLOAD_STATE_GETTING_NON_DIRS
            self.download.current_state = GDRIVE_DOWNLOAD_STATE_READY_TO_COMPILE

        # Insert all objects for the preceding page into the database:
        self.diskstore.insert_gdrive_files_and_parents(file_list=self.file_list, parent_mappings=self.id_parent_mappings,
                                                       current_download=self.download)

        if next_page_token:
            # Clear the buffers for reuse:
            self.file_list = []
            self.id_parent_mappings = []

        # yield to other threads
        time.sleep(0)


def parent_mappings_tuples(item_uid: UID, parent_goog_ids: List[str], sync_ts: int) -> List[Tuple[UID, Optional[UID], str, int]]:
    tuples = []
    if parent_goog_ids:
        for parent_goog_id in parent_goog_ids:
            tuples.append((item_uid, None, parent_goog_id, sync_ts))
    else:
        # If no parent, set to GDrive root
        tuples.append((item_uid, GDRIVE_ROOT_UID, None, sync_ts))
    return tuples
