from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple
import logging

from constants import GDRIVE_DOWNLOAD_STATE_GETTING_DIRS, GDRIVE_DOWNLOAD_STATE_GETTING_NON_DIRS, GDRIVE_DOWNLOAD_STATE_READY_TO_COMPILE, \
    MIME_TYPE_SHORTCUT
from index.sqlite.gdrive_db import CurrentDownload, GDriveDatabase
from index.uid.uid import UID
from model.node.gdrive_node import GDriveFile, GDriveFolder, GDriveNode
from model.gdrive_whole_tree import GDriveWholeTree

logger = logging.getLogger(__name__)


# ABSTRACT CLASS GDriveQueryObserver
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class GDriveQueryObserver(ABC):
    """Observer interface, to be implemented with various strategies for processing downloaded Google Drive query results"""

    def __init__(self):
        pass

    @abstractmethod
    def node_received(self, goog_node: GDriveNode, item):
        pass

    @abstractmethod
    def end_of_page(self, next_page_token: str):
        pass


# CLASS MetaCollector
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
class MetaCollector:
    def __init__(self):
        self.user_dict: Dict[str, Tuple[str, str, str, bool]] = {}
        self.mime_types: Dict[str, GDriveNode] = {}
        self.shortcuts: Dict[str, GDriveNode] = {}
        
    def _collect_user(self, user: Dict[str, Any]):
        user_id = user.get('permissionId', None)
        user_name = user.get('displayName', None)
        user_email = user.get('emailAddress', None)
        user_photo_link = user.get('photoLink', None)
        user_is_me = user.get('me', None)
        self.user_dict[user_id] = (user_name, user_email, user_photo_link, user_is_me)

    def process(self, goog_node: GDriveNode, item: Dict[str, Any]):
        # Collect users
        owners = item.get('owners', None)
        if owners:
            user = owners[0]
            self._collect_user(user)

        sharing_user = item.get('sharingUser', None)
        if sharing_user:
            logger.debug(f'Found sharingUser: "{sharing_user}" for goog_node: {goog_node}')
            self._collect_user(sharing_user)

        # Collect MIME types
        mime_type = item.get('mimeType', None)
        if mime_type:
            self.mime_types[mime_type] = goog_node

        # web_view_link = item.get('webViewLink', None)
        # if web_view_link:
        #     logger.debug(f'Found webViewLink: "{web_view_link}" for goog_node: {goog_node}')
        #
        # web_content_link = item.get('webContentLink', None)
        # if web_content_link:
        #     logger.debug(f'Found webContentLink: "{web_content_link}" for goog_node: {goog_node}')

        is_shortcut = mime_type == MIME_TYPE_SHORTCUT
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

    def summarize(self):
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f'Found {len(self.user_dict)} distinct users')
            for user_id, user in self.user_dict.items():
                logger.debug(f'Found user: id={user_id} name={user[0]} email={user[1]} is_me={user[3]}')

            logger.debug(f'Found {len(self.mime_types)} distinct MIME types')
            for mime_type, item in self.mime_types.items():
                logger.debug(f'MIME type: {mime_type} -> [{item.uid}] {item.name} {item.get_size_bytes()}')


# CLASS SimpleNodeCollector
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class SimpleNodeCollector(GDriveQueryObserver):
    """Just collects Google nodes in its internal list, to be retreived all at once when meta download is done"""

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


# CLASS FolderMetaPersister
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class FolderMetaPersister(GDriveQueryObserver):
    """Collect GDrive folder metas for mass insertion into database"""

    def __init__(self, tree: GDriveWholeTree, download: CurrentDownload, cache: GDriveDatabase):
        super().__init__()
        self.tree = tree
        assert download.current_state == GDRIVE_DOWNLOAD_STATE_GETTING_DIRS
        self.download: CurrentDownload = download
        self.cache: GDriveDatabase = cache

        self.meta_collector: MetaCollector = MetaCollector()
        self.folder_list: List[GDriveFolder] = []
        self.id_parent_mappings: List[Tuple] = []

    def node_received(self, goog_node: GDriveFolder, item):
        parent_google_ids = item.get('parents', [])
        self.tree.id_dict[goog_node.uid] = goog_node
        self.folder_list.append(goog_node)

        self.id_parent_mappings += parent_mappings_tuples(goog_node.uid, parent_google_ids, sync_ts=self.download.update_ts)

        self.meta_collector.process(goog_node, item)

    def end_of_page(self, next_page_token):
        self.download.page_token = next_page_token
        if not next_page_token:
            # done
            assert self.download.current_state == GDRIVE_DOWNLOAD_STATE_GETTING_DIRS
            self.download.current_state = GDRIVE_DOWNLOAD_STATE_GETTING_NON_DIRS

            if len(self.folder_list) > 0:
                self.meta_collector.summarize()
            # fall through

        # Insert all objects for the preceding page into the database:
        self.cache.insert_gdrive_folder_list_and_parents(folder_list=self.folder_list, parent_mappings=self.id_parent_mappings,
                                                         current_download=self.download)

        if next_page_token:
            # Clear the buffers for reuse:
            self.folder_list = []
            self.id_parent_mappings = []


# CLASS FileMetaPersister
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class FileMetaPersister(GDriveQueryObserver):
    """Collect GDrive file metas for mass insertion into database"""

    def __init__(self, tree: GDriveWholeTree, download: CurrentDownload, cache: GDriveDatabase):
        super().__init__()
        self.tree = tree
        assert download.current_state == GDRIVE_DOWNLOAD_STATE_GETTING_NON_DIRS
        self.download: CurrentDownload = download
        self.cache: GDriveDatabase = cache

        self.meta_collector: MetaCollector = MetaCollector()
        self.file_list: List[GDriveFile] = []
        self.id_parent_mappings: List[Tuple] = []

    def node_received(self, goog_node: GDriveFile, item):
        parent_google_ids = item.get('parents', [])
        self.tree.id_dict[goog_node.uid] = goog_node
        self.file_list.append(goog_node)

        self.id_parent_mappings += parent_mappings_tuples(goog_node.uid, parent_google_ids, sync_ts=self.download.update_ts)

        self.meta_collector.process(goog_node, item)

    def end_of_page(self, next_page_token):
        self.download.page_token = next_page_token
        if not next_page_token:
            # done
            assert self.download.current_state == GDRIVE_DOWNLOAD_STATE_GETTING_NON_DIRS
            self.download.current_state = GDRIVE_DOWNLOAD_STATE_READY_TO_COMPILE

            if len(self.file_list) > 0:
                self.meta_collector.summarize()
            # fall through

        # Insert all objects for the preceding page into the database:
        self.cache.insert_gdrive_files_and_parents(file_list=self.file_list, parent_mappings=self.id_parent_mappings,
                                                   current_download=self.download)

        if next_page_token:
            # Clear the buffers for reuse:
            self.file_list = []
            self.id_parent_mappings = []


def parent_mappings_tuples(item_uid: UID, parent_goog_ids: List[str], sync_ts: int) -> List[Tuple[UID, Optional[UID], str, int]]:
    tuples = []
    for parent_goog_id in parent_goog_ids:
        tuples.append((item_uid, None, parent_goog_id, sync_ts))
    return tuples
