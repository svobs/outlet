import logging
from typing import Dict, Iterable, List, Optional

import be.agent.grpc.generated.Node_pb2
from be.agent.grpc.generated.Outlet_pb2 import SignalMsg, TreeContextMenuItem
from constants import ErrorHandlingStrategy, IconId, MenuItemType, NodeIdentifierType, TreeLoadState, TreeType
from logging_constants import TRACE_ENABLED
from model.context_menu import ContextMenuItem
from model.device import Device
from model.disp_tree.display_tree import DisplayTree, DisplayTreeUiState
from model.disp_tree.filter_criteria import FilterCriteria, Ternary
from model.disp_tree.tree_action import TreeAction
from model.node.container_node import CategoryNode, ContainerNode, RootTypeNode
from model.node.dir_stats import DirStats
from model.node.gdrive_node import GDriveFile, GDriveFolder
from model.node.local_disk_node import LocalDirNode, LocalFileNode
from model.node.node import TNode, NonexistentDirNode, SPIDNodePair
from model.node_identifier import ChangeTreeSPID, GDriveIdentifier, GUID, LocalNodeIdentifier, NodeIdentifier, SinglePathNodeIdentifier
from model.uid import UID
from outlet.be.agent.grpc.generated import Outlet_pb2
from signal_constants import Signal

logger = logging.getLogger(__name__)


class GRPCConverter:
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS GRPCConverter

    Converts Swift objects to and from GRPC messages.
    Note on ordering of methods: TO comes before FROM
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """

    def __init__(self, outlet_backend):
        self.backend = outlet_backend

    # TNode
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def node_to_grpc(self, node: TNode, grpc_node: be.agent.grpc.generated.Node_pb2.TNode):
        assert isinstance(node, TNode), f'Not a TNode: {node}'

        self.node_identifier_to_grpc(node.node_identifier, grpc_node.node_identifier)

        # TNode common fields:
        grpc_node.trashed = node.get_trashed_status()
        grpc_node.is_shared = node.is_shared
        grpc_node.icon_id = node.get_icon().value

        if TRACE_ENABLED:
            logger.debug(f'Serializing node: {node}')

        if isinstance(node, NonexistentDirNode):
            grpc_node.nonexistent_dir_meta.name = node.name
        elif isinstance(node, ContainerNode):
            # ContainerNode or subclass
            if isinstance(node, CategoryNode):
                self.dir_stats_to_grpc(node.dir_stats, grpc_node.category_meta)
            elif isinstance(node, RootTypeNode):
                self.dir_stats_to_grpc(node.dir_stats, grpc_node.root_type_meta)
            else:
                # plain ContainerNode
                self.dir_stats_to_grpc(node.dir_stats, grpc_node.container_meta)
        elif node.tree_type == TreeType.LOCAL_DISK:
            if node.is_dir():
                assert isinstance(node, LocalDirNode)
                self.dir_stats_to_grpc(node.dir_stats, grpc_node.local_dir_meta)
                grpc_node.local_dir_meta.is_live = node.is_live()
                grpc_node.local_dir_meta.parent_uid = node.get_single_parent_uid()
                grpc_node.local_dir_meta.all_children_fetched = node.all_children_fetched
                if node.sync_ts:
                    grpc_node.local_dir_meta.sync_ts = node.sync_ts
                if node.create_ts:
                    grpc_node.local_dir_meta.create_ts = node.create_ts
                if node.modify_ts:
                    grpc_node.local_dir_meta.modify_ts = node.modify_ts
                if node.change_ts:
                    grpc_node.local_dir_meta.change_ts = node.change_ts
            else:
                if node.get_size_bytes():
                    grpc_node.local_file_meta.size_bytes = node.get_size_bytes()
                grpc_node.local_file_meta.is_live = node.is_live()
                if node.sync_ts:
                    grpc_node.local_file_meta.sync_ts = node.sync_ts
                if node.create_ts:
                    grpc_node.local_file_meta.create_ts = node.create_ts
                if node.modify_ts:
                    grpc_node.local_file_meta.modify_ts = node.modify_ts
                if node.change_ts:
                    grpc_node.local_file_meta.change_ts = node.change_ts
                if node.md5:
                    grpc_node.local_file_meta.md5 = node.md5
                if node.sha256:
                    grpc_node.local_file_meta.sha256 = node.sha256
                grpc_node.local_file_meta.parent_uid = node.get_single_parent_uid()
        elif node.tree_type == TreeType.GDRIVE:
            # GDriveNode
            if node.is_dir():
                assert isinstance(node, GDriveFolder)
                self.dir_stats_to_grpc(node.dir_stats, grpc_node.gdrive_folder_meta)
                grpc_node.gdrive_folder_meta.all_children_fetched = node.all_children_fetched
                meta = grpc_node.gdrive_folder_meta
            else:
                assert isinstance(node, GDriveFile)
                if node.md5:
                    grpc_node.gdrive_file_meta.md5 = node.md5
                if node.version:
                    grpc_node.gdrive_file_meta.version = node.version
                if node.get_size_bytes():
                    grpc_node.gdrive_file_meta.size_bytes = node.get_size_bytes()
                if node.mime_type_uid:
                    grpc_node.gdrive_file_meta.mime_type_uid = node.mime_type_uid
                meta = grpc_node.gdrive_file_meta

            # GDriveNode common fields:
            if node.goog_id:
                meta.goog_id = node.goog_id
            if node.name:
                meta.name = node.name
            if node.owner_uid:
                meta.owner_uid = node.owner_uid
            if node.shared_by_user_uid:
                meta.shared_by_user_uid = node.shared_by_user_uid
            if node.drive_id:
                meta.drive_id = node.drive_id
            for parent_uid in node.get_parent_uids():
                meta.parent_uid_list.append(parent_uid)
            if node.sync_ts:
                meta.sync_ts = node.sync_ts
            if node.modify_ts:
                meta.modify_ts = node.modify_ts
            if node.create_ts:
                meta.create_ts = node.create_ts
        return grpc_node

    def optional_node_from_grpc_container(self, node_container) -> Optional[TNode]:
        if not node_container.HasField('node'):
            return None

        grpc_node: be.agent.grpc.generated.Node_pb2.TNode = node_container.node
        return self.node_from_grpc(grpc_node)

    def node_from_grpc(self, grpc_node: be.agent.grpc.generated.Node_pb2.TNode) -> TNode:
        node_identifier = self.node_identifier_from_grpc(grpc_node.node_identifier)

        if grpc_node.HasField("gdrive_file_meta"):
            meta = grpc_node.gdrive_file_meta
            assert isinstance(node_identifier, GDriveIdentifier)
            content_meta = self.backend.cacheman.get_content_meta_for(size_bytes=meta.size_bytes, md5=meta.md5, sha256=meta.sha256)
            node = GDriveFile(node_identifier, meta.goog_id, meta.name, meta.mime_type_uid, grpc_node.trashed, meta.drive_id,
                              meta.version, content_meta, meta.size_bytes, grpc_node.is_shared, meta.create_ts, meta.modify_ts,
                              meta.owner_uid, meta.shared_by_user_uid, meta.sync_ts)
            node.set_parent_uids([UID(uid) for uid in meta.parent_uid_list])
        elif grpc_node.HasField("gdrive_folder_meta"):
            meta = grpc_node.gdrive_folder_meta
            assert isinstance(node_identifier, GDriveIdentifier)
            node = GDriveFolder(node_identifier, meta.goog_id, meta.name, grpc_node.trashed, meta.create_ts, meta.modify_ts,
                                meta.owner_uid, meta.drive_id, grpc_node.is_shared, meta.shared_by_user_uid, meta.sync_ts,
                                meta.all_children_fetched)
            node.set_parent_uids([UID(uid) for uid in meta.parent_uid_list])
            node.dir_stats = self.dir_stats_from_grpc(meta.dir_meta)
        elif grpc_node.HasField("local_dir_meta"):
            assert isinstance(node_identifier, LocalNodeIdentifier)
            node = LocalDirNode(node_identifier, grpc_node.local_dir_meta.parent_uid, grpc_node.trashed, grpc_node.local_dir_meta.is_live,
                                grpc_node.local_dir_meta.sync_ts, grpc_node.local_dir_meta.create_ts, grpc_node.local_dir_meta.modify_ts,
                                grpc_node.local_dir_meta.change_ts, grpc_node.all_children_fetched)
            node.dir_stats = self.dir_stats_from_grpc(grpc_node.local_dir_meta.dir_meta)
        elif grpc_node.HasField("local_file_meta"):
            meta = grpc_node.local_file_meta
            assert isinstance(node_identifier, LocalNodeIdentifier)
            content_meta = self.backend.cacheman.get_content_meta_for(size_bytes=meta.size_bytes, md5=meta.md5, sha256=meta.sha256)
            node = LocalFileNode(node_identifier, meta.parent_uid, content_meta, meta.size_bytes, meta.sync_ts, meta.create_ts,
                                 meta.modify_ts, meta.change_ts, grpc_node.trashed, meta.is_live)
        elif grpc_node.HasField("container_meta"):
            assert isinstance(node_identifier, ChangeTreeSPID)
            node = ContainerNode(node_identifier)
            node.dir_stats = self.dir_stats_from_grpc(grpc_node.local_dir_meta.dir_meta)
        elif grpc_node.HasField("category_meta"):
            assert isinstance(node_identifier, ChangeTreeSPID)
            node = CategoryNode(node_identifier)
            node.dir_stats = self.dir_stats_from_grpc(grpc_node.category_meta.dir_meta)
        elif grpc_node.HasField("root_type_meta"):
            assert isinstance(node_identifier, ChangeTreeSPID)
            node = RootTypeNode(node_identifier)
            node.dir_stats = self.dir_stats_from_grpc(grpc_node.root_type_meta.dir_meta)
        elif grpc_node.HasField("nonexistent_dir_meta"):
            assert isinstance(node_identifier, SinglePathNodeIdentifier)
            node = NonexistentDirNode(node_identifier=node_identifier, name=grpc_node.nonexistent_dir_meta.name)
        else:
            raise RuntimeError('Could not parse GRPC node!')

        node.set_icon(IconId(grpc_node.icon_id))

        return node

    @staticmethod
    def dir_stats_from_grpc(dir_meta: be.agent.grpc.generated.Node_pb2.DirMeta) -> DirStats:
        dir_stats = DirStats()
        if dir_meta.has_data:
            dir_stats.file_count = dir_meta.file_count
            dir_stats.dir_count = dir_meta.dir_count
            dir_stats.trashed_file_count = dir_meta.trashed_file_count
            dir_stats.trashed_dir_count = dir_meta.trashed_dir_count
            dir_stats.set_size_bytes(dir_meta.size_bytes)
            dir_stats.trashed_bytes = dir_meta.trashed_bytes
        else:
            dir_stats.size_bytes = None
        return dir_stats

    @staticmethod
    def dir_stats_to_grpc(dir_stats: DirStats, dir_meta_parent):
        if dir_stats:
            dir_meta_parent.dir_meta.has_data = True
            dir_meta_parent.dir_meta.file_count = dir_stats.file_count
            dir_meta_parent.dir_meta.dir_count = dir_stats.dir_count
            dir_meta_parent.dir_meta.trashed_file_count = dir_stats.trashed_file_count
            dir_meta_parent.dir_meta.trashed_dir_count = dir_stats.trashed_dir_count
            dir_meta_parent.dir_meta.size_bytes = dir_stats.get_size_bytes()
            dir_meta_parent.dir_meta.trashed_bytes = dir_stats.trashed_bytes
        else:
            dir_meta_parent.dir_meta.has_data = False

    def dir_stats_dicts_to_grpc(self, dir_stats_dict_by_guid: Dict, dir_stats_dict_by_uid: Dict, dir_meta_grpc_parent):
        if dir_stats_dict_by_guid:
            for key, dir_stats in dir_stats_dict_by_guid.items():
                dir_meta_grpc = dir_meta_grpc_parent.dir_meta_by_guid_list.add()
                dir_meta_grpc.guid = key
                self.dir_stats_to_grpc(dir_stats, dir_meta_parent=dir_meta_grpc)

        if dir_stats_dict_by_uid:
            for key, dir_stats in dir_stats_dict_by_uid.items():
                dir_meta_grpc = dir_meta_grpc_parent.dir_meta_by_uid_list.add()
                dir_meta_grpc.uid = key
                self.dir_stats_to_grpc(dir_stats, dir_meta_parent=dir_meta_grpc)

    # List[TNode]
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def node_list_to_grpc(self, node_list: Iterable[TNode], grpc_node_list):
        for node in node_list:
            grpc_node = grpc_node_list.add()
            self.node_to_grpc(node, grpc_node)

    def node_list_from_grpc(self, grpc_node_list) -> List[TNode]:
        node_list: List[TNode] = []
        for grpc_node in grpc_node_list:
            node = self.node_from_grpc(grpc_node)
            node_list.append(node)
        return node_list

    # NodeIdentifier
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    @staticmethod
    def node_identifier_to_grpc(node_identifier: NodeIdentifier, grpc_node_identifier: be.agent.grpc.generated.Node_pb2.NodeIdentifier):
        if not node_identifier:
            return

        # Common fields for all NodeIdentifiers:
        grpc_node_identifier.identifier_type = node_identifier.get_type()
        grpc_node_identifier.device_uid = node_identifier.device_uid
        grpc_node_identifier.node_uid = node_identifier.node_uid

        # Subtype-specific fields:
        if node_identifier.is_spid():
            assert node_identifier.is_spid() and isinstance(node_identifier, SinglePathNodeIdentifier), f'Not a SPID: {node_identifier}'
            assert node_identifier.path_uid > 0, f'SPID path_uid must positive number: {node_identifier}'

            grpc_node_identifier.spid_meta.path_uid = node_identifier.path_uid
            grpc_node_identifier.spid_meta.single_path = node_identifier.get_single_path()
            if node_identifier.parent_guid:
                grpc_node_identifier.spid_meta.parent_guid = node_identifier.parent_guid
        else:
            for path in node_identifier.get_path_list():
                # path_list is a "repeated" field: cannot be directly assigned in Py-proto
                grpc_node_identifier.multi_path_id_meta.path_list.append(path)

    def node_identifier_from_grpc(self, grpc_node_identifier: be.agent.grpc.generated.Node_pb2.NodeIdentifier):
        if grpc_node_identifier.HasField('multi_path_id_meta'):
            return self.backend.node_identifier_factory.build_node_id(node_uid=UID(grpc_node_identifier.node_uid),
                                                                      device_uid=UID(grpc_node_identifier.device_uid),
                                                                      identifier_type=NodeIdentifierType(grpc_node_identifier.identifier_type),
                                                                      path_list=list(grpc_node_identifier.multi_path_id_meta.path_list))
        else:
            return self.backend.node_identifier_factory.build_spid(node_uid=UID(grpc_node_identifier.node_uid),
                                                                   device_uid=UID(grpc_node_identifier.device_uid),
                                                                   identifier_type=NodeIdentifierType(grpc_node_identifier.identifier_type),
                                                                   single_path=grpc_node_identifier.spid_meta.single_path,
                                                                   path_uid=UID(grpc_node_identifier.spid_meta.path_uid),
                                                                   parent_guid=grpc_node_identifier.spid_meta.parent_guid)

    # SPIDNodePair
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def sn_to_grpc(self, sn: SPIDNodePair, grpc_sn: be.agent.grpc.generated.Node_pb2.SPIDNodePair):
        if not sn:
            raise RuntimeError(f'sn_to_grpc(): no sn!')

        if not sn.spid:
            raise RuntimeError(f'sn_to_grpc(): no SPID!')

        if not sn.node:
            raise RuntimeError(f'sn_to_grpc(): no node! (SPID={sn.spid})')

        self.node_identifier_to_grpc(sn.spid, grpc_sn.spid)
        if sn.node:
            self.node_to_grpc(sn.node, grpc_sn.node)

    def sn_from_grpc(self, grpc_sn: be.agent.grpc.generated.Node_pb2.SPIDNodePair) -> SPIDNodePair:
        spid = self.node_identifier_from_grpc(grpc_sn.spid)
        node = self.optional_node_from_grpc_container(grpc_sn)
        if not node:
            raise RuntimeError(f'sn_from_grpc(): no node! (SPID={spid})')
        return SPIDNodePair(spid, node)

    def sn_list_to_grpc(self, sn_list: Iterable[SPIDNodePair], grpc_sn_list):
        for sn in sn_list:
            grpc_sn = grpc_sn_list.add()
            self.sn_to_grpc(sn, grpc_sn)

    def sn_list_from_grpc(self, grpc_sn_list) -> List[SPIDNodePair]:
        sn_list: List[SPIDNodePair] = []
        for grpc_sn in grpc_sn_list:
            sn = self.sn_from_grpc(grpc_sn)
            sn_list.append(sn)
        return sn_list

    # FilterCriteria
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    @staticmethod
    def filter_criteria_to_grpc(filter_criteria: FilterCriteria, grpc_filter_criteria: be.agent.grpc.generated.Node_pb2.FilterCriteria):
        if filter_criteria.search_query:
            grpc_filter_criteria.search_query = filter_criteria.search_query
        grpc_filter_criteria.is_trashed = filter_criteria.is_trashed
        grpc_filter_criteria.is_shared = filter_criteria.is_shared
        grpc_filter_criteria.is_ignore_case = filter_criteria.ignore_case
        grpc_filter_criteria.show_subtrees_of_matches = filter_criteria.show_ancestors_of_matches

    @staticmethod
    def filter_criteria_from_grpc(grpc_filter_criteria: be.agent.grpc.generated.Node_pb2.FilterCriteria) -> FilterCriteria:
        filter_criteria: FilterCriteria = FilterCriteria()
        if grpc_filter_criteria.search_query:
            filter_criteria.search_query = grpc_filter_criteria.search_query
        filter_criteria.is_trashed = Ternary(grpc_filter_criteria.is_trashed)
        filter_criteria.is_shared = Ternary(grpc_filter_criteria.is_shared)
        filter_criteria.ignore_case = grpc_filter_criteria.is_ignore_case
        filter_criteria.show_ancestors_of_matches = grpc_filter_criteria.show_subtrees_of_matches
        return filter_criteria

    # DisplayTreeUiState
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def display_tree_ui_state_to_grpc(self, state, grpc_state: Outlet_pb2.DisplayTreeUiState):
        if state.tree_id:
            grpc_state.tree_id = state.tree_id
        self.sn_to_grpc(state.root_sn, grpc_state.root_sn)
        grpc_state.root_exists = state.root_exists
        if state.offending_path:
            grpc_state.offending_path = state.offending_path
        grpc_state.needs_manual_load = state.needs_manual_load
        grpc_state.tree_display_mode = state.tree_display_mode
        grpc_state.has_checkboxes = state.has_checkboxes

    def display_tree_ui_state_from_grpc(self, grpc: Outlet_pb2.DisplayTreeUiState) -> DisplayTreeUiState:
        root_sn: SPIDNodePair = self.sn_from_grpc(grpc.root_sn)
        offending_path = grpc.offending_path
        if not offending_path:
            offending_path = None
        return DisplayTreeUiState(grpc.tree_id, root_sn, grpc.root_exists, offending_path, grpc.tree_display_mode, grpc.has_checkboxes)

    # Device
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    @staticmethod
    def device_to_grpc(device: Device, grpc_device: Outlet_pb2.Device):
        grpc_device.device_uid = device.uid
        grpc_device.long_device_id = device.long_device_id
        grpc_device.tree_type = device.tree_type
        grpc_device.friendly_name = device.friendly_name

    @staticmethod
    def device_from_grpc(grpc_device: Outlet_pb2.Device) -> Device:
        return Device(grpc_device.device_uid, grpc_device.long_device_id, grpc_device.tree_type, grpc_device.friendly_name)

    # Tree Context Menu
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def menu_item_list_to_grpc(self, menu_item_list: List[ContextMenuItem], menu_item_list_grpc):
        for menu_item in menu_item_list:
            menu_item_grpc = menu_item_list_grpc.add()
            self.menu_item_to_grpc(menu_item, menu_item_grpc)

    def menu_item_to_grpc(self, menu_item: ContextMenuItem, menu_item_grpc: TreeContextMenuItem):
        menu_item_grpc.item_type = menu_item.item_type
        menu_item_grpc.title = menu_item.title
        menu_item_grpc.action_id = menu_item.action_id
        if menu_item.target_uid:
            menu_item_grpc.target_uid = menu_item.target_uid
        if menu_item.target_guid_list:
            for guid in menu_item.target_guid_list:
                menu_item_grpc.target_guid_list.append(guid)

        # Recurse for all submenu items:
        for submenu_item in menu_item.submenu_item_list:
            submenu_item_grpc = menu_item_grpc.submenu_item_list.add()
            self.menu_item_to_grpc(submenu_item, submenu_item_grpc)

    def menu_item_list_from_grpc(self, menu_item_list_grpc) -> List[ContextMenuItem]:
        menu_item_list = []
        for menu_item_grpc in menu_item_list_grpc:
            menu_item = self.menu_item_from_grpc(menu_item_grpc)
            menu_item_list.append(menu_item)
        return menu_item_list

    def menu_item_from_grpc(self, menu_item_grpc: TreeContextMenuItem) -> ContextMenuItem:
        menu_item = ContextMenuItem(MenuItemType(menu_item_grpc.item_type), menu_item_grpc.title, menu_item_grpc.action_id)
        if menu_item_grpc.target_uid:
            menu_item.target_uid = menu_item_grpc.target_uid
        if menu_item_grpc.target_guid_list:
            for guid in menu_item_grpc.target_guid_list:
                menu_item.target_guid_list.append(guid)
        for submenu_item_grpc in menu_item_grpc.submenu_item_list:
            submenu_item = self.menu_item_from_grpc(submenu_item_grpc)
            menu_item.add_submenu_item(submenu_item)
        return menu_item

    def tree_action_list_to_grpc(self, tree_action_list: List[TreeAction], grpc_action_list_container):
        for action in tree_action_list:
            action_grpc = grpc_action_list_container.action_list.add()

            action_grpc.tree_id = action.tree_id
            action_grpc.action_id = action.action_id

            if action.target_guid_list:
                for guid in action.target_guid_list:
                    action_grpc.target_guid_list.append(guid)

            if action.target_node_list:
                for node in action.target_node_list:
                    node_grpc = action_grpc.target_node_list.add()
                    self.node_to_grpc(node, node_grpc)

            if action.target_uid:
                action_grpc.target_uid = action.target_uid

    def tree_action_list_from_grpc(self, grpc_action_list_container) -> List[TreeAction]:
        action_list = []
        for action_grpc in grpc_action_list_container.action_list:

            target_guid_list = []
            if action_grpc.target_guid_list:
                for guid in action_grpc.target_guid_list:
                    target_guid_list.append(guid)

            target_node_list = []
            if action_grpc.target_node_list:
                for grpc_node in action_grpc.target_node_list:
                    target_node_list.append(self.node_from_grpc(grpc_node))

            target_uid = action_grpc.target_uid if action_grpc.target_uid else None
            action_list.append(TreeAction(action_grpc.tree_id, action_grpc.action_id, target_guid_list, target_node_list, target_uid))

        return action_list

    # Signal
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def signal_from_grpc(self, signal_msg: SignalMsg) -> Dict:
        """Take the signal (received from server) and dispatch it to our UI process"""
        signal = Signal(signal_msg.sig_int)
        kwargs = {}
        # TODO: convert this long conditional list into an action dict
        if signal == Signal.DISPLAY_TREE_CHANGED or signal == Signal.GENERATE_MERGE_TREE_DONE:
            display_tree_ui_state = self.display_tree_ui_state_from_grpc(signal.display_tree_ui_state)
            tree: DisplayTree = display_tree_ui_state.to_display_tree(backend=self.backend)
            kwargs['tree'] = tree
        elif signal == Signal.DIFF_TREES_DONE or signal == Signal.DIFF_TREES_CANCELLED:
            display_tree_ui_state = self.display_tree_ui_state_from_grpc(signal.dual_display_tree.left_tree)
            tree: DisplayTree = display_tree_ui_state.to_display_tree(backend=self.backend)
            kwargs['tree_left'] = tree
            display_tree_ui_state = self.display_tree_ui_state_from_grpc(signal.dual_display_tree.right_tree)
            tree: DisplayTree = display_tree_ui_state.to_display_tree(backend=self.backend)
            kwargs['right_tree'] = tree
        elif signal == Signal.EXECUTE_ACTION:
            kwargs['action_list'] = self.tree_action_list_from_grpc(signal_msg.tree_action_request)
        elif signal == Signal.OP_EXECUTION_PLAY_STATE_CHANGED:
            kwargs['is_enabled'] = signal_msg.play_state.is_enabled
        elif signal == Signal.TOGGLE_UI_ENABLEMENT:
            kwargs['enable'] = signal_msg.ui_enablement.enable
        elif signal == Signal.SET_SELECTED_ROWS:
            guid_set = set()
            for guid in signal.guid_set.guid_set:
                guid_set.add(guid)
            kwargs['selected_rows'] = guid_set
        elif signal == Signal.ERROR_OCCURRED:
            kwargs['msg'] = signal_msg.error_occurred.msg
            kwargs['secondary_msg'] = signal_msg.error_occurred.secondary_msg
        elif signal == Signal.NODE_UPSERTED or signal == Signal.NODE_REMOVED:
            kwargs['sn'] = self.sn_from_grpc(signal_msg.sn)
            kwargs['parent_guid'] = signal_msg.parent_guid
        elif signal == Signal.SUBTREE_NODES_CHANGED:
            kwargs['subtree_root_spid'] = self.node_identifier_from_grpc(signal_msg.subtree.subtree_root_spid)
            kwargs['upserted_sn_list'] = self.sn_list_from_grpc(signal_msg.subtree.upserted_sn_list)
            kwargs['removed_sn_list'] = self.sn_list_from_grpc(signal_msg.subtree.removed_sn_list)
        elif signal == Signal.STATS_UPDATED:
            self._convert_stats_and_status(signal_msg.stats_update, kwargs)
        elif signal == Signal.DOWNLOAD_FROM_GDRIVE_DONE:
            kwargs['filename'] = signal_msg.download_msg.filename
        elif signal == Signal.TREE_LOAD_STATE_UPDATED:
            kwargs['tree_load_state'] = TreeLoadState(signal_msg.tree_load_update.load_state_int)
            self._convert_stats_and_status(signal_msg.tree_load_update.stats_update, kwargs)
        elif signal == Signal.DEVICE_UPSERTED:
            kwargs['device'] = self.device_from_grpc(signal_msg.device)
        elif signal == Signal.HANDLE_BATCH_FAILED:
            kwargs['batch_uid'] = signal_msg.handle_batch_failed.batch_uid
            kwargs['error_handling_strategy'] = ErrorHandlingStrategy(signal_msg.handle_batch_failed.error_handling_strategy)
        logger.info(f'Relaying locally: signal="{signal.name}" sender="{signal_msg.sender}" args={kwargs}')
        kwargs['signal'] = signal
        kwargs['sender'] = signal_msg.sender
        return kwargs

    def _convert_stats_and_status(self, stats_update, kwargs):
        kwargs['status_msg'] = stats_update.status_msg
        dir_stats_dict_by_guid: Dict[GUID, DirStats] = {}
        dir_stats_dict_by_uid: Dict[UID, DirStats] = {}
        for dir_meta_grpc in stats_update.dir_meta_by_guid_list:
            dir_stats = self.dir_stats_from_grpc(dir_meta_grpc.dir_meta)
            dir_stats_dict_by_guid[dir_meta_grpc.guid] = dir_stats
        for dir_meta_grpc in stats_update.dir_meta_by_uid_list:
            dir_stats = self.dir_stats_from_grpc(dir_meta_grpc.dir_meta)
            dir_stats_dict_by_uid[dir_meta_grpc.uid] = dir_stats
        kwargs['dir_stats_dict_by_guid'] = dir_stats_dict_by_guid
        kwargs['dir_stats_dict_by_uid'] = dir_stats_dict_by_uid
