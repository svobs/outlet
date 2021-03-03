from typing import Iterable, List, Optional
import backend.daemon.grpc.generated.Node_pb2

from constants import IconId, TREE_TYPE_GDRIVE, TREE_TYPE_LOCAL_DISK
from model.display_tree.display_tree import DisplayTreeUiState
from model.node.container_node import CategoryNode, ContainerNode, RootTypeNode
from model.node.decorator_node import DecoNode, decorate_node
from model.node.directory_stats import DirectoryStats
from model.node.gdrive_node import GDriveFile, GDriveFolder
from model.node.local_disk_node import LocalDirNode, LocalFileNode
from model.node.node import Node, SPIDNodePair
import logging

from model.uid import UID
from outlet.backend.daemon.grpc.generated import Outlet_pb2

from model.node_identifier import GDriveIdentifier, LocalNodeIdentifier, NodeIdentifier, SinglePathNodeIdentifier
from model.node_identifier_factory import NodeIdentifierFactory
from model.display_tree.filter_criteria import Ternary, FilterCriteria

logger = logging.getLogger(__name__)


class GRPCConverter:
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS GRPCConverter

    Converts Swift objects to and from GRPC messages.
    Note on ordering of methods: TO comes before FROM
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """

    # Node
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    @staticmethod
    def node_to_grpc(node: Node, grpc_node: backend.daemon.grpc.generated.Node_pb2.Node):
        assert isinstance(node, Node), f'Not a Node: {node}'
        if node.is_decorator():
            grpc_node.decorator_nid = int(node.uid)
            grpc_node.decorator_parent_nid = int(node.get_parent_uids()[0])

            assert isinstance(node, DecoNode),  f'Not a DecoNode: {node}'
            # Fill out the delegate node identifier down below.
            # Remember: we still want to keep the DecoNode for its DirStats (way below)
            node_identifier = node.delegate.node_identifier
        else:
            node_identifier = node.node_identifier

        # node_identifier fields:
        grpc_node.uid = int(node_identifier.uid)
        grpc_node.tree_type = node_identifier.get_tree_type()
        for full_path in node_identifier.get_path_list():
            grpc_node.path_list.append(full_path)

        # Node common fields:
        grpc_node.trashed = node.get_trashed_status()
        grpc_node.is_shared = node.is_shared
        icon = node.get_custom_icon()
        if icon:
            grpc_node.icon_id = icon.value

        if isinstance(node, ContainerNode):
            # ContainerNode or subclass
            if isinstance(node, CategoryNode):
                GRPCConverter.dir_stats_to_grpc(node.dir_stats, grpc_node.category_meta)
                grpc_node.category_meta.op_type = node.op_type
            elif isinstance(node, RootTypeNode):
                GRPCConverter.dir_stats_to_grpc(node.dir_stats, grpc_node.root_type_meta)
            else:
                # plain ContainerNode
                GRPCConverter.dir_stats_to_grpc(node.dir_stats, grpc_node.container_meta)
        elif node.get_tree_type() == TREE_TYPE_LOCAL_DISK:
            if node.is_dir():
                assert isinstance(node, LocalDirNode)
                GRPCConverter.dir_stats_to_grpc(node.dir_stats, grpc_node.local_dir_meta)
                grpc_node.local_dir_meta.is_live = node.is_live()
                grpc_node.local_dir_meta.parent_uid = node.get_single_parent()
            else:
                if node.get_size_bytes():
                    grpc_node.local_file_meta.size_bytes = node.get_size_bytes()
                if node.sync_ts:
                    grpc_node.local_file_meta.sync_ts = node.sync_ts
                if node.modify_ts:
                    grpc_node.local_file_meta.modify_ts = node.modify_ts
                if node.change_ts:
                    grpc_node.local_file_meta.change_ts = node.change_ts
                grpc_node.local_file_meta.is_live = node.is_live()
                if node.md5:
                    grpc_node.local_file_meta.md5 = node.md5
                if node.sha256:
                    grpc_node.local_file_meta.sha256 = node.sha256
                grpc_node.local_file_meta.parent_uid = node.get_single_parent()
        elif node.get_tree_type() == TREE_TYPE_GDRIVE:
            # GDriveNode
            if node.is_dir():
                assert isinstance(node, GDriveFolder)
                GRPCConverter.dir_stats_to_grpc(node.dir_stats, grpc_node.gdrive_folder_meta)
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

    @staticmethod
    def optional_node_from_grpc_container(node_container) -> Optional[Node]:
        if not node_container.HasField('node'):
            return None

        grpc_node: backend.daemon.grpc.generated.Node_pb2.Node = node_container.node
        return GRPCConverter.node_from_grpc(grpc_node)

    @staticmethod
    def node_from_grpc(grpc_node: backend.daemon.grpc.generated.Node_pb2.Node) -> Node:
        node_identifier = NodeIdentifierFactory.for_all_values(grpc_node.uid, grpc_node.tree_type, list(grpc_node.path_list),
                                                               single_path=False)

        if grpc_node.HasField("gdrive_file_meta"):
            meta = grpc_node.gdrive_file_meta
            assert isinstance(node_identifier, GDriveIdentifier)
            node = GDriveFile(node_identifier, meta.goog_id, meta.name, meta.mime_type_uid, grpc_node.trashed, meta.drive_id,
                              meta.version, meta.md5, grpc_node.is_shared, meta.create_ts, meta.modify_ts, meta.size_bytes,
                              meta.owner_uid, meta.shared_by_user_uid, meta.sync_ts)
            node.set_parent_uids([UID(uid) for uid in meta.parent_uid_list])
        elif grpc_node.HasField("gdrive_folder_meta"):
            meta = grpc_node.gdrive_folder_meta
            assert isinstance(node_identifier, GDriveIdentifier)
            node = GDriveFolder(node_identifier, meta.goog_id, meta.name, grpc_node.trashed, meta.create_ts, meta.modify_ts,
                                meta.owner_uid, meta.drive_id, grpc_node.is_shared, meta.shared_by_user_uid, meta.sync_ts,
                                meta.all_children_fetched)
            node.set_parent_uids([UID(uid) for uid in meta.parent_uid_list])
            node.dir_stats = GRPCConverter.dir_stats_from_grpc(meta.dir_meta)
        elif grpc_node.HasField("local_dir_meta"):
            assert isinstance(node_identifier, LocalNodeIdentifier)
            node = LocalDirNode(node_identifier, grpc_node.local_dir_meta.parent_uid, grpc_node.trashed, grpc_node.local_dir_meta.is_live)
            node.dir_stats = GRPCConverter.dir_stats_from_grpc(grpc_node.local_dir_meta.dir_meta)
        elif grpc_node.HasField("local_file_meta"):
            meta = grpc_node.local_file_meta
            assert isinstance(node_identifier, LocalNodeIdentifier)
            node = LocalFileNode(node_identifier, meta.parent_uid, meta.md5, meta.sha256, meta.size_bytes, meta.sync_ts, meta.modify_ts,
                                 meta.change_ts, grpc_node.trashed, meta.is_live)
        elif grpc_node.HasField("container_meta"):
            assert isinstance(node_identifier, SinglePathNodeIdentifier)
            node = ContainerNode(node_identifier)
            node.dir_stats = GRPCConverter.dir_stats_from_grpc(grpc_node.local_dir_meta.dir_meta)
        elif grpc_node.HasField("category_meta"):
            assert isinstance(node_identifier, SinglePathNodeIdentifier)
            node = CategoryNode(node_identifier, grpc_node.category_meta.op_type)
            node.dir_stats = GRPCConverter.dir_stats_from_grpc(grpc_node.category_meta.dir_meta)
        elif grpc_node.HasField("root_type_meta"):
            assert isinstance(node_identifier, SinglePathNodeIdentifier)
            node = RootTypeNode(node_identifier)
            node.dir_stats = GRPCConverter.dir_stats_from_grpc(grpc_node.root_type_meta.dir_meta)
        else:
            raise RuntimeError('Could not parse GRPC node!')

        node.set_icon(IconId(grpc_node.icon_id))

        if grpc_node.decorator_nid:
            assert grpc_node.decorator_parent_nid, f'No parent_nid for decorator node! (node so far: {node})'
            node = decorate_node(grpc_node.decorator_nid, grpc_node.decorator_parent_nid, node)
        return node

    @staticmethod
    def dir_stats_from_grpc(dir_meta: backend.daemon.grpc.generated.Node_pb2.DirMeta) -> DirectoryStats:
        dir_stats = DirectoryStats()
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
    def dir_stats_to_grpc(dir_stats: DirectoryStats, dir_meta_parent):
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

    # List[Node]
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    @staticmethod
    def node_list_to_grpc(node_list: Iterable[Node], grpc_node_list):
        for node in node_list:
            grpc_node = grpc_node_list.add()
            GRPCConverter.node_to_grpc(node, grpc_node)

    @staticmethod
    def node_list_from_grpc(grpc_node_list) -> List[Node]:
        node_list: List[Node] = []
        for grpc_node in grpc_node_list:
            node = GRPCConverter.node_from_grpc(grpc_node)
            node_list.append(node)
        return node_list

    # NodeIdentifier
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    @staticmethod
    def node_identifier_to_grpc(node_identifier: NodeIdentifier, grpc_node_identifier: backend.daemon.grpc.generated.Node_pb2.NodeIdentifier):
        if not node_identifier:
            return
        grpc_node_identifier.uid = node_identifier.uid
        grpc_node_identifier.tree_type = node_identifier.tree_type
        for full_path in node_identifier.get_path_list():
            grpc_node_identifier.path_list.append(full_path)
        grpc_node_identifier.is_single_path = node_identifier.is_spid()
        assert not grpc_node_identifier.is_single_path or len(list(grpc_node_identifier.path_list)) <= 1, f'Wrong: {node_identifier}'

    @staticmethod
    def node_identifier_from_grpc(grpc_node_identifier: backend.daemon.grpc.generated.Node_pb2.NodeIdentifier):
        return NodeIdentifierFactory.for_all_values(grpc_node_identifier.uid, grpc_node_identifier.tree_type, list(grpc_node_identifier.path_list),
                                                    single_path=grpc_node_identifier.is_single_path)

    # SPIDNodePair
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    @staticmethod
    def sn_to_grpc(sn: SPIDNodePair, grpc_sn: backend.daemon.grpc.generated.Node_pb2.SPIDNodePair):
        if not sn:
            return

        GRPCConverter.node_identifier_to_grpc(sn.spid, grpc_sn.spid)
        if sn.node:
            GRPCConverter.node_to_grpc(sn.node, grpc_sn.node)

    @staticmethod
    def sn_from_grpc(grpc_sn: backend.daemon.grpc.generated.Node_pb2.SPIDNodePair) -> SPIDNodePair:
        spid = GRPCConverter.node_identifier_from_grpc(grpc_sn.spid)
        node = GRPCConverter.optional_node_from_grpc_container(grpc_sn)
        return SPIDNodePair(spid, node)

    # FilterCriteria
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    @staticmethod
    def filter_criteria_to_grpc(filter_criteria: FilterCriteria, grpc_filter_criteria: backend.daemon.grpc.generated.Node_pb2.FilterCriteria):
        if filter_criteria.search_query:
            grpc_filter_criteria.search_query = filter_criteria.search_query
        grpc_filter_criteria.is_trashed = filter_criteria.is_trashed
        grpc_filter_criteria.is_shared = filter_criteria.is_shared
        grpc_filter_criteria.is_ignore_case = filter_criteria.ignore_case
        grpc_filter_criteria.show_subtrees_of_matches = filter_criteria.show_ancestors_of_matches

    @staticmethod
    def filter_criteria_from_grpc(grpc_filter_criteria: backend.daemon.grpc.generated.Node_pb2.FilterCriteria) -> FilterCriteria:
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

    @staticmethod
    def display_tree_ui_state_to_grpc(state, grpc_state: Outlet_pb2.DisplayTreeUiState):
        if state.tree_id:
            grpc_state.tree_id = state.tree_id
        GRPCConverter.sn_to_grpc(state.root_sn, grpc_state.root_sn)
        grpc_state.root_exists = state.root_exists
        if state.offending_path:
            grpc_state.offending_path = state.offending_path
        grpc_state.needs_manual_load = state.needs_manual_load
        grpc_state.tree_display_mode = state.tree_display_mode
        grpc_state.has_checkboxes = state.has_checkboxes

    @staticmethod
    def display_tree_ui_state_from_grpc(grpc: Outlet_pb2.DisplayTreeUiState) -> DisplayTreeUiState:
        root_sn: SPIDNodePair = GRPCConverter.sn_from_grpc(grpc.root_sn)
        offending_path = grpc.offending_path
        if not offending_path:
            offending_path = None
        return DisplayTreeUiState(grpc.tree_id, root_sn, grpc.root_exists, offending_path, grpc.tree_display_mode, grpc.has_checkboxes)
