from typing import Optional

import outlet.daemon.grpc
from constants import TREE_TYPE_GDRIVE, TREE_TYPE_LOCAL_DISK
from model.node.container_node import CategoryNode, ContainerNode, RootTypeNode
from model.node.gdrive_node import GDriveFile, GDriveFolder
from model.node.local_disk_node import LocalDirNode, LocalFileNode
from model.node.node import HasChildStats, Node
import logging

from model.node_identifier import GDriveIdentifier, LocalNodeIdentifier, SinglePathNodeIdentifier
from model.node_identifier_factory import NodeIdentifierFactory

logger = logging.getLogger(__name__)


# CLASS NodeConverter
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class NodeConverter:
    @staticmethod
    def optional_node_from_grpc(optional_node) -> Optional[Node]:
        if optional_node.HasField("null"):
            return None
        else:
            return NodeConverter.node_from_grpc(optional_node.node)

    @staticmethod
    def node_from_grpc(grpc_node: outlet.daemon.grpc.Node_pb2.Node) -> Node:
        node_identifier = NodeIdentifierFactory.for_all_values(grpc_node.uid, grpc_node.tree_type, list(grpc_node.path_list))

        if grpc_node.HasField("gdrive_file_meta"):
            meta = grpc_node.gdrive_file_meta
            assert isinstance(node_identifier, GDriveIdentifier)
            return GDriveFile(node_identifier, meta.goog_id, meta.name, meta.mime_type_uid, grpc_node.trashed, meta.drive_id,
                              meta.version, meta.md5, grpc_node.is_shared, meta.create_ts, meta.modify_ts, meta.size_bytes,
                              meta.owner_uid, meta.shared_by_user_uid, meta.sync_ts)
        elif grpc_node.HasField("gdrive_folder_meta"):
            meta = grpc_node.gdrive_folder_meta
            assert isinstance(node_identifier, GDriveIdentifier)
            node = GDriveFolder(node_identifier, meta.goog_id, meta.name, grpc_node.trashed, meta.create_ts, meta.modify_ts,
                                meta.owner_uid, meta.drive_id, grpc_node.is_shared, meta.shared_by_user_uid, meta.sync_ts,
                                meta.all_children_fetched)
            NodeConverter._dir_meta_from_grpc(node, meta.dir_meta)
            return node
        elif grpc_node.HasField("local_dir_meta"):
            assert isinstance(node_identifier, LocalNodeIdentifier)
            node = LocalDirNode(node_identifier, grpc_node.trashed, grpc_node.local_dir_meta.is_live)
            NodeConverter._dir_meta_from_grpc(node, grpc_node.local_dir_meta.dir_meta)
            return node
        elif grpc_node.HasField("local_file_meta"):
            meta = grpc_node.local_file_meta
            assert isinstance(node_identifier, LocalNodeIdentifier)
            return LocalFileNode(node_identifier, meta.md5, meta.sha256, meta.size_bytes, meta.sync_ts, meta.modify_ts,
                                 meta.change_ts, grpc_node.trashed, meta.is_live)
        elif grpc_node.HasField("container_meta"):
            assert isinstance(node_identifier, SinglePathNodeIdentifier)
            node = ContainerNode(node_identifier, grpc_node.nid)
            NodeConverter._dir_meta_from_grpc(node, grpc_node.local_dir_meta.dir_meta)
            return node
        elif grpc_node.HasField("category_meta"):
            assert isinstance(node_identifier, SinglePathNodeIdentifier)
            node = CategoryNode(node_identifier, grpc_node.category_meta.op_type, grpc_node.nid)
            NodeConverter._dir_meta_from_grpc(node, grpc_node.category_meta.dir_meta)
            return node
        elif grpc_node.HasField("root_type_meta"):
            assert isinstance(node_identifier, SinglePathNodeIdentifier)
            node = RootTypeNode(node_identifier, grpc_node.nid)
            NodeConverter._dir_meta_from_grpc(node, grpc_node.root_type_meta.dir_meta)
            return node
        else:
            raise RuntimeError('Could not parse GRPC node!')

    @staticmethod
    def _dir_meta_from_grpc(node: HasChildStats, dir_meta: outlet.daemon.grpc.Node_pb2.DirMeta):
        if dir_meta.has_data:
            node.file_count = dir_meta.file_count
            node.dir_count = dir_meta.dir_count
            node.trashed_file_count = dir_meta.trashed_file_count
            node.trashed_dir_count = dir_meta.trashed_dir_count
            node.size_bytes = dir_meta.get_size_bytes()
            node.trashed_bytes = dir_meta.trashed_bytes
        else:
            node.size_bytes = None

    @staticmethod
    def _dir_meta_to_grpc(node: HasChildStats, dir_meta_parent) -> outlet.daemon.grpc.Node_pb2.DirMeta:
        if node.is_stats_loaded():
            dir_meta_parent.dir_meta.has_data = True
            dir_meta_parent.dir_meta.file_count = node.file_count
            dir_meta_parent.dir_meta.dir_count = node.dir_count
            dir_meta_parent.dir_meta.trashed_file_count = node.trashed_file_count
            dir_meta_parent.dir_meta.trashed_dir_count = node.trashed_dir_count
            dir_meta_parent.dir_meta.size_bytes = node.get_size_bytes()
            dir_meta_parent.dir_meta.trashed_bytes = node.trashed_bytes
        else:
            dir_meta_parent.dir_meta.has_data = False

    @staticmethod
    def optional_node_to_grpc(node: Node, optional_node):
        grpc_node = outlet.daemon.grpc.Node_pb2.OptionalNode()
        if node:
            NodeConverter.node_to_grpc(node, optional_node.node)
        if not node:
            optional_node.null.SetInParent()
        return grpc_node

    @staticmethod
    def node_to_grpc(node: Node, grpc_node: outlet.daemon.grpc.Node_pb2.Node()):
        # node_identifier fields:
        grpc_node.uid = int(node.uid)
        grpc_node.tree_type = node.get_tree_type()
        for full_path in node.get_path_list():
            grpc_node.path_list.append(full_path)
        grpc_node.nid = str(node.identifier)

        # Node common fields:
        grpc_node.trashed = node.get_trashed_status()
        grpc_node.is_shared = node.is_shared

        if isinstance(node, ContainerNode):
            # ContainerNode or subclass
            if isinstance(node, CategoryNode):
                # grpc_node.category_meta = outlet.daemon.grpc.Node_pb2.CategoryNodeMeta()
                NodeConverter._dir_meta_to_grpc(node, grpc_node.category_meta)
                grpc_node.category_meta.op_type = node.op_type
            elif isinstance(node, RootTypeNode):
                # grpc_node.root_type_meta = outlet.daemon.grpc.Node_pb2.RootTypeNodeMeta()
                NodeConverter._dir_meta_to_grpc(node, grpc_node.root_type_meta)
            else:
                # plain ContainerNode
                # grpc_node.container_meta = outlet.daemon.grpc.Node_pb2.ContainerNodeMeta()
                NodeConverter._dir_meta_to_grpc(node, grpc_node.container_meta)
        elif node.get_tree_type() == TREE_TYPE_LOCAL_DISK:
            # LocalNode
            if node.is_dir():
                assert isinstance(node, LocalDirNode)
                # grpc_node.local_dir_meta = outlet.daemon.grpc.Node_pb2.LocalDirMeta()
                NodeConverter._dir_meta_to_grpc(node, grpc_node.local_dir_meta)
                grpc_node.local_dir_meta.is_live = node.is_live()
            else:
                # grpc_node.local_file_meta = outlet.daemon.grpc.Node_pb2.LocalFileMeta()
                grpc_node.local_file_meta.size_bytes = node.get_size_bytes()
                grpc_node.local_file_meta.sync_ts = node.sync_ts
                grpc_node.local_file_meta.modify_ts = node.modify_ts
                grpc_node.local_file_meta.change_ts = node.change_ts
                grpc_node.local_file_meta.is_live = node.is_live()
                grpc_node.local_file_meta.md5 = node.md5
                grpc_node.local_file_meta.sha256 = node.sha256
        elif node.get_tree_type() == TREE_TYPE_GDRIVE:
            # GDriveNode
            if node.is_dir():
                assert isinstance(node, GDriveFolder)
                # meta = outlet.daemon.grpc.Node_pb2.GDriveFolderMeta()
                # grpc_node.gdrive_folder_meta = meta

                grpc_node.gdrive_folder_meta.dir_meta = NodeConverter._dir_meta_to_grpc(node)
                grpc_node.gdrive_folder_meta.all_children_fetched = node.all_children_fetched
                meta = grpc_node.gdrive_folder_meta
            else:
                assert isinstance(node, GDriveFile)
                # meta = outlet.daemon.grpc.Node_pb2.GDriveFileMeta()
                # grpc_node.gdrive_file_meta = meta

                grpc_node.gdrive_file_meta.md5 = node.md5
                grpc_node.gdrive_file_meta.version = node.version
                grpc_node.gdrive_file_meta.size_bytes = node.get_size_bytes()
                grpc_node.gdrive_file_meta.mime_type_uid = node.mime_type_uid
                meta = grpc_node.gdrive_file_meta

            # GDriveNode common fields:
            meta.goog_id = node.goog_id
            meta.name = node.name
            meta.owner_uid = node.owner_uid
            meta.shared_by_user_uid = node.shared_by_user_uid
            meta.drive_id = node.drive_id
            for parent_uid in node.get_parent_uids():
                meta.parent_uid_list.append(parent_uid)
            meta.sync_ts = node.sync_ts
            meta.modify_ts = node.modify_ts
            meta.create_ts = node.create_ts
        return grpc_node

