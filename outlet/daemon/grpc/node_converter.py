import outlet.daemon.grpc
from model.node.node import Node
import logging

logger = logging.getLogger(__name__)


# CLASS NodeConverter
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class NodeConverter:

    @staticmethod
    def node_from_grpc(grpc_node: outlet.daemon.grpc.Node_pb2.Node) -> Node:
        logger.info(f'Got: {grpc_node.uid}')
        return None

    @staticmethod
    def node_to_grpc(node: Node):
        grpc_node = outlet.daemon.grpc.Node_pb2.Node()
        grpc_node.uid = int(node.uid)
        for full_path in node.get_path_list():
            grpc_node.path_list.append(full_path)
        grpc_node.nid = str(node.identifier)
        grpc_node.trashed = node.get_trashed_status()
        grpc_node.is_shared = node.is_shared
        return grpc_node

