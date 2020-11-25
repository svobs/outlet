import logging

from daemon.grpc.conversion import NodeConverter
from daemon.grpc.Outlet_pb2 import GetNextUid_Response, GetNodeForLocalPath_Request, GetNodeForUid_Request, GetUidForLocalPath_Request, \
    GetUidForLocalPath_Response, PingResponse, \
    ReadSingleNodeFromDiskRequest, SingleNode_Response
from daemon.grpc.Outlet_pb2_grpc import OutletServicer
from store.cache_manager import CacheManager
from store.uid.uid_generator import UidGenerator

logger = logging.getLogger(__name__)


# CLASS OutletGRPCService
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class OutletGRPCService(OutletServicer):
    def __init__(self, parent):
        self.uid_generator: UidGenerator = parent.uid_generator
        self.cacheman: CacheManager = parent.cacheman

    def ping(self, request, context):
        logger.info(f'Got ping!')
        response = PingResponse()
        response.timestamp = 1000
        return response

    def read_single_node_from_disk_for_path(self, request: ReadSingleNodeFromDiskRequest, context):
        response = SingleNode_Response()
        node = self.cacheman.read_single_node_from_disk_for_path(request.full_path, request.tree_type)
        if node:
            NodeConverter.node_to_grpc(node, response.node)
        return response

    def get_node_for_uid(self, request: GetNodeForUid_Request, context):
        response = SingleNode_Response()
        node = self.cacheman.get_node_for_uid(request.full_path, request.tree_type)
        if node:
            NodeConverter.node_to_grpc(node, response.node)
        return response

    def get_node_for_local_path(self, request: GetNodeForLocalPath_Request, context):
        response = SingleNode_Response()
        node = self.cacheman.get_node_for_local_path(request.full_path)
        if node:
            NodeConverter.node_to_grpc(node, response.node)
        return response

    def get_next_uid(self, request, context):
        response = GetNextUid_Response()
        response.uid = self.uid_generator.next_uid()
        return response

    def get_uid_for_local_path(self, request: GetUidForLocalPath_Request, context):
        response = GetUidForLocalPath_Response()
        response.uid = self.cacheman.get_uid_for_path(request.full_path, request.uid_suggestion)
        return response
