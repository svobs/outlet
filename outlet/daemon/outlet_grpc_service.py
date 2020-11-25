import logging

from daemon.grpc.conversion import NodeConverter
from daemon.grpc.Outlet_pb2 import PingResponse, ReadSingleNodeFromDiskRequest, ReadSingleNodeFromDiskResponse
from daemon.grpc.Outlet_pb2_grpc import OutletServicer

logger = logging.getLogger(__name__)


# CLASS OutletGRPCService
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class OutletGRPCService(OutletServicer):
    def __init__(self, parent):
        self.cacheman = parent.cacheman

    def ping(self, request, context):
        logger.info(f'Got ping!')
        response = PingResponse()
        response.timestamp = 1000
        return response

    def read_single_node_from_disk_for_path(self, request: ReadSingleNodeFromDiskRequest, context):
        node = self.cacheman.read_single_node_from_disk_for_path(request.full_path, request.tree_type)
        response = ReadSingleNodeFromDiskResponse()
        NodeConverter.optional_node_to_grpc(node, response.node)
        return response
