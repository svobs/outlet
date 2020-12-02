import logging
import time

from daemon.grpc.conversion import NodeConverter
from daemon.grpc.Outlet_pb2 import GetNextUid_Response, GetNodeForLocalPath_Request, GetNodeForUid_Request, GetUidForLocalPath_Request, \
    GetUidForLocalPath_Response, PingResponse, Signal, SingleNode_Response, SubscribeRequest
from daemon.grpc.Outlet_pb2_grpc import OutletServicer
from model.display_tree.display_tree import DisplayTree
from store.cache_manager import CacheManager
from store.uid.uid_generator import UidGenerator
from ui import actions

logger = logging.getLogger(__name__)


def process_single_item(self, tree_id: str):
    logger.debug(f'[{self.name}] Submitted load request for tree_id: {tree_id}')
    self.backend.executor.submit_async_task(self.cacheman.load_data_for_display_tree, tree_id)


# CLASS OutletGRPCService
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class OutletGRPCService(OutletServicer):
    """Backend gRPC Server"""
    def __init__(self, parent):
        self.uid_generator: UidGenerator = parent.uid_generator
        self.cacheman: CacheManager = parent.cacheman

    def ping(self, request, context):
        logger.info(f'Got ping!')
        response = PingResponse()
        response.timestamp = 1000
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
        response.uid = self.cacheman.get_uid_for_local_path(request.full_path, request.uid_suggestion)
        return response

    def subscribe_to_signals(self, request: SubscribeRequest, context):
        for i in range(4):
            for sender_id in request.subscriber_id_list:
                # TODO: server ?
                logger.info(f'Got sender_id {i}: {sender_id}')
                time.sleep(1)
                signal = Signal(signal_name=actions.CALL_EXIFTOOL, sender_name=actions.ID_GLOBAL_CACHE)
                yield signal
    #
    # def signal(self, request_iterator, context):
    #     for req in request_iterator:
    #         print("Translating message: {}".format(req.msg))
    #         yield Msg(msg=translate_next(req.msg))

    def start_subtree_load(self, request):
        self.cacheman.enqueue_load_subtree_task(request.tree_id)
        # FIXME: implement in gRPC
        return None

    def _on_subtree_load_started(self, sender: str):
        # FIXME: implement stream in gRPC
        request = Signal()
        request.signal_name = actions.LOAD_SUBTREE_STARTED
        request.sender_name = sender

        self.signal(request)

    def _on_subtree_load_done(self, sender: str):
        # FIXME: implement stream in gRPC
        request = Signal()
        request.signal_name = actions.LOAD_SUBTREE_DONE
        request.sender_name = sender

        self.signal(request)

    def _on_display_tree_changed(self, sender: str, tree: DisplayTree):
        # FIXME: implement stream in gRPC
        request = Signal()
        request.signal_name = actions.DISPLAY_TREE_CHANGED
        request.sender_name = sender
        request.display_tree_meta = NodeConverter.display_tree_ui_state_to_grpc(tree.state, request.display_tree_meta)

        self.signal(request)
