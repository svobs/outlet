# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc

from outlet.daemon.grpc import Outlet_pb2 as outlet_dot_daemon_dot_grpc_dot_Outlet__pb2


class OutletStub(object):
    """Missing associated documentation comment in .proto file."""

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.subscribe_to_signals = channel.unary_stream(
                '/outlet.daemon.grpc.Outlet/subscribe_to_signals',
                request_serializer=outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.Subscribe_Request.SerializeToString,
                response_deserializer=outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.SignalMsg.FromString,
                )
        self.send_signal = channel.unary_unary(
                '/outlet.daemon.grpc.Outlet/send_signal',
                request_serializer=outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.SignalMsg.SerializeToString,
                response_deserializer=outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.SendSignalResponse.FromString,
                )
        self.get_node_for_uid = channel.unary_unary(
                '/outlet.daemon.grpc.Outlet/get_node_for_uid',
                request_serializer=outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.GetNodeForUid_Request.SerializeToString,
                response_deserializer=outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.SingleNode_Response.FromString,
                )
        self.get_node_for_local_path = channel.unary_unary(
                '/outlet.daemon.grpc.Outlet/get_node_for_local_path',
                request_serializer=outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.GetNodeForLocalPath_Request.SerializeToString,
                response_deserializer=outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.SingleNode_Response.FromString,
                )
        self.get_child_list_for_node = channel.unary_unary(
                '/outlet.daemon.grpc.Outlet/get_child_list_for_node',
                request_serializer=outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.GetChildList_Request.SerializeToString,
                response_deserializer=outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.GetChildList_Response.FromString,
                )
        self.get_ancestor_list_for_spid = channel.unary_unary(
                '/outlet.daemon.grpc.Outlet/get_ancestor_list_for_spid',
                request_serializer=outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.GetAncestorList_Request.SerializeToString,
                response_deserializer=outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.GetAncestorList_Response.FromString,
                )
        self.request_display_tree_ui_state = channel.unary_unary(
                '/outlet.daemon.grpc.Outlet/request_display_tree_ui_state',
                request_serializer=outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.RequestDisplayTree_Request.SerializeToString,
                response_deserializer=outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.RequestDisplayTree_Response.FromString,
                )
        self.start_subtree_load = channel.unary_unary(
                '/outlet.daemon.grpc.Outlet/start_subtree_load',
                request_serializer=outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.StartSubtreeLoad_Request.SerializeToString,
                response_deserializer=outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.StartSubtreeLoad_Response.FromString,
                )
        self.get_op_exec_play_state = channel.unary_unary(
                '/outlet.daemon.grpc.Outlet/get_op_exec_play_state',
                request_serializer=outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.GetOpExecPlayState_Request.SerializeToString,
                response_deserializer=outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.PlayState.FromString,
                )
        self.start_diff_trees = channel.unary_unary(
                '/outlet.daemon.grpc.Outlet/start_diff_trees',
                request_serializer=outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.StartDiffTrees_Request.SerializeToString,
                response_deserializer=outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.StartDiffTrees_Response.FromString,
                )
        self.refresh_subtree = channel.unary_unary(
                '/outlet.daemon.grpc.Outlet/refresh_subtree',
                request_serializer=outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.RefreshSubtree_Request.SerializeToString,
                response_deserializer=outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.Empty.FromString,
                )
        self.refresh_subtree_stats = channel.unary_unary(
                '/outlet.daemon.grpc.Outlet/refresh_subtree_stats',
                request_serializer=outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.RefreshSubtreeStats_Request.SerializeToString,
                response_deserializer=outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.Empty.FromString,
                )
        self.get_next_uid = channel.unary_unary(
                '/outlet.daemon.grpc.Outlet/get_next_uid',
                request_serializer=outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.GetNextUid_Request.SerializeToString,
                response_deserializer=outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.GetNextUid_Response.FromString,
                )
        self.get_uid_for_local_path = channel.unary_unary(
                '/outlet.daemon.grpc.Outlet/get_uid_for_local_path',
                request_serializer=outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.GetUidForLocalPath_Request.SerializeToString,
                response_deserializer=outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.GetUidForLocalPath_Response.FromString,
                )
        self.drop_dragged_nodes = channel.unary_unary(
                '/outlet.daemon.grpc.Outlet/drop_dragged_nodes',
                request_serializer=outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.DragDrop_Request.SerializeToString,
                response_deserializer=outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.DragDrop_Response.FromString,
                )
        self.get_last_pending_op_for_node = channel.unary_unary(
                '/outlet.daemon.grpc.Outlet/get_last_pending_op_for_node',
                request_serializer=outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.GetLastPendingOp_Request.SerializeToString,
                response_deserializer=outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.GetLastPendingOp_Response.FromString,
                )
        self.download_file_from_gdrive = channel.unary_unary(
                '/outlet.daemon.grpc.Outlet/download_file_from_gdrive',
                request_serializer=outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.DownloadFromGDrive_Request.SerializeToString,
                response_deserializer=outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.Empty.FromString,
                )
        self.delete_subtree = channel.unary_unary(
                '/outlet.daemon.grpc.Outlet/delete_subtree',
                request_serializer=outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.DeleteSubtree_Request.SerializeToString,
                response_deserializer=outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.Empty.FromString,
                )


class OutletServicer(object):
    """Missing associated documentation comment in .proto file."""

    def subscribe_to_signals(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def send_signal(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def get_node_for_uid(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def get_node_for_local_path(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def get_child_list_for_node(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def get_ancestor_list_for_spid(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def request_display_tree_ui_state(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def start_subtree_load(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def get_op_exec_play_state(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def start_diff_trees(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def refresh_subtree(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def refresh_subtree_stats(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def get_next_uid(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def get_uid_for_local_path(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def drop_dragged_nodes(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def get_last_pending_op_for_node(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def download_file_from_gdrive(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def delete_subtree(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_OutletServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'subscribe_to_signals': grpc.unary_stream_rpc_method_handler(
                    servicer.subscribe_to_signals,
                    request_deserializer=outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.Subscribe_Request.FromString,
                    response_serializer=outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.SignalMsg.SerializeToString,
            ),
            'send_signal': grpc.unary_unary_rpc_method_handler(
                    servicer.send_signal,
                    request_deserializer=outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.SignalMsg.FromString,
                    response_serializer=outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.SendSignalResponse.SerializeToString,
            ),
            'get_node_for_uid': grpc.unary_unary_rpc_method_handler(
                    servicer.get_node_for_uid,
                    request_deserializer=outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.GetNodeForUid_Request.FromString,
                    response_serializer=outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.SingleNode_Response.SerializeToString,
            ),
            'get_node_for_local_path': grpc.unary_unary_rpc_method_handler(
                    servicer.get_node_for_local_path,
                    request_deserializer=outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.GetNodeForLocalPath_Request.FromString,
                    response_serializer=outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.SingleNode_Response.SerializeToString,
            ),
            'get_child_list_for_node': grpc.unary_unary_rpc_method_handler(
                    servicer.get_child_list_for_node,
                    request_deserializer=outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.GetChildList_Request.FromString,
                    response_serializer=outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.GetChildList_Response.SerializeToString,
            ),
            'get_ancestor_list_for_spid': grpc.unary_unary_rpc_method_handler(
                    servicer.get_ancestor_list_for_spid,
                    request_deserializer=outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.GetAncestorList_Request.FromString,
                    response_serializer=outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.GetAncestorList_Response.SerializeToString,
            ),
            'request_display_tree_ui_state': grpc.unary_unary_rpc_method_handler(
                    servicer.request_display_tree_ui_state,
                    request_deserializer=outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.RequestDisplayTree_Request.FromString,
                    response_serializer=outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.RequestDisplayTree_Response.SerializeToString,
            ),
            'start_subtree_load': grpc.unary_unary_rpc_method_handler(
                    servicer.start_subtree_load,
                    request_deserializer=outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.StartSubtreeLoad_Request.FromString,
                    response_serializer=outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.StartSubtreeLoad_Response.SerializeToString,
            ),
            'get_op_exec_play_state': grpc.unary_unary_rpc_method_handler(
                    servicer.get_op_exec_play_state,
                    request_deserializer=outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.GetOpExecPlayState_Request.FromString,
                    response_serializer=outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.PlayState.SerializeToString,
            ),
            'start_diff_trees': grpc.unary_unary_rpc_method_handler(
                    servicer.start_diff_trees,
                    request_deserializer=outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.StartDiffTrees_Request.FromString,
                    response_serializer=outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.StartDiffTrees_Response.SerializeToString,
            ),
            'refresh_subtree': grpc.unary_unary_rpc_method_handler(
                    servicer.refresh_subtree,
                    request_deserializer=outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.RefreshSubtree_Request.FromString,
                    response_serializer=outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.Empty.SerializeToString,
            ),
            'refresh_subtree_stats': grpc.unary_unary_rpc_method_handler(
                    servicer.refresh_subtree_stats,
                    request_deserializer=outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.RefreshSubtreeStats_Request.FromString,
                    response_serializer=outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.Empty.SerializeToString,
            ),
            'get_next_uid': grpc.unary_unary_rpc_method_handler(
                    servicer.get_next_uid,
                    request_deserializer=outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.GetNextUid_Request.FromString,
                    response_serializer=outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.GetNextUid_Response.SerializeToString,
            ),
            'get_uid_for_local_path': grpc.unary_unary_rpc_method_handler(
                    servicer.get_uid_for_local_path,
                    request_deserializer=outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.GetUidForLocalPath_Request.FromString,
                    response_serializer=outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.GetUidForLocalPath_Response.SerializeToString,
            ),
            'drop_dragged_nodes': grpc.unary_unary_rpc_method_handler(
                    servicer.drop_dragged_nodes,
                    request_deserializer=outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.DragDrop_Request.FromString,
                    response_serializer=outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.DragDrop_Response.SerializeToString,
            ),
            'get_last_pending_op_for_node': grpc.unary_unary_rpc_method_handler(
                    servicer.get_last_pending_op_for_node,
                    request_deserializer=outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.GetLastPendingOp_Request.FromString,
                    response_serializer=outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.GetLastPendingOp_Response.SerializeToString,
            ),
            'download_file_from_gdrive': grpc.unary_unary_rpc_method_handler(
                    servicer.download_file_from_gdrive,
                    request_deserializer=outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.DownloadFromGDrive_Request.FromString,
                    response_serializer=outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.Empty.SerializeToString,
            ),
            'delete_subtree': grpc.unary_unary_rpc_method_handler(
                    servicer.delete_subtree,
                    request_deserializer=outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.DeleteSubtree_Request.FromString,
                    response_serializer=outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.Empty.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'outlet.daemon.grpc.Outlet', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))


 # This class is part of an EXPERIMENTAL API.
class Outlet(object):
    """Missing associated documentation comment in .proto file."""

    @staticmethod
    def subscribe_to_signals(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_stream(request, target, '/outlet.daemon.grpc.Outlet/subscribe_to_signals',
            outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.Subscribe_Request.SerializeToString,
            outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.SignalMsg.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def send_signal(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/outlet.daemon.grpc.Outlet/send_signal',
            outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.SignalMsg.SerializeToString,
            outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.SendSignalResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def get_node_for_uid(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/outlet.daemon.grpc.Outlet/get_node_for_uid',
            outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.GetNodeForUid_Request.SerializeToString,
            outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.SingleNode_Response.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def get_node_for_local_path(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/outlet.daemon.grpc.Outlet/get_node_for_local_path',
            outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.GetNodeForLocalPath_Request.SerializeToString,
            outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.SingleNode_Response.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def get_child_list_for_node(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/outlet.daemon.grpc.Outlet/get_child_list_for_node',
            outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.GetChildList_Request.SerializeToString,
            outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.GetChildList_Response.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def get_ancestor_list_for_spid(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/outlet.daemon.grpc.Outlet/get_ancestor_list_for_spid',
            outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.GetAncestorList_Request.SerializeToString,
            outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.GetAncestorList_Response.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def request_display_tree_ui_state(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/outlet.daemon.grpc.Outlet/request_display_tree_ui_state',
            outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.RequestDisplayTree_Request.SerializeToString,
            outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.RequestDisplayTree_Response.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def start_subtree_load(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/outlet.daemon.grpc.Outlet/start_subtree_load',
            outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.StartSubtreeLoad_Request.SerializeToString,
            outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.StartSubtreeLoad_Response.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def get_op_exec_play_state(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/outlet.daemon.grpc.Outlet/get_op_exec_play_state',
            outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.GetOpExecPlayState_Request.SerializeToString,
            outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.PlayState.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def start_diff_trees(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/outlet.daemon.grpc.Outlet/start_diff_trees',
            outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.StartDiffTrees_Request.SerializeToString,
            outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.StartDiffTrees_Response.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def refresh_subtree(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/outlet.daemon.grpc.Outlet/refresh_subtree',
            outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.RefreshSubtree_Request.SerializeToString,
            outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.Empty.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def refresh_subtree_stats(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/outlet.daemon.grpc.Outlet/refresh_subtree_stats',
            outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.RefreshSubtreeStats_Request.SerializeToString,
            outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.Empty.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def get_next_uid(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/outlet.daemon.grpc.Outlet/get_next_uid',
            outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.GetNextUid_Request.SerializeToString,
            outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.GetNextUid_Response.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def get_uid_for_local_path(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/outlet.daemon.grpc.Outlet/get_uid_for_local_path',
            outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.GetUidForLocalPath_Request.SerializeToString,
            outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.GetUidForLocalPath_Response.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def drop_dragged_nodes(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/outlet.daemon.grpc.Outlet/drop_dragged_nodes',
            outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.DragDrop_Request.SerializeToString,
            outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.DragDrop_Response.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def get_last_pending_op_for_node(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/outlet.daemon.grpc.Outlet/get_last_pending_op_for_node',
            outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.GetLastPendingOp_Request.SerializeToString,
            outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.GetLastPendingOp_Response.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def download_file_from_gdrive(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/outlet.daemon.grpc.Outlet/download_file_from_gdrive',
            outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.DownloadFromGDrive_Request.SerializeToString,
            outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.Empty.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def delete_subtree(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/outlet.daemon.grpc.Outlet/delete_subtree',
            outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.DeleteSubtree_Request.SerializeToString,
            outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.Empty.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)
