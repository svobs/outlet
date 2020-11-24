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
        self.ping = channel.unary_unary(
                '/outlet.daemon.grpc.Outlet/ping',
                request_serializer=outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.PingRequest.SerializeToString,
                response_deserializer=outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.PingResponse.FromString,
                )
        self.read_single_node_from_disk_for_path = channel.unary_unary(
                '/outlet.daemon.grpc.Outlet/read_single_node_from_disk_for_path',
                request_serializer=outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.ReadSingleNodeFromDiskRequest.SerializeToString,
                response_deserializer=outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.ReadSingleNodeFromDiskResponse.FromString,
                )


class OutletServicer(object):
    """Missing associated documentation comment in .proto file."""

    def ping(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def read_single_node_from_disk_for_path(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_OutletServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'ping': grpc.unary_unary_rpc_method_handler(
                    servicer.ping,
                    request_deserializer=outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.PingRequest.FromString,
                    response_serializer=outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.PingResponse.SerializeToString,
            ),
            'read_single_node_from_disk_for_path': grpc.unary_unary_rpc_method_handler(
                    servicer.read_single_node_from_disk_for_path,
                    request_deserializer=outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.ReadSingleNodeFromDiskRequest.FromString,
                    response_serializer=outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.ReadSingleNodeFromDiskResponse.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'outlet.daemon.grpc.Outlet', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))


 # This class is part of an EXPERIMENTAL API.
class Outlet(object):
    """Missing associated documentation comment in .proto file."""

    @staticmethod
    def ping(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/outlet.daemon.grpc.Outlet/ping',
            outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.PingRequest.SerializeToString,
            outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.PingResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def read_single_node_from_disk_for_path(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/outlet.daemon.grpc.Outlet/read_single_node_from_disk_for_path',
            outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.ReadSingleNodeFromDiskRequest.SerializeToString,
            outlet_dot_daemon_dot_grpc_dot_Outlet__pb2.ReadSingleNodeFromDiskResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)
