# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: outlet/daemon/grpc/Outlet.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from outlet.daemon.grpc import Node_pb2 as outlet_dot_daemon_dot_grpc_dot_Node__pb2

from outlet.daemon.grpc.Node_pb2 import *

DESCRIPTOR = _descriptor.FileDescriptor(
  name='outlet/daemon/grpc/Outlet.proto',
  package='outlet.daemon.grpc',
  syntax='proto3',
  serialized_options=b'\n\023com.msvoboda.outletP\001Z\031msvoboda.com/outlet/proto',
  create_key=_descriptor._internal_create_key,
  serialized_pb=b'\n\x1foutlet/daemon/grpc/Outlet.proto\x12\x12outlet.daemon.grpc\x1a\x1doutlet/daemon/grpc/Node.proto\"\r\n\x0bPingRequest\"!\n\x0cPingResponse\x12\x11\n\ttimestamp\x18\x01 \x01(\x03\"=\n\x13SingleNode_Response\x12&\n\x04node\x18\x01 \x01(\x0b\x32\x18.outlet.daemon.grpc.Node\"E\n\x1dReadSingleNodeFromDiskRequest\x12\x11\n\tfull_path\x18\x01 \x01(\t\x12\x11\n\ttree_type\x18\x02 \x01(\x05\"P\n\x1eReadSingleNodeFromDiskResponse\x12.\n\x04node\x18\x01 \x01(\x0b\x32 .outlet.daemon.grpc.OptionalNode\"G\n\x1aGetUidForLocalPath_Request\x12\x11\n\tfull_path\x18\x01 \x01(\t\x12\x16\n\x0euid_suggestion\x18\x02 \x01(\x05\"*\n\x1bGetUidForLocalPath_Response\x12\x0b\n\x03uid\x18\x01 \x01(\x05\"7\n\x15GetNodeForUid_Request\x12\x0b\n\x03uid\x18\x01 \x01(\x05\x12\x11\n\ttree_type\x18\x02 \x01(\x05\x32\xc7\x03\n\x06Outlet\x12K\n\x04ping\x12\x1f.outlet.daemon.grpc.PingRequest\x1a .outlet.daemon.grpc.PingResponse\"\x00\x12\x8e\x01\n#read_single_node_from_disk_for_path\x12\x31.outlet.daemon.grpc.ReadSingleNodeFromDiskRequest\x1a\x32.outlet.daemon.grpc.ReadSingleNodeFromDiskResponse\"\x00\x12u\n\x10\x62uild_identifier\x12..outlet.daemon.grpc.GetUidForLocalPath_Request\x1a/.outlet.daemon.grpc.GetUidForLocalPath_Response\"\x00\x12h\n\x10get_node_for_uid\x12).outlet.daemon.grpc.GetNodeForUid_Request\x1a\'.outlet.daemon.grpc.SingleNode_Response\"\x00\x42\x32\n\x13\x63om.msvoboda.outletP\x01Z\x19msvoboda.com/outlet/protoP\x00\x62\x06proto3'
  ,
  dependencies=[outlet_dot_daemon_dot_grpc_dot_Node__pb2.DESCRIPTOR,],
  public_dependencies=[outlet_dot_daemon_dot_grpc_dot_Node__pb2.DESCRIPTOR,])




_PINGREQUEST = _descriptor.Descriptor(
  name='PingRequest',
  full_name='outlet.daemon.grpc.PingRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=86,
  serialized_end=99,
)


_PINGRESPONSE = _descriptor.Descriptor(
  name='PingResponse',
  full_name='outlet.daemon.grpc.PingResponse',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='timestamp', full_name='outlet.daemon.grpc.PingResponse.timestamp', index=0,
      number=1, type=3, cpp_type=2, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=101,
  serialized_end=134,
)


_SINGLENODE_RESPONSE = _descriptor.Descriptor(
  name='SingleNode_Response',
  full_name='outlet.daemon.grpc.SingleNode_Response',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='node', full_name='outlet.daemon.grpc.SingleNode_Response.node', index=0,
      number=1, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=136,
  serialized_end=197,
)


_READSINGLENODEFROMDISKREQUEST = _descriptor.Descriptor(
  name='ReadSingleNodeFromDiskRequest',
  full_name='outlet.daemon.grpc.ReadSingleNodeFromDiskRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='full_path', full_name='outlet.daemon.grpc.ReadSingleNodeFromDiskRequest.full_path', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='tree_type', full_name='outlet.daemon.grpc.ReadSingleNodeFromDiskRequest.tree_type', index=1,
      number=2, type=5, cpp_type=1, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=199,
  serialized_end=268,
)


_READSINGLENODEFROMDISKRESPONSE = _descriptor.Descriptor(
  name='ReadSingleNodeFromDiskResponse',
  full_name='outlet.daemon.grpc.ReadSingleNodeFromDiskResponse',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='node', full_name='outlet.daemon.grpc.ReadSingleNodeFromDiskResponse.node', index=0,
      number=1, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=270,
  serialized_end=350,
)


_GETUIDFORLOCALPATH_REQUEST = _descriptor.Descriptor(
  name='GetUidForLocalPath_Request',
  full_name='outlet.daemon.grpc.GetUidForLocalPath_Request',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='full_path', full_name='outlet.daemon.grpc.GetUidForLocalPath_Request.full_path', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='uid_suggestion', full_name='outlet.daemon.grpc.GetUidForLocalPath_Request.uid_suggestion', index=1,
      number=2, type=5, cpp_type=1, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=352,
  serialized_end=423,
)


_GETUIDFORLOCALPATH_RESPONSE = _descriptor.Descriptor(
  name='GetUidForLocalPath_Response',
  full_name='outlet.daemon.grpc.GetUidForLocalPath_Response',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='uid', full_name='outlet.daemon.grpc.GetUidForLocalPath_Response.uid', index=0,
      number=1, type=5, cpp_type=1, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=425,
  serialized_end=467,
)


_GETNODEFORUID_REQUEST = _descriptor.Descriptor(
  name='GetNodeForUid_Request',
  full_name='outlet.daemon.grpc.GetNodeForUid_Request',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='uid', full_name='outlet.daemon.grpc.GetNodeForUid_Request.uid', index=0,
      number=1, type=5, cpp_type=1, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='tree_type', full_name='outlet.daemon.grpc.GetNodeForUid_Request.tree_type', index=1,
      number=2, type=5, cpp_type=1, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=469,
  serialized_end=524,
)

_SINGLENODE_RESPONSE.fields_by_name['node'].message_type = outlet_dot_daemon_dot_grpc_dot_Node__pb2._NODE
_READSINGLENODEFROMDISKRESPONSE.fields_by_name['node'].message_type = outlet_dot_daemon_dot_grpc_dot_Node__pb2._OPTIONALNODE
DESCRIPTOR.message_types_by_name['PingRequest'] = _PINGREQUEST
DESCRIPTOR.message_types_by_name['PingResponse'] = _PINGRESPONSE
DESCRIPTOR.message_types_by_name['SingleNode_Response'] = _SINGLENODE_RESPONSE
DESCRIPTOR.message_types_by_name['ReadSingleNodeFromDiskRequest'] = _READSINGLENODEFROMDISKREQUEST
DESCRIPTOR.message_types_by_name['ReadSingleNodeFromDiskResponse'] = _READSINGLENODEFROMDISKRESPONSE
DESCRIPTOR.message_types_by_name['GetUidForLocalPath_Request'] = _GETUIDFORLOCALPATH_REQUEST
DESCRIPTOR.message_types_by_name['GetUidForLocalPath_Response'] = _GETUIDFORLOCALPATH_RESPONSE
DESCRIPTOR.message_types_by_name['GetNodeForUid_Request'] = _GETNODEFORUID_REQUEST
_sym_db.RegisterFileDescriptor(DESCRIPTOR)

PingRequest = _reflection.GeneratedProtocolMessageType('PingRequest', (_message.Message,), {
  'DESCRIPTOR' : _PINGREQUEST,
  '__module__' : 'outlet.daemon.grpc.Outlet_pb2'
  # @@protoc_insertion_point(class_scope:outlet.daemon.grpc.PingRequest)
  })
_sym_db.RegisterMessage(PingRequest)

PingResponse = _reflection.GeneratedProtocolMessageType('PingResponse', (_message.Message,), {
  'DESCRIPTOR' : _PINGRESPONSE,
  '__module__' : 'outlet.daemon.grpc.Outlet_pb2'
  # @@protoc_insertion_point(class_scope:outlet.daemon.grpc.PingResponse)
  })
_sym_db.RegisterMessage(PingResponse)

SingleNode_Response = _reflection.GeneratedProtocolMessageType('SingleNode_Response', (_message.Message,), {
  'DESCRIPTOR' : _SINGLENODE_RESPONSE,
  '__module__' : 'outlet.daemon.grpc.Outlet_pb2'
  # @@protoc_insertion_point(class_scope:outlet.daemon.grpc.SingleNode_Response)
  })
_sym_db.RegisterMessage(SingleNode_Response)

ReadSingleNodeFromDiskRequest = _reflection.GeneratedProtocolMessageType('ReadSingleNodeFromDiskRequest', (_message.Message,), {
  'DESCRIPTOR' : _READSINGLENODEFROMDISKREQUEST,
  '__module__' : 'outlet.daemon.grpc.Outlet_pb2'
  # @@protoc_insertion_point(class_scope:outlet.daemon.grpc.ReadSingleNodeFromDiskRequest)
  })
_sym_db.RegisterMessage(ReadSingleNodeFromDiskRequest)

ReadSingleNodeFromDiskResponse = _reflection.GeneratedProtocolMessageType('ReadSingleNodeFromDiskResponse', (_message.Message,), {
  'DESCRIPTOR' : _READSINGLENODEFROMDISKRESPONSE,
  '__module__' : 'outlet.daemon.grpc.Outlet_pb2'
  # @@protoc_insertion_point(class_scope:outlet.daemon.grpc.ReadSingleNodeFromDiskResponse)
  })
_sym_db.RegisterMessage(ReadSingleNodeFromDiskResponse)

GetUidForLocalPath_Request = _reflection.GeneratedProtocolMessageType('GetUidForLocalPath_Request', (_message.Message,), {
  'DESCRIPTOR' : _GETUIDFORLOCALPATH_REQUEST,
  '__module__' : 'outlet.daemon.grpc.Outlet_pb2'
  # @@protoc_insertion_point(class_scope:outlet.daemon.grpc.GetUidForLocalPath_Request)
  })
_sym_db.RegisterMessage(GetUidForLocalPath_Request)

GetUidForLocalPath_Response = _reflection.GeneratedProtocolMessageType('GetUidForLocalPath_Response', (_message.Message,), {
  'DESCRIPTOR' : _GETUIDFORLOCALPATH_RESPONSE,
  '__module__' : 'outlet.daemon.grpc.Outlet_pb2'
  # @@protoc_insertion_point(class_scope:outlet.daemon.grpc.GetUidForLocalPath_Response)
  })
_sym_db.RegisterMessage(GetUidForLocalPath_Response)

GetNodeForUid_Request = _reflection.GeneratedProtocolMessageType('GetNodeForUid_Request', (_message.Message,), {
  'DESCRIPTOR' : _GETNODEFORUID_REQUEST,
  '__module__' : 'outlet.daemon.grpc.Outlet_pb2'
  # @@protoc_insertion_point(class_scope:outlet.daemon.grpc.GetNodeForUid_Request)
  })
_sym_db.RegisterMessage(GetNodeForUid_Request)


DESCRIPTOR._options = None

_OUTLET = _descriptor.ServiceDescriptor(
  name='Outlet',
  full_name='outlet.daemon.grpc.Outlet',
  file=DESCRIPTOR,
  index=0,
  serialized_options=None,
  create_key=_descriptor._internal_create_key,
  serialized_start=527,
  serialized_end=982,
  methods=[
  _descriptor.MethodDescriptor(
    name='ping',
    full_name='outlet.daemon.grpc.Outlet.ping',
    index=0,
    containing_service=None,
    input_type=_PINGREQUEST,
    output_type=_PINGRESPONSE,
    serialized_options=None,
    create_key=_descriptor._internal_create_key,
  ),
  _descriptor.MethodDescriptor(
    name='read_single_node_from_disk_for_path',
    full_name='outlet.daemon.grpc.Outlet.read_single_node_from_disk_for_path',
    index=1,
    containing_service=None,
    input_type=_READSINGLENODEFROMDISKREQUEST,
    output_type=_READSINGLENODEFROMDISKRESPONSE,
    serialized_options=None,
    create_key=_descriptor._internal_create_key,
  ),
  _descriptor.MethodDescriptor(
    name='build_identifier',
    full_name='outlet.daemon.grpc.Outlet.build_identifier',
    index=2,
    containing_service=None,
    input_type=_GETUIDFORLOCALPATH_REQUEST,
    output_type=_GETUIDFORLOCALPATH_RESPONSE,
    serialized_options=None,
    create_key=_descriptor._internal_create_key,
  ),
  _descriptor.MethodDescriptor(
    name='get_node_for_uid',
    full_name='outlet.daemon.grpc.Outlet.get_node_for_uid',
    index=3,
    containing_service=None,
    input_type=_GETNODEFORUID_REQUEST,
    output_type=_SINGLENODE_RESPONSE,
    serialized_options=None,
    create_key=_descriptor._internal_create_key,
  ),
])
_sym_db.RegisterServiceDescriptor(_OUTLET)

DESCRIPTOR.services_by_name['Outlet'] = _OUTLET

# @@protoc_insertion_point(module_scope)
