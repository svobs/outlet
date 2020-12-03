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
  serialized_pb=b'\n\x1foutlet/daemon/grpc/Outlet.proto\x12\x12outlet.daemon.grpc\x1a\x1doutlet/daemon/grpc/Node.proto\"\x07\n\x05\x45mpty\"\r\n\x0bPingRequest\"!\n\x0cPingResponse\x12\x11\n\ttimestamp\x18\x01 \x01(\x03\"\x13\n\x11Subscribe_Request\"\xb6\x01\n\x06Signal\x12\x13\n\x0bsignal_name\x18\x01 \x01(\t\x12\x13\n\x0bsender_name\x18\x02 \x01(\t\x12*\n\x05\x65mpty\x18\n \x01(\x0b\x32\x19.outlet.daemon.grpc.EmptyH\x00\x12G\n\x15\x64isplay_tree_ui_state\x18\x0b \x01(\x0b\x32&.outlet.daemon.grpc.DisplayTreeUiStateH\x00\x42\r\n\x0bsignal_data\"+\n\x18StartSubtreeLoad_Request\x12\x0f\n\x07tree_id\x18\x01 \x01(\t\"\x1b\n\x19StartSubtreeLoad_Response\"\xa0\x01\n\x12\x44isplayTreeUiState\x12\x0f\n\x07tree_id\x18\x01 \x01(\t\x12\x31\n\x07root_sn\x18\x02 \x01(\x0b\x32 .outlet.daemon.grpc.SPIDNodePair\x12\x13\n\x0broot_exists\x18\x03 \x01(\x08\x12\x16\n\x0eoffending_path\x18\x04 \x01(\t\x12\x19\n\x11needs_manual_load\x18\x05 \x01(\x08\"\x86\x01\n\x1aRequestDisplayTree_Request\x12\x12\n\nis_startup\x18\x01 \x01(\x08\x12\x0f\n\x07tree_id\x18\x02 \x01(\t\x12\x11\n\tuser_path\x18\x03 \x01(\t\x12\x30\n\x04spid\x18\x04 \x01(\x0b\x32\".outlet.daemon.grpc.NodeIdentifier\"d\n\x1bRequestDisplayTree_Response\x12\x45\n\x15\x64isplay_tree_ui_state\x18\x01 \x01(\x0b\x32&.outlet.daemon.grpc.DisplayTreeUiState\"=\n\x13SingleNode_Response\x12&\n\x04node\x18\x01 \x01(\x0b\x32\x18.outlet.daemon.grpc.Node\"G\n\x1aGetUidForLocalPath_Request\x12\x11\n\tfull_path\x18\x01 \x01(\t\x12\x16\n\x0euid_suggestion\x18\x02 \x01(\x05\"*\n\x1bGetUidForLocalPath_Response\x12\x0b\n\x03uid\x18\x01 \x01(\x05\"7\n\x15GetNodeForUid_Request\x12\x0b\n\x03uid\x18\x01 \x01(\x05\x12\x11\n\ttree_type\x18\x02 \x01(\x05\"0\n\x1bGetNodeForLocalPath_Request\x12\x11\n\tfull_path\x18\x01 \x01(\t\"\x14\n\x12GetNextUid_Request\"\"\n\x13GetNextUid_Response\x12\x0b\n\x03uid\x18\x01 \x01(\x05\"\r\n\x0bLoadNewTree2\xed\x06\n\x06Outlet\x12[\n\x14subscribe_to_signals\x12%.outlet.daemon.grpc.Subscribe_Request\x1a\x1a.outlet.daemon.grpc.Signal0\x01\x12K\n\x04ping\x12\x1f.outlet.daemon.grpc.PingRequest\x1a .outlet.daemon.grpc.PingResponse\"\x00\x12h\n\x10get_node_for_uid\x12).outlet.daemon.grpc.GetNodeForUid_Request\x1a\'.outlet.daemon.grpc.SingleNode_Response\"\x00\x12u\n\x17get_node_for_local_path\x12/.outlet.daemon.grpc.GetNodeForLocalPath_Request\x1a\'.outlet.daemon.grpc.SingleNode_Response\"\x00\x12\x82\x01\n\x1drequest_display_tree_ui_state\x12..outlet.daemon.grpc.RequestDisplayTree_Request\x1a/.outlet.daemon.grpc.RequestDisplayTree_Response\"\x00\x12s\n\x12start_subtree_load\x12,.outlet.daemon.grpc.StartSubtreeLoad_Request\x1a-.outlet.daemon.grpc.StartSubtreeLoad_Response\"\x00\x12\x61\n\x0cget_next_uid\x12&.outlet.daemon.grpc.GetNextUid_Request\x1a\'.outlet.daemon.grpc.GetNextUid_Response\"\x00\x12{\n\x16get_uid_for_local_path\x12..outlet.daemon.grpc.GetUidForLocalPath_Request\x1a/.outlet.daemon.grpc.GetUidForLocalPath_Response\"\x00\x42\x32\n\x13\x63om.msvoboda.outletP\x01Z\x19msvoboda.com/outlet/protoP\x00\x62\x06proto3'
  ,
  dependencies=[outlet_dot_daemon_dot_grpc_dot_Node__pb2.DESCRIPTOR,],
  public_dependencies=[outlet_dot_daemon_dot_grpc_dot_Node__pb2.DESCRIPTOR,])




_EMPTY = _descriptor.Descriptor(
  name='Empty',
  full_name='outlet.daemon.grpc.Empty',
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
  serialized_end=93,
)


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
  serialized_start=95,
  serialized_end=108,
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
  serialized_start=110,
  serialized_end=143,
)


_SUBSCRIBE_REQUEST = _descriptor.Descriptor(
  name='Subscribe_Request',
  full_name='outlet.daemon.grpc.Subscribe_Request',
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
  serialized_start=145,
  serialized_end=164,
)


_SIGNAL = _descriptor.Descriptor(
  name='Signal',
  full_name='outlet.daemon.grpc.Signal',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='signal_name', full_name='outlet.daemon.grpc.Signal.signal_name', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='sender_name', full_name='outlet.daemon.grpc.Signal.sender_name', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='empty', full_name='outlet.daemon.grpc.Signal.empty', index=2,
      number=10, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='display_tree_ui_state', full_name='outlet.daemon.grpc.Signal.display_tree_ui_state', index=3,
      number=11, type=11, cpp_type=10, label=1,
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
    _descriptor.OneofDescriptor(
      name='signal_data', full_name='outlet.daemon.grpc.Signal.signal_data',
      index=0, containing_type=None,
      create_key=_descriptor._internal_create_key,
    fields=[]),
  ],
  serialized_start=167,
  serialized_end=349,
)


_STARTSUBTREELOAD_REQUEST = _descriptor.Descriptor(
  name='StartSubtreeLoad_Request',
  full_name='outlet.daemon.grpc.StartSubtreeLoad_Request',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='tree_id', full_name='outlet.daemon.grpc.StartSubtreeLoad_Request.tree_id', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
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
  serialized_start=351,
  serialized_end=394,
)


_STARTSUBTREELOAD_RESPONSE = _descriptor.Descriptor(
  name='StartSubtreeLoad_Response',
  full_name='outlet.daemon.grpc.StartSubtreeLoad_Response',
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
  serialized_start=396,
  serialized_end=423,
)


_DISPLAYTREEUISTATE = _descriptor.Descriptor(
  name='DisplayTreeUiState',
  full_name='outlet.daemon.grpc.DisplayTreeUiState',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='tree_id', full_name='outlet.daemon.grpc.DisplayTreeUiState.tree_id', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='root_sn', full_name='outlet.daemon.grpc.DisplayTreeUiState.root_sn', index=1,
      number=2, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='root_exists', full_name='outlet.daemon.grpc.DisplayTreeUiState.root_exists', index=2,
      number=3, type=8, cpp_type=7, label=1,
      has_default_value=False, default_value=False,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='offending_path', full_name='outlet.daemon.grpc.DisplayTreeUiState.offending_path', index=3,
      number=4, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='needs_manual_load', full_name='outlet.daemon.grpc.DisplayTreeUiState.needs_manual_load', index=4,
      number=5, type=8, cpp_type=7, label=1,
      has_default_value=False, default_value=False,
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
  serialized_start=426,
  serialized_end=586,
)


_REQUESTDISPLAYTREE_REQUEST = _descriptor.Descriptor(
  name='RequestDisplayTree_Request',
  full_name='outlet.daemon.grpc.RequestDisplayTree_Request',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='is_startup', full_name='outlet.daemon.grpc.RequestDisplayTree_Request.is_startup', index=0,
      number=1, type=8, cpp_type=7, label=1,
      has_default_value=False, default_value=False,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='tree_id', full_name='outlet.daemon.grpc.RequestDisplayTree_Request.tree_id', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='user_path', full_name='outlet.daemon.grpc.RequestDisplayTree_Request.user_path', index=2,
      number=3, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='spid', full_name='outlet.daemon.grpc.RequestDisplayTree_Request.spid', index=3,
      number=4, type=11, cpp_type=10, label=1,
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
  serialized_start=589,
  serialized_end=723,
)


_REQUESTDISPLAYTREE_RESPONSE = _descriptor.Descriptor(
  name='RequestDisplayTree_Response',
  full_name='outlet.daemon.grpc.RequestDisplayTree_Response',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='display_tree_ui_state', full_name='outlet.daemon.grpc.RequestDisplayTree_Response.display_tree_ui_state', index=0,
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
  serialized_start=725,
  serialized_end=825,
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
  serialized_start=827,
  serialized_end=888,
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
  serialized_start=890,
  serialized_end=961,
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
  serialized_start=963,
  serialized_end=1005,
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
  serialized_start=1007,
  serialized_end=1062,
)


_GETNODEFORLOCALPATH_REQUEST = _descriptor.Descriptor(
  name='GetNodeForLocalPath_Request',
  full_name='outlet.daemon.grpc.GetNodeForLocalPath_Request',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='full_path', full_name='outlet.daemon.grpc.GetNodeForLocalPath_Request.full_path', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
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
  serialized_start=1064,
  serialized_end=1112,
)


_GETNEXTUID_REQUEST = _descriptor.Descriptor(
  name='GetNextUid_Request',
  full_name='outlet.daemon.grpc.GetNextUid_Request',
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
  serialized_start=1114,
  serialized_end=1134,
)


_GETNEXTUID_RESPONSE = _descriptor.Descriptor(
  name='GetNextUid_Response',
  full_name='outlet.daemon.grpc.GetNextUid_Response',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='uid', full_name='outlet.daemon.grpc.GetNextUid_Response.uid', index=0,
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
  serialized_start=1136,
  serialized_end=1170,
)


_LOADNEWTREE = _descriptor.Descriptor(
  name='LoadNewTree',
  full_name='outlet.daemon.grpc.LoadNewTree',
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
  serialized_start=1172,
  serialized_end=1185,
)

_SIGNAL.fields_by_name['empty'].message_type = _EMPTY
_SIGNAL.fields_by_name['display_tree_ui_state'].message_type = _DISPLAYTREEUISTATE
_SIGNAL.oneofs_by_name['signal_data'].fields.append(
  _SIGNAL.fields_by_name['empty'])
_SIGNAL.fields_by_name['empty'].containing_oneof = _SIGNAL.oneofs_by_name['signal_data']
_SIGNAL.oneofs_by_name['signal_data'].fields.append(
  _SIGNAL.fields_by_name['display_tree_ui_state'])
_SIGNAL.fields_by_name['display_tree_ui_state'].containing_oneof = _SIGNAL.oneofs_by_name['signal_data']
_DISPLAYTREEUISTATE.fields_by_name['root_sn'].message_type = outlet_dot_daemon_dot_grpc_dot_Node__pb2._SPIDNODEPAIR
_REQUESTDISPLAYTREE_REQUEST.fields_by_name['spid'].message_type = outlet_dot_daemon_dot_grpc_dot_Node__pb2._NODEIDENTIFIER
_REQUESTDISPLAYTREE_RESPONSE.fields_by_name['display_tree_ui_state'].message_type = _DISPLAYTREEUISTATE
_SINGLENODE_RESPONSE.fields_by_name['node'].message_type = outlet_dot_daemon_dot_grpc_dot_Node__pb2._NODE
DESCRIPTOR.message_types_by_name['Empty'] = _EMPTY
DESCRIPTOR.message_types_by_name['PingRequest'] = _PINGREQUEST
DESCRIPTOR.message_types_by_name['PingResponse'] = _PINGRESPONSE
DESCRIPTOR.message_types_by_name['Subscribe_Request'] = _SUBSCRIBE_REQUEST
DESCRIPTOR.message_types_by_name['Signal'] = _SIGNAL
DESCRIPTOR.message_types_by_name['StartSubtreeLoad_Request'] = _STARTSUBTREELOAD_REQUEST
DESCRIPTOR.message_types_by_name['StartSubtreeLoad_Response'] = _STARTSUBTREELOAD_RESPONSE
DESCRIPTOR.message_types_by_name['DisplayTreeUiState'] = _DISPLAYTREEUISTATE
DESCRIPTOR.message_types_by_name['RequestDisplayTree_Request'] = _REQUESTDISPLAYTREE_REQUEST
DESCRIPTOR.message_types_by_name['RequestDisplayTree_Response'] = _REQUESTDISPLAYTREE_RESPONSE
DESCRIPTOR.message_types_by_name['SingleNode_Response'] = _SINGLENODE_RESPONSE
DESCRIPTOR.message_types_by_name['GetUidForLocalPath_Request'] = _GETUIDFORLOCALPATH_REQUEST
DESCRIPTOR.message_types_by_name['GetUidForLocalPath_Response'] = _GETUIDFORLOCALPATH_RESPONSE
DESCRIPTOR.message_types_by_name['GetNodeForUid_Request'] = _GETNODEFORUID_REQUEST
DESCRIPTOR.message_types_by_name['GetNodeForLocalPath_Request'] = _GETNODEFORLOCALPATH_REQUEST
DESCRIPTOR.message_types_by_name['GetNextUid_Request'] = _GETNEXTUID_REQUEST
DESCRIPTOR.message_types_by_name['GetNextUid_Response'] = _GETNEXTUID_RESPONSE
DESCRIPTOR.message_types_by_name['LoadNewTree'] = _LOADNEWTREE
_sym_db.RegisterFileDescriptor(DESCRIPTOR)

Empty = _reflection.GeneratedProtocolMessageType('Empty', (_message.Message,), {
  'DESCRIPTOR' : _EMPTY,
  '__module__' : 'outlet.daemon.grpc.Outlet_pb2'
  # @@protoc_insertion_point(class_scope:outlet.daemon.grpc.Empty)
  })
_sym_db.RegisterMessage(Empty)

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

Subscribe_Request = _reflection.GeneratedProtocolMessageType('Subscribe_Request', (_message.Message,), {
  'DESCRIPTOR' : _SUBSCRIBE_REQUEST,
  '__module__' : 'outlet.daemon.grpc.Outlet_pb2'
  # @@protoc_insertion_point(class_scope:outlet.daemon.grpc.Subscribe_Request)
  })
_sym_db.RegisterMessage(Subscribe_Request)

Signal = _reflection.GeneratedProtocolMessageType('Signal', (_message.Message,), {
  'DESCRIPTOR' : _SIGNAL,
  '__module__' : 'outlet.daemon.grpc.Outlet_pb2'
  # @@protoc_insertion_point(class_scope:outlet.daemon.grpc.Signal)
  })
_sym_db.RegisterMessage(Signal)

StartSubtreeLoad_Request = _reflection.GeneratedProtocolMessageType('StartSubtreeLoad_Request', (_message.Message,), {
  'DESCRIPTOR' : _STARTSUBTREELOAD_REQUEST,
  '__module__' : 'outlet.daemon.grpc.Outlet_pb2'
  # @@protoc_insertion_point(class_scope:outlet.daemon.grpc.StartSubtreeLoad_Request)
  })
_sym_db.RegisterMessage(StartSubtreeLoad_Request)

StartSubtreeLoad_Response = _reflection.GeneratedProtocolMessageType('StartSubtreeLoad_Response', (_message.Message,), {
  'DESCRIPTOR' : _STARTSUBTREELOAD_RESPONSE,
  '__module__' : 'outlet.daemon.grpc.Outlet_pb2'
  # @@protoc_insertion_point(class_scope:outlet.daemon.grpc.StartSubtreeLoad_Response)
  })
_sym_db.RegisterMessage(StartSubtreeLoad_Response)

DisplayTreeUiState = _reflection.GeneratedProtocolMessageType('DisplayTreeUiState', (_message.Message,), {
  'DESCRIPTOR' : _DISPLAYTREEUISTATE,
  '__module__' : 'outlet.daemon.grpc.Outlet_pb2'
  # @@protoc_insertion_point(class_scope:outlet.daemon.grpc.DisplayTreeUiState)
  })
_sym_db.RegisterMessage(DisplayTreeUiState)

RequestDisplayTree_Request = _reflection.GeneratedProtocolMessageType('RequestDisplayTree_Request', (_message.Message,), {
  'DESCRIPTOR' : _REQUESTDISPLAYTREE_REQUEST,
  '__module__' : 'outlet.daemon.grpc.Outlet_pb2'
  # @@protoc_insertion_point(class_scope:outlet.daemon.grpc.RequestDisplayTree_Request)
  })
_sym_db.RegisterMessage(RequestDisplayTree_Request)

RequestDisplayTree_Response = _reflection.GeneratedProtocolMessageType('RequestDisplayTree_Response', (_message.Message,), {
  'DESCRIPTOR' : _REQUESTDISPLAYTREE_RESPONSE,
  '__module__' : 'outlet.daemon.grpc.Outlet_pb2'
  # @@protoc_insertion_point(class_scope:outlet.daemon.grpc.RequestDisplayTree_Response)
  })
_sym_db.RegisterMessage(RequestDisplayTree_Response)

SingleNode_Response = _reflection.GeneratedProtocolMessageType('SingleNode_Response', (_message.Message,), {
  'DESCRIPTOR' : _SINGLENODE_RESPONSE,
  '__module__' : 'outlet.daemon.grpc.Outlet_pb2'
  # @@protoc_insertion_point(class_scope:outlet.daemon.grpc.SingleNode_Response)
  })
_sym_db.RegisterMessage(SingleNode_Response)

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

GetNodeForLocalPath_Request = _reflection.GeneratedProtocolMessageType('GetNodeForLocalPath_Request', (_message.Message,), {
  'DESCRIPTOR' : _GETNODEFORLOCALPATH_REQUEST,
  '__module__' : 'outlet.daemon.grpc.Outlet_pb2'
  # @@protoc_insertion_point(class_scope:outlet.daemon.grpc.GetNodeForLocalPath_Request)
  })
_sym_db.RegisterMessage(GetNodeForLocalPath_Request)

GetNextUid_Request = _reflection.GeneratedProtocolMessageType('GetNextUid_Request', (_message.Message,), {
  'DESCRIPTOR' : _GETNEXTUID_REQUEST,
  '__module__' : 'outlet.daemon.grpc.Outlet_pb2'
  # @@protoc_insertion_point(class_scope:outlet.daemon.grpc.GetNextUid_Request)
  })
_sym_db.RegisterMessage(GetNextUid_Request)

GetNextUid_Response = _reflection.GeneratedProtocolMessageType('GetNextUid_Response', (_message.Message,), {
  'DESCRIPTOR' : _GETNEXTUID_RESPONSE,
  '__module__' : 'outlet.daemon.grpc.Outlet_pb2'
  # @@protoc_insertion_point(class_scope:outlet.daemon.grpc.GetNextUid_Response)
  })
_sym_db.RegisterMessage(GetNextUid_Response)

LoadNewTree = _reflection.GeneratedProtocolMessageType('LoadNewTree', (_message.Message,), {
  'DESCRIPTOR' : _LOADNEWTREE,
  '__module__' : 'outlet.daemon.grpc.Outlet_pb2'
  # @@protoc_insertion_point(class_scope:outlet.daemon.grpc.LoadNewTree)
  })
_sym_db.RegisterMessage(LoadNewTree)


DESCRIPTOR._options = None

_OUTLET = _descriptor.ServiceDescriptor(
  name='Outlet',
  full_name='outlet.daemon.grpc.Outlet',
  file=DESCRIPTOR,
  index=0,
  serialized_options=None,
  create_key=_descriptor._internal_create_key,
  serialized_start=1188,
  serialized_end=2065,
  methods=[
  _descriptor.MethodDescriptor(
    name='subscribe_to_signals',
    full_name='outlet.daemon.grpc.Outlet.subscribe_to_signals',
    index=0,
    containing_service=None,
    input_type=_SUBSCRIBE_REQUEST,
    output_type=_SIGNAL,
    serialized_options=None,
    create_key=_descriptor._internal_create_key,
  ),
  _descriptor.MethodDescriptor(
    name='ping',
    full_name='outlet.daemon.grpc.Outlet.ping',
    index=1,
    containing_service=None,
    input_type=_PINGREQUEST,
    output_type=_PINGRESPONSE,
    serialized_options=None,
    create_key=_descriptor._internal_create_key,
  ),
  _descriptor.MethodDescriptor(
    name='get_node_for_uid',
    full_name='outlet.daemon.grpc.Outlet.get_node_for_uid',
    index=2,
    containing_service=None,
    input_type=_GETNODEFORUID_REQUEST,
    output_type=_SINGLENODE_RESPONSE,
    serialized_options=None,
    create_key=_descriptor._internal_create_key,
  ),
  _descriptor.MethodDescriptor(
    name='get_node_for_local_path',
    full_name='outlet.daemon.grpc.Outlet.get_node_for_local_path',
    index=3,
    containing_service=None,
    input_type=_GETNODEFORLOCALPATH_REQUEST,
    output_type=_SINGLENODE_RESPONSE,
    serialized_options=None,
    create_key=_descriptor._internal_create_key,
  ),
  _descriptor.MethodDescriptor(
    name='request_display_tree_ui_state',
    full_name='outlet.daemon.grpc.Outlet.request_display_tree_ui_state',
    index=4,
    containing_service=None,
    input_type=_REQUESTDISPLAYTREE_REQUEST,
    output_type=_REQUESTDISPLAYTREE_RESPONSE,
    serialized_options=None,
    create_key=_descriptor._internal_create_key,
  ),
  _descriptor.MethodDescriptor(
    name='start_subtree_load',
    full_name='outlet.daemon.grpc.Outlet.start_subtree_load',
    index=5,
    containing_service=None,
    input_type=_STARTSUBTREELOAD_REQUEST,
    output_type=_STARTSUBTREELOAD_RESPONSE,
    serialized_options=None,
    create_key=_descriptor._internal_create_key,
  ),
  _descriptor.MethodDescriptor(
    name='get_next_uid',
    full_name='outlet.daemon.grpc.Outlet.get_next_uid',
    index=6,
    containing_service=None,
    input_type=_GETNEXTUID_REQUEST,
    output_type=_GETNEXTUID_RESPONSE,
    serialized_options=None,
    create_key=_descriptor._internal_create_key,
  ),
  _descriptor.MethodDescriptor(
    name='get_uid_for_local_path',
    full_name='outlet.daemon.grpc.Outlet.get_uid_for_local_path',
    index=7,
    containing_service=None,
    input_type=_GETUIDFORLOCALPATH_REQUEST,
    output_type=_GETUIDFORLOCALPATH_RESPONSE,
    serialized_options=None,
    create_key=_descriptor._internal_create_key,
  ),
])
_sym_db.RegisterServiceDescriptor(_OUTLET)

DESCRIPTOR.services_by_name['Outlet'] = _OUTLET

# @@protoc_insertion_point(module_scope)
