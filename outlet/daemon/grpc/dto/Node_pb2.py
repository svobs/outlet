# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: outlet/daemon/grpc/dto/Node.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor.FileDescriptor(
  name='outlet/daemon/grpc/dto/Node.proto',
  package='outlet.daemon.grpc.dto',
  syntax='proto3',
  serialized_options=b'\n\027com.msvoboda.outlet.dtoP\001Z\035msvoboda.com/outlet/proto/dto',
  create_key=_descriptor._internal_create_key,
  serialized_pb=b'\n!outlet/daemon/grpc/dto/Node.proto\x12\x16outlet.daemon.grpc.dto\"T\n\x04Node\x12\x0b\n\x03uid\x18\x01 \x01(\x05\x12\x11\n\tpath_list\x18\x02 \x03(\t\x12\x0b\n\x03nid\x18\x03 \x01(\t\x12\x0f\n\x07trashed\x18\x04 \x01(\x05\x12\x0e\n\x06shared\x18\x05 \x01(\x08\"]\n\rContainerNode\x12\x0b\n\x03uid\x18\x01 \x01(\x05\x12\x11\n\tpath_list\x18\x02 \x03(\t\x12\x0b\n\x03nid\x18\x03 \x01(\t\x12\x0f\n\x07trashed\x18\x04 \x01(\x05\x12\x0e\n\x06shared\x18\x05 \x01(\x08\x42:\n\x17\x63om.msvoboda.outlet.dtoP\x01Z\x1dmsvoboda.com/outlet/proto/dtob\x06proto3'
)




_NODE = _descriptor.Descriptor(
  name='Node',
  full_name='outlet.daemon.grpc.dto.Node',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='uid', full_name='outlet.daemon.grpc.dto.Node.uid', index=0,
      number=1, type=5, cpp_type=1, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='path_list', full_name='outlet.daemon.grpc.dto.Node.path_list', index=1,
      number=2, type=9, cpp_type=9, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='nid', full_name='outlet.daemon.grpc.dto.Node.nid', index=2,
      number=3, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='trashed', full_name='outlet.daemon.grpc.dto.Node.trashed', index=3,
      number=4, type=5, cpp_type=1, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='shared', full_name='outlet.daemon.grpc.dto.Node.shared', index=4,
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
  serialized_start=61,
  serialized_end=145,
)


_CONTAINERNODE = _descriptor.Descriptor(
  name='ContainerNode',
  full_name='outlet.daemon.grpc.dto.ContainerNode',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='uid', full_name='outlet.daemon.grpc.dto.ContainerNode.uid', index=0,
      number=1, type=5, cpp_type=1, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='path_list', full_name='outlet.daemon.grpc.dto.ContainerNode.path_list', index=1,
      number=2, type=9, cpp_type=9, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='nid', full_name='outlet.daemon.grpc.dto.ContainerNode.nid', index=2,
      number=3, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='trashed', full_name='outlet.daemon.grpc.dto.ContainerNode.trashed', index=3,
      number=4, type=5, cpp_type=1, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='shared', full_name='outlet.daemon.grpc.dto.ContainerNode.shared', index=4,
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
  serialized_start=147,
  serialized_end=240,
)

DESCRIPTOR.message_types_by_name['Node'] = _NODE
DESCRIPTOR.message_types_by_name['ContainerNode'] = _CONTAINERNODE
_sym_db.RegisterFileDescriptor(DESCRIPTOR)

Node = _reflection.GeneratedProtocolMessageType('Node', (_message.Message,), {
  'DESCRIPTOR' : _NODE,
  '__module__' : 'outlet.daemon.grpc.dto.Node_pb2'
  # @@protoc_insertion_point(class_scope:outlet.daemon.grpc.dto.Node)
  })
_sym_db.RegisterMessage(Node)

ContainerNode = _reflection.GeneratedProtocolMessageType('ContainerNode', (_message.Message,), {
  'DESCRIPTOR' : _CONTAINERNODE,
  '__module__' : 'outlet.daemon.grpc.dto.Node_pb2'
  # @@protoc_insertion_point(class_scope:outlet.daemon.grpc.dto.ContainerNode)
  })
_sym_db.RegisterMessage(ContainerNode)


DESCRIPTOR._options = None
# @@protoc_insertion_point(module_scope)
