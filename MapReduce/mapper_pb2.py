# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: mapper.proto
# Protobuf Python Version: 4.25.0
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x0cmapper.proto\x12\tMapReduce\"5\n\nMapRequest\x12\x12\n\ninput_file\x18\x01 \x01(\t\x12\x13\n\x0boutput_file\x18\x02 \x01(\t\".\n\x0bMapResponse\x12\x0f\n\x07message\x18\x01 \x01(\t\x12\x0e\n\x06status\x18\x02 \x01(\x08\x32\x45\n\rMapperService\x12\x34\n\x03Map\x12\x15.MapReduce.MapRequest\x1a\x16.MapReduce.MapResponseb\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'mapper_pb2', _globals)
if _descriptor._USE_C_DESCRIPTORS == False:
  DESCRIPTOR._options = None
  _globals['_MAPREQUEST']._serialized_start=27
  _globals['_MAPREQUEST']._serialized_end=80
  _globals['_MAPRESPONSE']._serialized_start=82
  _globals['_MAPRESPONSE']._serialized_end=128
  _globals['_MAPPERSERVICE']._serialized_start=130
  _globals['_MAPPERSERVICE']._serialized_end=199
# @@protoc_insertion_point(module_scope)
