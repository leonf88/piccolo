set(CMAKE_CXX_FLAGS "-fPIC -march=core2 -Wall -Wno-sign-compare -Wno-unused-function -fno-omit-frame-pointer -ggdb1")

add_library(protobuf
            google/protobuf/extension_set.cc
            google/protobuf/wire_format.cc
            google/protobuf/text_format.cc
            google/protobuf/descriptor_database.cc
            google/protobuf/generated_message_reflection.cc
            google/protobuf/wire_format_lite.cc
            google/protobuf/service.cc
            google/protobuf/repeated_field.cc
            google/protobuf/stubs/strutil.cc
            google/protobuf/stubs/hash.cc
            google/protobuf/stubs/once.cc
            google/protobuf/stubs/common.cc
            google/protobuf/stubs/structurally_valid.cc
            google/protobuf/stubs/substitute.cc
            google/protobuf/message_lite.cc
            google/protobuf/descriptor.pb.cc
            google/protobuf/reflection_ops.cc
            google/protobuf/io/zero_copy_stream_impl_lite.cc
            google/protobuf/io/tokenizer.cc
            google/protobuf/io/zero_copy_stream.cc
            google/protobuf/io/zero_copy_stream_impl.cc
            google/protobuf/io/gzip_stream.cc
            google/protobuf/io/printer.cc
            google/protobuf/io/coded_stream.cc
            google/protobuf/descriptor.cc
            google/protobuf/message.cc
            google/protobuf/unknown_field_set.cc
            google/protobuf/extension_set_heavy.cc
            google/protobuf/dynamic_message.cc
            google/protobuf/generated_message_util.cc)

add_executable(protoc
               google/protobuf/compiler/parser.cc
               google/protobuf/compiler/java/java_file.cc
               google/protobuf/compiler/java/java_primitive_field.cc
               google/protobuf/compiler/java/java_field.cc
               google/protobuf/compiler/java/java_generator.cc
               google/protobuf/compiler/java/java_helpers.cc
               google/protobuf/compiler/java/java_enum_field.cc
               google/protobuf/compiler/java/java_service.cc
               google/protobuf/compiler/java/java_enum.cc
               google/protobuf/compiler/java/java_message_field.cc
               google/protobuf/compiler/java/java_extension.cc
               google/protobuf/compiler/java/java_message.cc
               google/protobuf/compiler/subprocess.cc
               google/protobuf/compiler/zip_writer.cc
               google/protobuf/compiler/importer.cc
               google/protobuf/compiler/main.cc
               google/protobuf/compiler/cpp/cpp_primitive_field.cc
               google/protobuf/compiler/cpp/cpp_message.cc
               google/protobuf/compiler/cpp/cpp_extension.cc
               google/protobuf/compiler/cpp/cpp_helpers.cc
               google/protobuf/compiler/cpp/cpp_string_field.cc
               google/protobuf/compiler/cpp/cpp_enum.cc
               google/protobuf/compiler/cpp/cpp_field.cc
               google/protobuf/compiler/cpp/cpp_enum_field.cc
               google/protobuf/compiler/cpp/cpp_service.cc
               google/protobuf/compiler/cpp/cpp_message_field.cc
               google/protobuf/compiler/cpp/cpp_generator.cc
               google/protobuf/compiler/cpp/cpp_file.cc
               google/protobuf/compiler/code_generator.cc
               google/protobuf/compiler/plugin.cc
               google/protobuf/compiler/python/python_generator.cc
               google/protobuf/compiler/plugin.pb.cc
               google/protobuf/compiler/command_line_interface.cc)

target_link_libraries(protoc protobuf pthread)

function(PROTOBUF_GENERATE_CPP SRCS HDRS)
  if(NOT ARGN)
    message(SEND_ERROR "Error: PROTOBUF_GENERATE_CPP() called without any proto files")
    return()
  endif(NOT ARGN)

  set(${SRCS})
  set(${HDRS})
  foreach(FIL ${ARGN})
    get_filename_component(ABS_FIL ${FIL} ABSOLUTE)
    get_filename_component(FIL_WE ${FIL} NAME_WE)

    list(APPEND ${SRCS} "${CMAKE_CURRENT_BINARY_DIR}/${FIL_WE}.pb.cc")
    list(APPEND ${HDRS} "${CMAKE_CURRENT_BINARY_DIR}/${FIL_WE}.pb.h")
    
    add_custom_command(
      OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/${FIL_WE}.pb.cc"
             "${CMAKE_CURRENT_BINARY_DIR}/${FIL_WE}.pb.h"
      COMMAND protoc
      ARGS --cpp_out  ${CMAKE_BINARY_DIR} --proto_path ${CMAKE_SOURCE_DIR} ${ABS_FIL}
      DEPENDS ${ABS_FIL}
      COMMENT "Running C++ protocol buffer compiler on ${FIL}"
      VERBATIM )
  endforeach()

  set_source_files_properties(${${SRCS}} ${${HDRS}} PROPERTIES GENERATED TRUE)
  set(${SRCS} ${${SRCS}} PARENT_SCOPE)
  set(${HDRS} ${${HDRS}} PARENT_SCOPE)
endfunction()

