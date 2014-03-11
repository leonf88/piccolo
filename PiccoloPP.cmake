function(PICCOLO_PP)
  if(NOT ARGN)
    message(SEND_ERROR "Error: PICCOLO_PP() called without any proto files")
    return()
  endif(NOT ARGN)
  
  set(SRCS)
  
  foreach(FIL ${ARGN})
    get_filename_component(ABS_FIL ${FIL} ABSOLUTE)
    get_filename_component(FIL_WE ${FIL} NAME_WE)
    
    list(APPEND ${SRCS} "${CMAKE_CURRENT_BINARY_DIR}/${FIL_WE}.pp.cc")

    add_custom_command(
      OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/${FIL_WE}.pp.cc"
      COMMAND "${PICCOLO_SOURCE_DIR}/util/ppp.py"
      ARGS ${ABS_FIL} "${CMAKE_CURRENT_BINARY_DIR}/${FIL_WE}.pp.cc"
      DEPENDS ${ABS_FIL} "${PICCOLO_SOURCE_DIR}/util/ppp.py" 
      COMMENT "Piccolo Pre-Processor running on ${FIL}"
      VERBATIM )
  endforeach()

  set_source_files_properties(${${SRCS}} PROPERTIES GENERATED TRUE)
  set(${SRCS} ${${SRCS}} PARENT_SCOPE)
endfunction()
