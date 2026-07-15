add_library(mawmaw_ingestor INTERFACE)
target_include_directories(mawmaw_ingestor INTERFACE ${CMAKE_SOURCE_DIR})
target_link_libraries(mawmaw_ingestor INTERFACE mawmaw_core ${CMAKE_DL_LIBS})
