add_library(mawmaw_plugin_dummy SHARED dummy_ingestor.cpp)
target_include_directories(mawmaw_plugin_dummy PRIVATE ${CMAKE_SOURCE_DIR})
target_link_libraries(mawmaw_plugin_dummy PRIVATE mawmaw_ingestor)
set_target_properties(mawmaw_plugin_dummy PROPERTIES PREFIX "" OUTPUT_NAME "plugin_dummy")
set_target_properties(mawmaw_plugin_dummy PROPERTIES
    LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/server"
)