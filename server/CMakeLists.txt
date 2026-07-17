add_executable(mawmaw main.cpp)

set_target_properties(mawmaw PROPERTIES ENABLE_EXPORTS ON)

target_link_libraries(mawmaw PRIVATE
    mawmaw_config
    mawmaw_core
    mawmaw_ingestor
    mawmaw_executor
    mawmaw_publisher
    m3
    Python3::Python
)
# Copy scripts directory to build dir
add_custom_command(TARGET mawmaw POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_directory
        ${CMAKE_SOURCE_DIR}/examples/scripts $<TARGET_FILE_DIR:mawmaw>/scripts
    COMMENT "Copying scripts to build dir"
)
# Copy default config to build dir
add_custom_command(TARGET mawmaw POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_if_different
        ${CMAKE_SOURCE_DIR}/examples/mawmaw.conf $<TARGET_FILE_DIR:mawmaw>/mawmaw.conf
    COMMENT "Copying mawmaw.conf to build dir"
)
