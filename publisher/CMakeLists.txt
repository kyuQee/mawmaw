find_package(Threads REQUIRED)

add_library(mawmaw_publisher INTERFACE)
target_include_directories(mawmaw_publisher INTERFACE ${CMAKE_SOURCE_DIR})
target_link_libraries(mawmaw_publisher INTERFACE mawmaw_core Threads::Threads)