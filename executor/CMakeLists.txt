add_library(mawmaw_executor INTERFACE)
target_include_directories(mawmaw_executor INTERFACE
    ${CMAKE_SOURCE_DIR}
    ${Python3_INCLUDE_DIRS}
    ${CMAKE_SOURCE_DIR}/third_party/wasm3
)
target_link_libraries(mawmaw_executor INTERFACE mawmaw_core)
