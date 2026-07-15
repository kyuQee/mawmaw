add_executable(test_ring_buffer test_ring_buffer.cpp)
target_link_libraries(test_ring_buffer PRIVATE mawmaw_core)

enable_testing()
add_test(NAME ring_buffer COMMAND test_ring_buffer)
