add_library(mawmaw_core INTERFACE)
target_include_directories(mawmaw_core INTERFACE ${CMAKE_SOURCE_DIR})
target_compile_features(mawmaw_core INTERFACE cxx_std_20)
