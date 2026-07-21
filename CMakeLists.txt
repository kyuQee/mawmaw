cmake_minimum_required(VERSION 3.22)
project(mawmaw VERSION 0.1.0 LANGUAGES CXX C)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE RelWithDebInfo)
endif()
add_compile_options(-Wall -Wextra -Wpedantic -Wno-unused-parameter)

# ==============================================================================
# Configure MAWMAW_PAYLOAD_MAX globally
# ==============================================================================
# Default to 256 if not set, but allow overriding via -DMAWMAW_PAYLOAD_MAX=8192
if(NOT DEFINED MAWMAW_PAYLOAD_MAX)
    set(MAWMAW_PAYLOAD_MAX 256)
endif()
add_compile_definitions(MAWMAW_PAYLOAD_MAX=${MAWMAW_PAYLOAD_MAX})
message(STATUS "MAWMAW_PAYLOAD_MAX = ${MAWMAW_PAYLOAD_MAX}")


if(CMAKE_BUILD_TYPE STREQUAL "Release")
  option(MAWMAW_NATIVE_ARCH "Target this exact machine's CPU (-march=native). OFF = portable binary." ON)
  if(MAWMAW_NATIVE_ARCH)
    add_compile_options(-O3 -march=native)
  else()
    add_compile_options(-O3 -march=x86-64-v2)
  endif()
elseif(CMAKE_BUILD_TYPE STREQUAL "Debug")
  add_compile_options(-O0 -g -fsanitize=address,undefined)
  add_link_options(-fsanitize=address,undefined)
else()
  add_compile_options(-O2 -g)
endif()
if(TARGET m3)
    target_compile_options(m3 PRIVATE
        -w
    )
endif()
include_directories(${CMAKE_SOURCE_DIR})
find_package(Python3 COMPONENTS Interpreter Development REQUIRED)
message(STATUS "Python3 ${Python3_VERSION} at ${Python3_EXECUTABLE}")
# ==============================================================================
# FORCE WASM3 TO SKIP NATIVE CLI 
# ==============================================================================
set(BUILD_NATIVE OFF CACHE BOOL "Disable standalone wasm3 CLI" FORCE)
set(BUILD_WASI OFF CACHE BOOL "Disable WASI support" FORCE)
add_subdirectory(third_party/wasm3)
add_subdirectory(config)
add_subdirectory(core)
add_subdirectory(ingestor)
add_subdirectory(executor)
add_subdirectory(publisher)
add_subdirectory(server)
add_subdirectory(examples/plugins)
add_subdirectory(tests)
add_subdirectory(examples/scripts)
target_link_libraries(mawmaw PRIVATE m3)
