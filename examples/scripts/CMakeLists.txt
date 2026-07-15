find_program(CLANG_BIN clang REQUIRED)
set(SCRIPT_OUTPUT_DIR "${CMAKE_BINARY_DIR}/server/scripts")
add_custom_target(prep_script_dir ALL
    COMMAND ${CMAKE_COMMAND} -E make_directory "${SCRIPT_OUTPUT_DIR}")
add_custom_target(sync_python_scripts ALL
    COMMAND ${CMAKE_COMMAND} -E copy
        "${CMAKE_CURRENT_SOURCE_DIR}/passthrough.py"
        "${SCRIPT_OUTPUT_DIR}/passthrough.py"
    DEPENDS prep_script_dir
    COMMENT "Copying Python scripts...")
add_custom_command(
    OUTPUT "${SCRIPT_OUTPUT_DIR}/passthrough.wasm"
    COMMAND ${CLANG_BIN} --target=wasm32 -nostdlib -fuse-ld=lld
        -Wl,--no-entry -Wl,--export-all -Wl,--allow-undefined
        -Wl,--initial-memory=131072
        -O2 -o "${SCRIPT_OUTPUT_DIR}/passthrough.wasm"
        "${CMAKE_CURRENT_SOURCE_DIR}/passthrough.c"
    DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/passthrough.c"
    COMMENT "Compiling passthrough.wasm...")
add_custom_target(compile_wasm_scripts ALL
    DEPENDS "${SCRIPT_OUTPUT_DIR}/passthrough.wasm" sync_python_scripts)
