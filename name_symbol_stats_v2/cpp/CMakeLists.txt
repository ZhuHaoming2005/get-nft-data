cmake_minimum_required(VERSION 3.20)
project(name_worker LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

enable_testing()

add_library(name_worker_core src/worker_core.cpp)
target_include_directories(name_worker_core PUBLIC src)

add_executable(name_worker_tests tests/worker_tests.cpp)
target_link_libraries(name_worker_tests PRIVATE name_worker_core)
add_test(NAME name_worker_tests COMMAND name_worker_tests)

if (DEFINED ENV{POSTGRESQL_ROOT} AND NOT "$ENV{POSTGRESQL_ROOT}" STREQUAL "")
    set(POSTGRESQL_ROOT_HINT "$ENV{POSTGRESQL_ROOT}")
else()
    set(POSTGRESQL_ROOT_HINT "C:/Program Files/PostgreSQL/18")
endif()

find_program(POSTGRESQL_PG_CONFIG NAMES pg_config HINTS "${POSTGRESQL_ROOT_HINT}/bin")
if (NOT POSTGRESQL_PG_CONFIG)
    message(FATAL_ERROR "pg_config not found under ${POSTGRESQL_ROOT_HINT}/bin. Set POSTGRESQL_ROOT to your PostgreSQL install directory.")
endif()

execute_process(
    COMMAND "${POSTGRESQL_PG_CONFIG}" --includedir
    OUTPUT_VARIABLE POSTGRESQL_INCLUDEDIR
    OUTPUT_STRIP_TRAILING_WHITESPACE
)
execute_process(
    COMMAND "${POSTGRESQL_PG_CONFIG}" --libdir
    OUTPUT_VARIABLE POSTGRESQL_LIBDIR
    OUTPUT_STRIP_TRAILING_WHITESPACE
)

if (NOT EXISTS "${POSTGRESQL_INCLUDEDIR}/libpq-fe.h")
    message(FATAL_ERROR "PostgreSQL headers not found under ${POSTGRESQL_INCLUDEDIR}")
endif()
if (NOT POSTGRESQL_LIBDIR)
    message(FATAL_ERROR "Unable to resolve PostgreSQL libdir from ${POSTGRESQL_PG_CONFIG}")
endif()

get_filename_component(POSTGRESQL_ROOT_DIR "${POSTGRESQL_LIBDIR}" DIRECTORY)
set(POSTGRESQL_BINDIR "${POSTGRESQL_ROOT_DIR}/bin")

find_library(
    PostgreSQL_LIBRARY
    NAMES pq libpq libpq.dll.a libpq.a
    PATHS "${POSTGRESQL_LIBDIR}"
    NO_DEFAULT_PATH
)

if (NOT PostgreSQL_LIBRARY)
    message(FATAL_ERROR "libpq library not found under ${POSTGRESQL_LIBDIR}")
endif()

add_executable(name_worker src/main.cpp)
target_include_directories(name_worker PRIVATE src "${POSTGRESQL_INCLUDEDIR}")
target_link_libraries(name_worker PRIVATE name_worker_core "${PostgreSQL_LIBRARY}")
if (WIN32 AND EXISTS "${POSTGRESQL_BINDIR}")
    file(GLOB POSTGRESQL_RUNTIME_DLLS "${POSTGRESQL_BINDIR}/*.dll")
    foreach(runtime_dll IN LISTS POSTGRESQL_RUNTIME_DLLS)
        add_custom_command(
            TARGET name_worker POST_BUILD
            COMMAND ${CMAKE_COMMAND} -E copy_if_different "${runtime_dll}" "$<TARGET_FILE_DIR:name_worker>"
        )
    endforeach()
endif()
