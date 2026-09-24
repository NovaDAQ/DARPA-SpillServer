# DARPA Spill Information Server -- C/C++ client library and command line.
#
# The server itself is Python (pyproject.toml). This builds the native client
# for its HTTP API: libdarpa_spill_client (C++17 with a C ABI) and the
# darpa-spill-client-cpp command line, with CppUnit tests. See
# docs/CPP_LIBRARY.md for per-platform instructions.
#
#   cmake --preset default            # or: cmake -S . -B build
#   cmake --build build -j
#   ctest --test-dir build --output-on-failure
#   cmake --install build --prefix /usr/local
cmake_minimum_required(VERSION 3.16)

project(DarpaSpillServer VERSION 1.3.0
        DESCRIPTION "Client library for the DARPA Spill Information Server"
        HOMEPAGE_URL "https://github.com/NovaDAQ/DARPA-SpillServer"
        LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE RelWithDebInfo CACHE STRING "Build type" FORCE)
endif()

option(BUILD_SHARED_LIBS "Build libdarpa_spill_client as a shared library" ON)
option(DARPA_SPILL_BUILD_TESTS "Build the CppUnit tests" ON)
option(DARPA_SPILL_BUILD_EXAMPLES "Build the C example programs" ON)
option(DARPA_SPILL_WITH_TLS "Support https:// servers (needs OpenSSL)" ON)

include(GNUInstallDirs)
include(GenerateExportHeader)
include(CMakePackageConfigHelpers)

# ---------------------------------------------------------------- dependencies

if(POLICY CMP0167)
  cmake_policy(SET CMP0167 NEW)  # use Boost's own BoostConfig.cmake
endif()
find_package(Boost 1.75 REQUIRED COMPONENTS json program_options)
find_package(yaml-cpp REQUIRED)
if(TARGET yaml-cpp::yaml-cpp)
  set(DARPA_SPILL_YAML_TARGET yaml-cpp::yaml-cpp)
else()
  set(DARPA_SPILL_YAML_TARGET yaml-cpp)  # yaml-cpp < 0.8
endif()
find_package(Threads REQUIRED)

if(DARPA_SPILL_WITH_TLS)
  if(APPLE AND NOT OPENSSL_ROOT_DIR)
    foreach(candidate /opt/homebrew/opt/openssl@3 /usr/local/opt/openssl@3)
      if(EXISTS "${candidate}")
        set(OPENSSL_ROOT_DIR "${candidate}")
        break()
      endif()
    endforeach()
  endif()
  find_package(OpenSSL)
  if(NOT OpenSSL_FOUND)
    message(WARNING "OpenSSL not found: building without https support "
                    "(set OPENSSL_ROOT_DIR, or -DDARPA_SPILL_WITH_TLS=OFF to silence this)")
    set(DARPA_SPILL_WITH_TLS OFF)
  endif()
endif()

if(MSVC)
  set(DARPA_SPILL_WARNINGS /W4 /permissive- /bigobj)
else()
  set(DARPA_SPILL_WARNINGS -Wall -Wextra -Wpedantic)
endif()

# ------------------------------------------------------------------- library

set(DARPA_SPILL_GENERATED "${CMAKE_CURRENT_BINARY_DIR}/include")
configure_file(src/include/darpa_spill/version.hpp.in
               "${DARPA_SPILL_GENERATED}/darpa_spill/version.hpp" @ONLY)

add_library(darpa_spill_client
  src/cpp/client.cpp
  src/cpp/http.cpp
  src/cpp/config.cpp
  src/cpp/format.cpp
  src/cpp/c_api.cpp)
add_library(DarpaSpill::client ALIAS darpa_spill_client)

generate_export_header(darpa_spill_client
  BASE_NAME DARPA_SPILL
  EXPORT_FILE_NAME "${DARPA_SPILL_GENERATED}/darpa_spill/darpa_spill_export.h")
if(NOT BUILD_SHARED_LIBS)
  target_compile_definitions(darpa_spill_client PUBLIC DARPA_SPILL_STATIC_DEFINE)
endif()

target_include_directories(darpa_spill_client
  PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/src/include>
    $<BUILD_INTERFACE:${DARPA_SPILL_GENERATED}>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
  PRIVATE src/cpp)
# Boost.JSON is in the public headers; the rest are implementation details.
target_link_libraries(darpa_spill_client
  PUBLIC Boost::json
  PRIVATE Boost::headers ${DARPA_SPILL_YAML_TARGET} Threads::Threads)
if(DARPA_SPILL_WITH_TLS)
  target_compile_definitions(darpa_spill_client PRIVATE DARPA_SPILL_WITH_TLS)
  target_link_libraries(darpa_spill_client PRIVATE OpenSSL::SSL OpenSSL::Crypto)
endif()
if(WIN32)
  target_compile_definitions(darpa_spill_client PRIVATE _WIN32_WINNT=0x0A00 NOMINMAX)
  target_link_libraries(darpa_spill_client PRIVATE ws2_32 mswsock)
endif()
target_compile_options(darpa_spill_client PRIVATE ${DARPA_SPILL_WARNINGS})
set_target_properties(darpa_spill_client PROPERTIES
  EXPORT_NAME client
  VERSION ${PROJECT_VERSION}
  SOVERSION ${PROJECT_VERSION_MAJOR}
  CXX_VISIBILITY_PRESET hidden
  VISIBILITY_INLINES_HIDDEN ON)

# --------------------------------------------------------------- command line

add_executable(darpa-spill-client-cpp src/cpp/cli/main.cpp)
target_link_libraries(darpa-spill-client-cpp PRIVATE DarpaSpill::client Boost::program_options)
target_compile_options(darpa-spill-client-cpp PRIVATE ${DARPA_SPILL_WARNINGS})
if(WIN32)
  target_compile_definitions(darpa-spill-client-cpp PRIVATE NOMINMAX)
endif()

if(DARPA_SPILL_BUILD_EXAMPLES)
  add_executable(example_c_latest examples/c/latest.c)
  target_link_libraries(example_c_latest PRIVATE DarpaSpill::client)
  set_target_properties(example_c_latest PROPERTIES C_STANDARD 99)
endif()

# An installed program finds the library relative to itself, so the install
# tree can be moved or unpacked anywhere.
if(APPLE)
  set(DARPA_SPILL_RPATH "@loader_path/../${CMAKE_INSTALL_LIBDIR}")
elseif(UNIX)
  set(DARPA_SPILL_RPATH "$ORIGIN/../${CMAKE_INSTALL_LIBDIR}")
endif()
if(DARPA_SPILL_RPATH)
  set_target_properties(darpa-spill-client-cpp PROPERTIES INSTALL_RPATH "${DARPA_SPILL_RPATH}")
endif()

# Keep the programs where the Python tests look for them, whatever the generator.
set_target_properties(darpa-spill-client-cpp PROPERTIES
  RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}")

# --------------------------------------------------------------------- tests

if(DARPA_SPILL_BUILD_TESTS)
  find_package(PkgConfig QUIET)
  if(PkgConfig_FOUND)
    pkg_check_modules(CPPUNIT IMPORTED_TARGET cppunit)
  endif()
  if(CPPUNIT_FOUND)
    set(DARPA_SPILL_CPPUNIT PkgConfig::CPPUNIT)
  else()
    find_path(CPPUNIT_INCLUDE_DIR cppunit/TestCase.h)
    find_library(CPPUNIT_LIBRARY NAMES cppunit cppunit_dll)
    if(CPPUNIT_INCLUDE_DIR AND CPPUNIT_LIBRARY)
      add_library(DarpaSpill_cppunit UNKNOWN IMPORTED)
      set_target_properties(DarpaSpill_cppunit PROPERTIES
        IMPORTED_LOCATION "${CPPUNIT_LIBRARY}"
        INTERFACE_INCLUDE_DIRECTORIES "${CPPUNIT_INCLUDE_DIR}")
      set(DARPA_SPILL_CPPUNIT DarpaSpill_cppunit)
    endif()
  endif()

  if(DARPA_SPILL_CPPUNIT)
    enable_testing()
    add_executable(test_darpa_spill_client
      tests/cpp/main.cpp
      tests/cpp/test_query.cpp
      tests/cpp/test_format.cpp
      tests/cpp/test_config.cpp
      tests/cpp/test_errors.cpp)
    target_link_libraries(test_darpa_spill_client PRIVATE DarpaSpill::client ${DARPA_SPILL_CPPUNIT})
    target_compile_options(test_darpa_spill_client PRIVATE ${DARPA_SPILL_WARNINGS})
    add_test(NAME cppunit COMMAND test_darpa_spill_client
             WORKING_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}")
    add_test(NAME cli_version COMMAND darpa-spill-client-cpp --version)
    set_tests_properties(cli_version PROPERTIES
      PASS_REGULAR_EXPRESSION "^darpa-spill-client-cpp ${PROJECT_VERSION}\n$")
  else()
    message(STATUS "CppUnit not found: the C++ unit tests are not built "
                   "(install cppunit, or -DDARPA_SPILL_BUILD_TESTS=OFF)")
  endif()
endif()

# ------------------------------------------------------------------- install

install(TARGETS darpa_spill_client darpa-spill-client-cpp
  EXPORT DarpaSpillClientTargets
  RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
  LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
  ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
  INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
install(FILES
    src/include/darpa_spill/client.hpp
    src/include/darpa_spill/client.h
    src/include/darpa_spill/config.hpp
    src/include/darpa_spill/format.hpp
    "${DARPA_SPILL_GENERATED}/darpa_spill/version.hpp"
    "${DARPA_SPILL_GENERATED}/darpa_spill/darpa_spill_export.h"
  DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/darpa_spill)
install(FILES man/darpa-spill-client-cpp.1 DESTINATION ${CMAKE_INSTALL_MANDIR}/man1)
install(FILES man/darpa_spill_client.3 DESTINATION ${CMAKE_INSTALL_MANDIR}/man3)

set(DARPA_SPILL_CMAKE_DIR ${CMAKE_INSTALL_LIBDIR}/cmake/DarpaSpillClient)
install(EXPORT DarpaSpillClientTargets
  NAMESPACE DarpaSpill::
  DESTINATION ${DARPA_SPILL_CMAKE_DIR})
configure_package_config_file(cmake/DarpaSpillClientConfig.cmake.in
  "${CMAKE_CURRENT_BINARY_DIR}/DarpaSpillClientConfig.cmake"
  INSTALL_DESTINATION ${DARPA_SPILL_CMAKE_DIR})
write_basic_package_version_file(
  "${CMAKE_CURRENT_BINARY_DIR}/DarpaSpillClientConfigVersion.cmake"
  VERSION ${PROJECT_VERSION}
  COMPATIBILITY SameMajorVersion)
install(FILES
    "${CMAKE_CURRENT_BINARY_DIR}/DarpaSpillClientConfig.cmake"
    "${CMAKE_CURRENT_BINARY_DIR}/DarpaSpillClientConfigVersion.cmake"
  DESTINATION ${DARPA_SPILL_CMAKE_DIR})

configure_file(cmake/darpa_spill_client.pc.in
  "${CMAKE_CURRENT_BINARY_DIR}/darpa_spill_client.pc" @ONLY)
install(FILES "${CMAKE_CURRENT_BINARY_DIR}/darpa_spill_client.pc"
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/pkgconfig)
