cmake_minimum_required(VERSION 3.10)

project(vcoproc)

set(VCOPROC_VER_MAJ 1)
set(VCOPROC_VER_MIN 14)

# Compiler checks
message(STATUS "C Compiler: ${CMAKE_C_COMPILER}")
message(STATUS "C++ Compiler: ${CMAKE_CXX_COMPILER}")

if(CMAKE_C_COMPILER STREQUAL "CMAKE_C_COMPILER-NOTFOUND")
    message(FATAL_ERROR "Cannot find C compiler")
endif()

if(CMAKE_CXX_COMPILER STREQUAL "CMAKE_CXX_COMPILER-NOTFOUND")
    message(FATAL_ERROR "Cannot find C++ compiler")
endif()

# Prepare compiler flags for C++14
if (CMAKE_VERSION VERSION_LESS "3.1")
    set(CMAKE_CXX_FLAGS "-std=c++14 ${CMAKE_CXX_FLAGS}")
else()
    set(CMAKE_CXX_STANDARD 14)
endif()

set(INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/src)

message(STATUS "CMAKE_CURRENT_SOURCE_DIR: ${CMAKE_CURRENT_SOURCE_DIR}")
message(STATUS "INCLUDE_DIR: ${INCLUDE_DIR}")

add_definitions("-DVC_VERSION=${VCOPROC_VER_MAJ}.${VCOPROC_VER_MIN}")
add_definitions("-Wall -g -Werror")
if(DEBUG STREQUAL "y")
    # Enable address sanitizer
    set (CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fno-omit-frame-pointer -fsanitize=address")
    set (CMAKE_LINKER_FLAGS_DEBUG "${CMAKE_LINKER_FLAGS_DEBUG} -fno-omit-frame-pointer -fsanitize=address")
    message(STATUS "Configuring for debug mode")
elseif(OPTIMIZE STREQUAL "y")
    add_definitions("-O2")
    message(STATUS "Building with optimization enabled (-O2)")
endif()

include_directories(${INCLUDE_DIR})

enable_testing()

# Install the systemd services
install(FILES vcoproc.service DESTINATION /usr/share/vcoproc/examples/)

# Include the subdirectory which contains the actual sources.
add_subdirectory(src)

if (ARCH STREQUAL "x86_64")
	set(PKG_ARCH "amd64")
elseif (ARCH STREQUAL "aarch64")
	set(PKG_ARCH "arm64")
else()
	set(PKG_ARCH ${ARCH})
endif()
message(STATUS "Architecture is ${ARCH} (${PKG_ARCH})")

# Support for generating deb, tar.gz, rpm, etc.
# The CPACK_GENERATOR value is actually overridden by the cpack -G option.
set(CPACK_GENERATOR "DEB;TGZ")
set(CPACK_PACKAGE_VERSION_MAJOR ${VCOPROC_VER_MAJ})
set(CPACK_PACKAGE_VERSION_MINOR ${VCOPROC_VER_MIN})
set(CPACK_PACKAGE_RELEASE 1)
set(CPACK_DEBIAN_PACKAGE_DEPENDS "libcurl4, libsqlite3-0")
set(CPACK_DEBIAN_PACKAGE_MAINTAINER "VincenzoM")
set(CPACK_DEBIAN_PACKAGE_ARCHITECTURE ${PKG_ARCH})
set(CPACK_DEBIAN_PACKAGE_SECTION "net")
SET(CPACK_SET_DESTDIR "ON")
set(CPACK_PACKAGE_DESCRIPTION "Coprocessor")
include(CPack)
