cmake_minimum_required(VERSION 2.8)
project(odbxuv-lua C)

if(NOT DEFINED LUAJIT_INCLUDE_DIRS)
    find_package(PkgConfig)
    message(STATUS "Detecting LuaJIT...")
    if(PKG_CONFIG_FOUND)
        message(STATUS "Using pkg-config to detect LuaJIT...")
        pkg_check_modules(LUAJIT luajit)
        if(LUAJIT_FOUND)
            message(STATUS "Found LuaJIT.")
            message(STATUS "include: ${LUAJIT_INCLUDE_DIRS}")
            include_directories(${LUAJIT_INCLUDE_DIRS})
            link_directories(${LUAJIT_LIBRARY_DIRS})
            set(EXTRA_LIBS ${LUAJIT_LIBRARIES})
        else()
            message(FATAL_ERROR "LuaJIT not found.")
        endif()
    else()
        message(STATUS "Using local LuaJIT.")
        set(LUAJIT_LIBRARIES luajit)
    endif()
endif()

option(HAVE_LOCAL_OPENDBXUV 0)
if(${HAVE_LOCAL_OPENDBXUV})
    add_subdirectory(libraries/OpenDBX-uv)
endif()


set(ODBXUVLUA_INCLUDE_DIRS
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${ODBXUV_INCLUDE_DIRS}
    ${LUAJIT_INCLUDE_DIRS}
    CACHE INTERNAL "Odbxuv-lua include directories")

set(ODBXUVLUA_LIBRARY
    "odbxuv-lua"
    CACHE INTERNAL "Odbxuv-lua library name")

set(ODBXUVLUA_LIBRARIES
    ${ODBXUVLUA_LIBRARY}
    ${ODBXUV_LIBRARIES}
    CACHE INTERNAL "Odbxuv-lua libraries")

set(ODBXUVLUA_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/src/lua_opendbxuv.c)

set(ODBXUVLUA_MODE "STATIC")

include_directories(${ODBXUVLUA_INCLUDE_DIRS})

if(${ODBXUVLUA_MODE} MATCHES "SHARED")
    add_definitions(-DLIB_SHARED=1)
endif()

add_library(${ODBXUVLUA_LIBRARY} ${ODBXUVLUA_MODE}
    ${ODBXUVLUA_SOURCES})

if(INSTALL_LIBRARY_DIR)
    install(
        TARGETS ${ODBXUVLUA_LIBRARY}
        ARCHIVE DESTINATION ${INSTALL_ARCHIVE_DIR}
        LIBRARY DESTINATION ${INSTALL_LIBRARY_DIR})
endif()

if(INSTALL_LUA_DIR)
    FILE(GLOB_RECURSE TEST_FILES "${CMAKE_CURRENT_SOURCE_DIR}/test/**")
    install(
        FILES ${TEST_FILES}
        DESTINATION ${INSTALL_LUA_DIR}/test)
endif()
