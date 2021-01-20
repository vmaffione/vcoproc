find_package(Threads REQUIRED)
find_package(CURL REQUIRED)

message(STATUS "CURL_LIBRARIES = ${CURL_LIBRARIES}")
add_executable(vcoproc vcoproc.cpp utils.cpp)
target_link_libraries(vcoproc ${CMAKE_THREAD_LIBS_INIT} m ${CURL_LIBRARIES})
install(TARGETS vcoproc DESTINATION usr/bin)
