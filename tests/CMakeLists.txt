project(test_rest_rpc)
set(project_name test_rest_rpc)

include_directories(
        "../thirdparty/msgpack-c/include"
        "../thirdparty/asio"
        )

add_executable(${project_name}
		test_rest_rpc.cpp
		test_server_race.cpp
		test_client_error_close.cpp
        main.cpp
        )

add_test(NAME ${project_name} COMMAND test_rest_rpc)
