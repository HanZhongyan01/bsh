#pragma once
#include <string>

struct Config {
    std::string db_path;
    int max_suggestions = 5;
    std::string ipc_socket_path;

    static Config load();
};