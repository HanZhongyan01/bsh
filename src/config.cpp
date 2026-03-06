#include "config.hpp"
#include <filesystem>
#include <fstream>
#include <iostream>
#include <algorithm>
#include <cstdlib>

namespace fs = std::filesystem;

std::string get_config_path() {
    const char* xdg_config_home = std::getenv("XDG_CONFIG_HOME");
    fs::path dir;

    if (xdg_config_home && *xdg_config_home != '\0') {
        dir = fs::path(xdg_config_home) / "bsh";
    } else {
        const char* home = std::getenv("HOME");
        if (home) {
            dir = fs::path(home) / ".config" / "bsh";
        } else {
            return "";
        }
    }
    return (dir / "config.toml").string();
}

std::string trim(const std::string& str) {
    auto start = str.find_first_not_of(" \t\r\n\"'");
    if (start == std::string::npos) return "";
    auto end = str.find_last_not_of(" \t\r\n\"'");
    return str.substr(start, end - start + 1);
}

Config Config::load() {
    Config config;
    std::string config_path = get_config_path();
    if (config_path.empty() || !fs::exists(config_path)) {
        return config;
    }

    std::ifstream file(config_path);
    if (!file.is_open()) {
        return config;
    }

    std::string line;
    while (std::getline(file, line)) {
        auto comment_pos = line.find('#');
        if (comment_pos != std::string::npos) {
            line = line.substr(0, comment_pos);
        }

        auto eq_pos = line.find('=');
        if (eq_pos != std::string::npos) {
            std::string key = trim(line.substr(0, eq_pos));
            std::string value = trim(line.substr(eq_pos + 1));

            if (key == "db_path") {
                config.db_path = value;
            } else if (key == "max_suggestions") {
                try {
                    config.max_suggestions = std::stoi(value);
                } catch (...) {}
            } else if (key == "ipc_socket_path") {
                config.ipc_socket_path = value;
            }
        }
    }

    return config;
}