// config.cpp -- the client configuration layers (docs/CLIENT.md).
//
// Mirrors darpa_spillserver.client.load_client_config step for step, so that
// both clients accept and reject the same files and print the same
// --print-config text.
#include <darpa_spill/config.hpp>

#include <yaml-cpp/yaml.h>

#include <algorithm>
#include <cctype>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <map>
#include <sstream>

namespace darpa {
namespace spill {

namespace fs = std::filesystem;

namespace {

const std::string kEnvPrefix = "DARPA_SPILL_";
const std::string kClientPrefix = kEnvPrefix + "CLIENT_";

/// The settings in --print-config order.
const std::vector<std::string> kKeys = {"url",     "timeout",    "admin_token", "admin_token_file",
                                        "ca_file", "verify_tls", "format",      "timezone"};

std::string strip(const std::string& text) {
    static const char* const space = " \t\r\n\f\v";
    auto first = text.find_first_not_of(space);
    if (first == std::string::npos) return {};
    return text.substr(first, text.find_last_not_of(space) - first + 1);
}

std::string lower(std::string text) {
    std::transform(text.begin(), text.end(), text.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return text;
}

std::string upper(std::string text) {
    std::transform(text.begin(), text.end(), text.begin(),
                   [](unsigned char c) { return static_cast<char>(std::toupper(c)); });
    return text;
}

std::optional<std::string> getenv_string(const char* name) {
    const char* value = std::getenv(name);  // NOLINT(concurrency-mt-unsafe)
    if (!value) return std::nullopt;
    return std::string(value);
}

/// "~" and "~/..." to the home directory, as Python's expanduser does.
std::string expand_user(const std::string& path) {
    if (path.empty() || path[0] != '~') return path;
    if (path.size() > 1 && path[1] != '/' && path[1] != '\\') return path;
    auto home = getenv_string("HOME");
#ifdef _WIN32
    if (!home) home = getenv_string("USERPROFILE");
#endif
    if (!home) return path;
    return *home + path.substr(1);
}

bool is_file(const std::string& path) {
    std::error_code ec;
    return fs::is_regular_file(fs::path(expand_user(path)), ec);
}

std::string read_file(const std::string& path, const std::string& what) {
    std::ifstream in(expand_user(path), std::ios::binary);
    if (!in) throw ConfigError(what + ": cannot read " + path);
    std::ostringstream text;
    text << in.rdbuf();
    return text.str();
}

double parse_number(const std::string& text, const std::string& what) {
    std::string value = strip(text);
    try {
        std::size_t used = 0;
        double number = std::stod(value, &used);
        if (used == value.size()) return number;
    } catch (const std::exception&) {
    }
    throw ConfigError(what + " must be a number, got '" + text + "'");
}

/// Apply one (key, text) setting from @p origin.
void apply(ClientConfig& config, const std::string& key, const std::string& value,
           const std::string& origin) {
    if (key == "url") config.url = value;
    else if (key == "timeout") config.timeout = parse_number(value, origin + ": timeout");
    else if (key == "admin_token") config.admin_token = value;
    else if (key == "admin_token_file") config.admin_token_file = value;
    else if (key == "ca_file") config.ca_file = value;
    else if (key == "verify_tls") config.verify_tls = parse_bool(value, origin + ": verify_tls");
    else if (key == "format") config.format = value;
    else if (key == "timezone") config.timezone = value;
    else {
        std::string known;
        for (const auto& k : kKeys) known += (known.empty() ? "" : ", ") + k;
        throw ConfigError(origin + ": unknown client setting '" + key + "'; known: " + known);
    }
}

void apply_environment(ClientConfig& config, const EnvLookup& lookup, const std::string& origin) {
    for (const auto& key : kKeys) {
        if (auto value = lookup(kClientPrefix + upper(key))) apply(config, key, *value, origin);
    }
}

void apply_yaml(ClientConfig& config, const std::string& path) {
    YAML::Node root;
    try {
        root = YAML::LoadFile(expand_user(path));
    } catch (const YAML::Exception& exc) {
        throw ConfigError("cannot read " + path + ": " + exc.what());
    }
    if (!root || root.IsNull()) return;
    if (!root.IsMap()) throw ConfigError(path + ": expected a single 'client:' section");
    for (const auto& item : root) {
        if (item.first.as<std::string>() != "client")
            throw ConfigError(path + ": expected a single 'client:' section");
    }
    YAML::Node section = root["client"];
    if (!section || section.IsNull()) return;
    if (!section.IsMap()) throw ConfigError(path + ": 'client' must be a mapping");
    for (const auto& item : section) {
        const auto key = item.first.as<std::string>();
        const YAML::Node& node = item.second;
        std::string text;
        if (node.IsNull()) {
            if (key == "timeout") throw ConfigError(path + ": timeout must be a number, got None");
            if (key == "verify_tls")
                throw ConfigError(path + ": verify_tls must be a boolean, got None");
        } else if (node.IsScalar()) {
            text = node.Scalar();
        } else {
            throw ConfigError(path + ": " + key + " must be a single value");
        }
        apply(config, key, text, path);
    }
}

void validate(const ClientConfig& config) {
    if (config.url.rfind("http://", 0) != 0 && config.url.rfind("https://", 0) != 0)
        throw ConfigError("url must start with http:// or https://, got '" + config.url + "'");
    if (!(config.timeout > 0))
        throw ConfigError("timeout must be positive, got " + python_float_repr(config.timeout));
    if (config.format != "table" && config.format != "json" && config.format != "csv")
        throw ConfigError("format must be table, json or csv, got '" + config.format + "'");
}

}  // namespace

// ------------------------------------------------------------------ public

EnvLookup process_environment() {
    return [](const std::string& name) { return getenv_string(name.c_str()); };
}

std::vector<std::string> default_search_paths() {
#ifdef _WIN32
    std::string system = getenv_string("PROGRAMDATA").value_or("C:\\ProgramData") +
                         "\\darpa-spillserver\\spillclient.yaml";
#else
    std::string system = "/etc/darpa-spillserver/spillclient.yaml";
#endif
    return {"./config/spillclient.yaml", "~/.config/darpa-spillserver/spillclient.yaml", system};
}

std::vector<std::pair<std::string, std::string>> parse_dotenv(const std::string& text) {
    std::vector<std::pair<std::string, std::string>> values;
    std::size_t pos = 0;
    int number = 0;
    while (pos <= text.size()) {
        auto end = text.find('\n', pos);
        std::string raw = text.substr(pos, end == std::string::npos ? std::string::npos : end - pos);
        pos = end == std::string::npos ? text.size() + 1 : end + 1;
        ++number;
        if (!raw.empty() && raw.back() == '\r') raw.pop_back();
        std::string line = strip(raw);
        if (line.empty() || line[0] == '#') continue;
        if (line.rfind("export ", 0) == 0) line = strip(line.substr(7));
        auto eq = line.find('=');
        std::string name = strip(line.substr(0, eq));
        if (eq == std::string::npos || name.empty())
            throw ConfigError("line " + std::to_string(number) + ": expected KEY=VALUE, got '" +
                              raw + "'");
        std::string value = strip(line.substr(eq + 1));
        if (value.size() >= 2 && value.front() == value.back() &&
            (value.front() == '\'' || value.front() == '"'))
            value = value.substr(1, value.size() - 2);
        values.emplace_back(name, value);
    }
    return values;
}

bool parse_bool(const std::string& text, const std::string& what) {
    const std::string value = lower(strip(text));
    if (value == "1" || value == "true" || value == "yes" || value == "on") return true;
    if (value == "0" || value == "false" || value == "no" || value == "off") return false;
    throw ConfigError(what + " must be a boolean, got '" + text + "'");
}

std::string python_float_repr(double value) {
    if (std::isnan(value)) return "nan";
    if (std::isinf(value)) return value < 0 ? "-inf" : "inf";

    // Shortest %.{p}e that reads back as the same double: Python's repr digits.
    char buffer[64];
    int precision = 0;
    for (precision = 0; precision < 17; ++precision) {
        std::snprintf(buffer, sizeof buffer, "%.*e", precision, value);
        if (std::strtod(buffer, nullptr) == value) break;
    }
    std::string sci(buffer);
    const bool negative = sci[0] == '-';
    if (negative) sci.erase(0, 1);
    auto epos = sci.find('e');
    int exponent = std::atoi(sci.c_str() + epos + 1);
    std::string digits;
    for (std::size_t i = 0; i < epos; ++i)
        if (sci[i] != '.') digits.push_back(sci[i]);
    while (digits.size() > 1 && digits.back() == '0') digits.pop_back();

    std::string out;
    if (exponent < -4 || exponent >= 16) {
        out = digits.substr(0, 1);
        if (digits.size() > 1) out += "." + digits.substr(1);
        char exp[16];
        std::snprintf(exp, sizeof exp, "e%c%02d", exponent < 0 ? '-' : '+', std::abs(exponent));
        out += exp;
    } else if (exponent < 0) {
        out = "0." + std::string(static_cast<std::size_t>(-exponent - 1), '0') + digits;
    } else {
        const auto whole = static_cast<std::size_t>(exponent) + 1;
        if (digits.size() <= whole) {
            out = digits + std::string(whole - digits.size(), '0') + ".0";
        } else {
            out = digits.substr(0, whole) + "." + digits.substr(whole);
        }
    }
    return negative ? "-" + out : out;
}

std::string ClientConfig::resolved_admin_token() const {
    if (!admin_token.empty()) return admin_token;
    if (admin_token_file.empty()) return {};
    std::ifstream in(expand_user(admin_token_file), std::ios::binary);
    if (!in) throw ConfigError("admin_token_file: cannot read " + admin_token_file);
    std::string line;
    std::getline(in, line);
    return strip(line);
}

ClientOptions ClientConfig::to_options() const {
    ClientOptions options;
    options.url = url;
    options.timeout = timeout;
    options.admin_token = resolved_admin_token();
    options.ca_file = expand_user(ca_file);
    options.verify_tls = verify_tls;
    return options;
}

ClientConfig load_config(const LoadRequest& request) {
    const EnvLookup environment = request.environment ? request.environment : process_environment();

    // -- the .env file: explicit, then $DARPA_SPILL_ENV_FILE, then the default.
    std::map<std::string, std::string> dotenv;
    std::optional<std::string> env_file = request.env_file;
    if (!env_file || env_file->empty()) env_file = environment(kEnvPrefix + "ENV_FILE");
    std::string dotenv_path;
    if (env_file && !env_file->empty()) {
        if (!is_file(*env_file)) throw ConfigError(".env file not found: " + *env_file);
        dotenv_path = *env_file;
    } else if (!request.default_env_file.empty() && is_file(request.default_env_file)) {
        dotenv_path = request.default_env_file;
    }
    if (!dotenv_path.empty()) {
        try {
            for (auto& kv : parse_dotenv(read_file(dotenv_path, ".env file")))
                dotenv[kv.first] = kv.second;
        } catch (const ConfigError& exc) {
            throw ConfigError(dotenv_path + ": " + exc.what());
        }
    }
    auto dotenv_lookup = [&dotenv](const std::string& name) -> std::optional<std::string> {
        auto it = dotenv.find(name);
        if (it == dotenv.end()) return std::nullopt;
        return it->second;
    };

    ClientConfig config;

    // -- the YAML file.
    std::optional<std::string> chosen = request.config_path;
    if (!chosen || chosen->empty()) chosen = environment(kClientPrefix + "CONFIG");
    if (!chosen || chosen->empty()) chosen = dotenv_lookup(kClientPrefix + "CONFIG");
    std::string path;
    if (chosen && !chosen->empty()) {
        path = expand_user(*chosen);
        if (!is_file(path)) throw ConfigError("config file not found: " + path);
    } else {
        for (const auto& candidate : request.search_paths.value_or(default_search_paths())) {
            if (is_file(candidate)) {
                path = expand_user(candidate);
                break;
            }
        }
    }
    if (!path.empty()) {
        apply_yaml(config, path);
        config.source = path;
    }

    // -- .env, environment, command line.
    apply_environment(config, dotenv_lookup, "the .env file");
    apply_environment(config, environment, "the environment");

    const auto& o = request.overrides;
    if (o.url) config.url = *o.url;
    if (o.timeout) config.timeout = *o.timeout;
    if (o.admin_token) config.admin_token = *o.admin_token;
    if (o.admin_token_file) config.admin_token_file = *o.admin_token_file;
    if (o.ca_file) config.ca_file = *o.ca_file;
    if (o.verify_tls) config.verify_tls = *o.verify_tls;
    if (o.format) config.format = *o.format;
    if (o.timezone) config.timezone = *o.timezone;

    validate(config);
    return config;
}

std::string to_yaml(const ClientConfig& config) {
    auto text = [](const std::string& value) { return value.empty() ? std::string("''") : value; };
    std::string out = "client:\n";
    out += "  url: " + text(config.url) + "\n";
    out += "  timeout: " + python_float_repr(config.timeout) + "\n";
    out += "  admin_token: " + (config.admin_token.empty() ? std::string("''") : std::string("'***'")) + "\n";
    out += "  admin_token_file: " + text(config.admin_token_file) + "\n";
    out += "  ca_file: " + text(config.ca_file) + "\n";
    out += "  verify_tls: " + std::string(config.verify_tls ? "true" : "false") + "\n";
    out += "  format: " + text(config.format) + "\n";
    out += "  timezone: " + text(config.timezone) + "\n";
    return out;
}

}  // namespace spill
}  // namespace darpa
