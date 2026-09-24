// darpa_spill/config.hpp -- configuration for the client and its CLI.
//
// Five layers, each overriding the one before (docs/CLIENT.md):
//   defaults < YAML file < .env file < environment < command line.
#ifndef DARPA_SPILL_CONFIG_HPP
#define DARPA_SPILL_CONFIG_HPP

#include <darpa_spill/client.hpp>
#include <darpa_spill/darpa_spill_export.h>

#include <functional>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace darpa {
namespace spill {

/// A configuration problem: missing file, unknown key, bad value.
class DARPA_SPILL_EXPORT ConfigError : public std::runtime_error {
public:
    explicit ConfigError(const std::string& what) : std::runtime_error(what) {}
};

/// The merged `client:` section.
struct DARPA_SPILL_EXPORT ClientConfig {
    std::string url = "http://localhost:8080";
    double timeout = 30.0;
    std::string admin_token;
    std::string admin_token_file;
    std::string ca_file;
    bool verify_tls = true;
    std::string format = "table";  ///< table | json | csv
    std::string timezone;

    /// Where the YAML layer came from, empty when none was read.
    std::string source;

    /// admin_token if set, else the first line of admin_token_file.
    /// @throws ConfigError if the file cannot be read.
    std::string resolved_admin_token() const;

    /// Connection options for Client.
    ClientOptions to_options() const;
};

/// Settings given on the command line; unset members leave lower layers alone.
struct ConfigOverrides {
    std::optional<std::string> url;
    std::optional<double> timeout;
    std::optional<std::string> admin_token;
    std::optional<std::string> admin_token_file;
    std::optional<std::string> ca_file;
    std::optional<bool> verify_tls;
    std::optional<std::string> format;
    std::optional<std::string> timezone;
};

/// Reads one environment variable; std::nullopt when unset.
using EnvLookup = std::function<std::optional<std::string>(const std::string&)>;

/// The process environment.
DARPA_SPILL_EXPORT EnvLookup process_environment();

/// What load_config reads.
struct LoadRequest {
    std::optional<std::string> config_path;  ///< --config
    std::optional<std::string> env_file;     ///< --env-file
    ConfigOverrides overrides;               ///< the command line
    EnvLookup environment;                   ///< defaults to process_environment()
    /// Candidate YAML files when neither --config nor
    /// $DARPA_SPILL_CLIENT_CONFIG names one; std::nullopt = the standard list.
    std::optional<std::vector<std::string>> search_paths;
    /// Default .env path when neither --env-file nor $DARPA_SPILL_ENV_FILE is set.
    std::string default_env_file = ".env";
};

/// Merge every layer. @throws ConfigError
DARPA_SPILL_EXPORT ClientConfig load_config(const LoadRequest& request);

/// The standard YAML search list for this platform.
DARPA_SPILL_EXPORT std::vector<std::string> default_search_paths();

/// Parse .env text into KEY/VALUE pairs, in file order.
DARPA_SPILL_EXPORT std::vector<std::pair<std::string, std::string>>
parse_dotenv(const std::string& text);

/// 1/0, true/false, yes/no, on/off in any case. @throws ConfigError
DARPA_SPILL_EXPORT bool parse_bool(const std::string& text, const std::string& what);

/// A double as Python's repr() writes it: 30.0, 2.5, 1e-05, 1e+16.
DARPA_SPILL_EXPORT std::string python_float_repr(double value);

/// The merged section as the YAML --print-config writes.
DARPA_SPILL_EXPORT std::string to_yaml(const ClientConfig& config);

}  // namespace spill
}  // namespace darpa

#endif
