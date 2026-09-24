// Configuration layers (docs/CLIENT.md, "Configuration").
#include <darpa_spill/config.hpp>

#include <cppunit/extensions/HelperMacros.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <map>

using namespace darpa::spill;
namespace fs = std::filesystem;

namespace {

/// A scratch directory removed when the test ends.
struct Scratch {
    fs::path dir;
    Scratch() {
        auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
        dir = fs::temp_directory_path() / ("darpa_spill_test_" + std::to_string(stamp));
        fs::create_directories(dir);
    }
    ~Scratch() {
        std::error_code ec;
        fs::remove_all(dir, ec);
    }
    std::string write(const std::string& name, const std::string& text) const {
        auto path = dir / name;
        std::ofstream(path, std::ios::binary) << text;
        return path.string();
    }
};

EnvLookup env(std::map<std::string, std::string> values) {
    return [values](const std::string& name) -> std::optional<std::string> {
        auto it = values.find(name);
        if (it == values.end()) return std::nullopt;
        return it->second;
    };
}

/// A request that reads nothing from the machine it runs on.
LoadRequest hermetic() {
    LoadRequest request;
    request.environment = env({});
    request.search_paths = std::vector<std::string>{};
    request.default_env_file = "";
    return request;
}

}  // namespace

class ConfigTest : public CppUnit::TestFixture {
    CPPUNIT_TEST_SUITE(ConfigTest);
    CPPUNIT_TEST(defaults);
    CPPUNIT_TEST(yaml_layer);
    CPPUNIT_TEST(yaml_rejects_unknown_keys);
    CPPUNIT_TEST(yaml_rejects_other_sections);
    CPPUNIT_TEST(empty_yaml_is_fine);
    CPPUNIT_TEST(precedence);
    CPPUNIT_TEST(config_named_by_environment_or_dotenv);
    CPPUNIT_TEST(search_path_first_hit);
    CPPUNIT_TEST(missing_files_are_errors);
    CPPUNIT_TEST(validation);
    CPPUNIT_TEST(dotenv_syntax);
    CPPUNIT_TEST(booleans);
    CPPUNIT_TEST(float_repr);
    CPPUNIT_TEST(yaml_output);
    CPPUNIT_TEST(token_file);
    CPPUNIT_TEST_SUITE_END();

public:
    void defaults() {
        auto c = load_config(hermetic());
        CPPUNIT_ASSERT_EQUAL(std::string("http://localhost:8080"), c.url);
        CPPUNIT_ASSERT_EQUAL(30.0, c.timeout);
        CPPUNIT_ASSERT(c.verify_tls);
        CPPUNIT_ASSERT_EQUAL(std::string("table"), c.format);
        CPPUNIT_ASSERT(c.source.empty());
    }

    void yaml_layer() {
        Scratch s;
        auto r = hermetic();
        r.config_path = s.write("c.yaml",
                                "client:\n  url: https://gw:8443/spills\n  timeout: 5\n"
                                "  verify_tls: no\n  format: csv\n  timezone: America/Chicago\n"
                                "  ca_file: ~\n");
        auto c = load_config(r);
        CPPUNIT_ASSERT_EQUAL(std::string("https://gw:8443/spills"), c.url);
        CPPUNIT_ASSERT_EQUAL(5.0, c.timeout);
        CPPUNIT_ASSERT(!c.verify_tls);
        CPPUNIT_ASSERT_EQUAL(std::string("csv"), c.format);
        CPPUNIT_ASSERT_EQUAL(std::string(""), c.ca_file);
        CPPUNIT_ASSERT_EQUAL(*r.config_path, c.source);
    }

    void yaml_rejects_unknown_keys() {
        Scratch s;
        auto r = hermetic();
        r.config_path = s.write("c.yaml", "client:\n  urll: http://x\n");
        CPPUNIT_ASSERT_THROW(load_config(r), ConfigError);
    }

    void yaml_rejects_other_sections() {
        Scratch s;
        auto r = hermetic();
        r.config_path = s.write("c.yaml", "client:\n  url: http://x\nserver:\n  port: 1\n");
        CPPUNIT_ASSERT_THROW(load_config(r), ConfigError);
        r.config_path = s.write("d.yaml", "client: [1, 2]\n");
        CPPUNIT_ASSERT_THROW(load_config(r), ConfigError);
    }

    void empty_yaml_is_fine() {
        Scratch s;
        auto r = hermetic();
        r.config_path = s.write("c.yaml", "");
        CPPUNIT_ASSERT_EQUAL(std::string("table"), load_config(r).format);
        r.config_path = s.write("d.yaml", "client:\n");
        CPPUNIT_ASSERT_EQUAL(std::string("table"), load_config(r).format);
    }

    void precedence() {
        Scratch s;
        auto r = hermetic();
        r.config_path = s.write("c.yaml", "client:\n  url: http://yaml\n  timeout: 1\n"
                                          "  format: csv\n  timezone: UTC\n");
        r.env_file = s.write(".env", "DARPA_SPILL_CLIENT_TIMEOUT=2\nDARPA_SPILL_CLIENT_FORMAT=json\n"
                                     "DARPA_SPILL_CLIENT_URL=http://dotenv\n");
        r.environment = env({{"DARPA_SPILL_CLIENT_FORMAT", "table"},
                             {"DARPA_SPILL_CLIENT_URL", "http://env"}});
        r.overrides.url = "http://cli";
        auto c = load_config(r);
        CPPUNIT_ASSERT_EQUAL(std::string("http://cli"), c.url);  // command line
        CPPUNIT_ASSERT_EQUAL(std::string("table"), c.format);    // environment over .env
        CPPUNIT_ASSERT_EQUAL(2.0, c.timeout);                    // .env over YAML
        CPPUNIT_ASSERT_EQUAL(std::string("UTC"), c.timezone);    // YAML over default
    }

    void config_named_by_environment_or_dotenv() {
        Scratch s;
        auto yaml = s.write("c.yaml", "client:\n  timezone: Europe/Paris\n");
        auto r = hermetic();
        r.environment = env({{"DARPA_SPILL_CLIENT_CONFIG", yaml}});
        CPPUNIT_ASSERT_EQUAL(std::string("Europe/Paris"), load_config(r).timezone);

        auto r2 = hermetic();
        r2.environment = env({{"DARPA_SPILL_ENV_FILE", s.write("x.env", "DARPA_SPILL_CLIENT_CONFIG=" + yaml + "\n")}});
        CPPUNIT_ASSERT_EQUAL(std::string("Europe/Paris"), load_config(r2).timezone);

        auto r3 = hermetic();
        r3.default_env_file = s.write(".env", "DARPA_SPILL_CLIENT_TIMEZONE=Asia/Tokyo\n");
        CPPUNIT_ASSERT_EQUAL(std::string("Asia/Tokyo"), load_config(r3).timezone);
    }

    void search_path_first_hit() {
        Scratch s;
        auto r = hermetic();
        r.search_paths = std::vector<std::string>{(s.dir / "absent.yaml").string(),
                                                  s.write("b.yaml", "client:\n  format: json\n"),
                                                  s.write("c.yaml", "client:\n  format: csv\n")};
        CPPUNIT_ASSERT_EQUAL(std::string("json"), load_config(r).format);
    }

    void missing_files_are_errors() {
        auto r = hermetic();
        r.config_path = "/nonexistent/spillclient.yaml";
        CPPUNIT_ASSERT_THROW(load_config(r), ConfigError);
        auto r2 = hermetic();
        r2.env_file = "/nonexistent/.env";
        CPPUNIT_ASSERT_THROW(load_config(r2), ConfigError);
        auto r3 = hermetic();
        r3.default_env_file = "/nonexistent/.env";  // the default is skipped when absent
        CPPUNIT_ASSERT_NO_THROW(load_config(r3));
    }

    void validation() {
        auto r = hermetic();
        r.overrides.format = "xml";
        CPPUNIT_ASSERT_THROW(load_config(r), ConfigError);
        auto r2 = hermetic();
        r2.overrides.timeout = 0.0;
        CPPUNIT_ASSERT_THROW(load_config(r2), ConfigError);
        auto r3 = hermetic();
        r3.environment = env({{"DARPA_SPILL_CLIENT_URL", "localhost:8080"}});
        CPPUNIT_ASSERT_THROW(load_config(r3), ConfigError);
        auto r4 = hermetic();
        r4.environment = env({{"DARPA_SPILL_CLIENT_TIMEOUT", "soon"}});
        CPPUNIT_ASSERT_THROW(load_config(r4), ConfigError);
        auto r5 = hermetic();
        r5.environment = env({{"DARPA_SPILL_CLIENT_VERIFY_TLS", "maybe"}});
        CPPUNIT_ASSERT_THROW(load_config(r5), ConfigError);
    }

    void dotenv_syntax() {
        auto values = parse_dotenv("# comment\n\n  export A=1\r\nB = 'two words' \nC=\"x\"\nD=\"'\nE=\n");
        CPPUNIT_ASSERT_EQUAL(std::size_t(5), values.size());
        CPPUNIT_ASSERT_EQUAL(std::string("A"), values[0].first);
        CPPUNIT_ASSERT_EQUAL(std::string("1"), values[0].second);
        CPPUNIT_ASSERT_EQUAL(std::string("two words"), values[1].second);
        CPPUNIT_ASSERT_EQUAL(std::string("x"), values[2].second);
        CPPUNIT_ASSERT_EQUAL(std::string("\"'"), values[3].second);
        CPPUNIT_ASSERT_EQUAL(std::string(""), values[4].second);
        CPPUNIT_ASSERT_THROW(parse_dotenv("JUSTAKEY\n"), ConfigError);
        CPPUNIT_ASSERT_THROW(parse_dotenv("=value\n"), ConfigError);
    }

    void booleans() {
        for (const char* t : {"1", "true", "TRUE", "Yes", "on"}) CPPUNIT_ASSERT(parse_bool(t, "x"));
        for (const char* f : {"0", "false", "No", "OFF"}) CPPUNIT_ASSERT(!parse_bool(f, "x"));
        CPPUNIT_ASSERT_THROW(parse_bool("2", "x"), ConfigError);
    }

    void float_repr() {
        CPPUNIT_ASSERT_EQUAL(std::string("30.0"), python_float_repr(30.0));
        CPPUNIT_ASSERT_EQUAL(std::string("2.5"), python_float_repr(2.5));
        CPPUNIT_ASSERT_EQUAL(std::string("0.1"), python_float_repr(0.1));
        CPPUNIT_ASSERT_EQUAL(std::string("0.0001"), python_float_repr(0.0001));
        CPPUNIT_ASSERT_EQUAL(std::string("1e-05"), python_float_repr(0.00001));
        CPPUNIT_ASSERT_EQUAL(std::string("100000.0"), python_float_repr(1e5));
        CPPUNIT_ASSERT_EQUAL(std::string("1e+16"), python_float_repr(1e16));
        CPPUNIT_ASSERT_EQUAL(std::string("1234567890123456.0"), python_float_repr(1234567890123456.0));
        CPPUNIT_ASSERT_EQUAL(std::string("-2.25"), python_float_repr(-2.25));
        CPPUNIT_ASSERT_EQUAL(std::string("0.30000000000000004"), python_float_repr(0.1 + 0.2));
        CPPUNIT_ASSERT_EQUAL(std::string("0.0"), python_float_repr(0.0));
    }

    void yaml_output() {
        ClientConfig c;
        CPPUNIT_ASSERT_EQUAL(std::string("client:\n"
                                         "  url: http://localhost:8080\n"
                                         "  timeout: 30.0\n"
                                         "  admin_token: ''\n"
                                         "  admin_token_file: ''\n"
                                         "  ca_file: ''\n"
                                         "  verify_tls: true\n"
                                         "  format: table\n"
                                         "  timezone: ''\n"),
                             to_yaml(c));
        c.admin_token = "secret";
        c.timeout = 2.5;
        c.verify_tls = false;
        auto text = to_yaml(c);
        CPPUNIT_ASSERT(text.find("  admin_token: '***'\n") != std::string::npos);
        CPPUNIT_ASSERT(text.find("secret") == std::string::npos);
        CPPUNIT_ASSERT(text.find("  timeout: 2.5\n") != std::string::npos);
        CPPUNIT_ASSERT(text.find("  verify_tls: false\n") != std::string::npos);
    }

    void token_file() {
        Scratch s;
        ClientConfig c;
        c.admin_token_file = s.write("token", "  abc123  \nsecond line\n");
        CPPUNIT_ASSERT_EQUAL(std::string("abc123"), c.resolved_admin_token());
        c.admin_token = "direct";
        CPPUNIT_ASSERT_EQUAL(std::string("direct"), c.resolved_admin_token());
        ClientConfig missing;
        missing.admin_token_file = (s.dir / "absent").string();
        CPPUNIT_ASSERT_THROW(missing.resolved_admin_token(), ConfigError);
        CPPUNIT_ASSERT_EQUAL(std::string("abc123"), [&] {
            ClientConfig d;
            d.admin_token_file = c.admin_token_file;
            return d.to_options().admin_token;
        }());
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(ConfigTest);
