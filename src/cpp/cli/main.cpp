// darpa-spill-client-cpp -- drive a DARPA Spill Information Server's API.
//
// The C++ twin of the Python darpa-spill-client: same commands, options and
// output, byte for byte (docs/CLIENT.md). Every HTTP exchange, configuration
// layer and rendering rule is the library's; this file only parses the
// command line and picks what to print.
#include <darpa_spill/client.hpp>
#include <darpa_spill/config.hpp>
#include <darpa_spill/format.hpp>
#include <darpa_spill/version.hpp>

#include <boost/json.hpp>
#include <boost/program_options.hpp>

#include <fstream>
#include <iostream>
#include <map>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#ifdef _WIN32
#include <fcntl.h>
#include <io.h>
#include <stdio.h>
#endif

namespace po = boost::program_options;
namespace json = boost::json;
namespace ds = darpa::spill;

namespace {

constexpr int kExitOk = 0;
constexpr int kExitHttp = 1;
constexpr int kExitUsage = 2;
constexpr int kExitConnect = 3;

const char* const kProgram = "darpa-spill-client-cpp";

/// A usage error: printed after the usage line, exit status 2.
struct UsageError : std::runtime_error {
    explicit UsageError(const std::string& what) : std::runtime_error(what) {}
};

const std::vector<std::string> kCommands = {
    "health", "status", "sources", "signals", "types", "convert", "time-help",
    "latest", "events", "export", "admin-check", "source"};

std::string usage_line() {
    return std::string("usage: ") + kProgram +
           " [GLOBAL OPTIONS] COMMAND [COMMAND OPTIONS]\n";
}

po::options_description global_options() {
    po::options_description desc("Global options");
    desc.add_options()
        ("help,h", "show this help and exit")
        ("config,c", po::value<std::string>()->value_name("FILE"), "YAML configuration file")
        ("env-file", po::value<std::string>()->value_name("FILE"), ".env file to read")
        ("url,u", po::value<std::string>()->value_name("URL"), "server base URL")
        ("timeout,t", po::value<double>()->value_name("SECONDS"), "per-request timeout")
        ("admin-token", po::value<std::string>()->value_name("TOKEN"), "admin token")
        ("admin-token-file", po::value<std::string>()->value_name("FILE"),
         "file holding the admin token")
        ("ca-file", po::value<std::string>()->value_name("FILE"), "CA bundle for https")
        ("insecure,k", "skip TLS certificate verification")
        ("format,f", po::value<std::string>()->value_name("FMT"),
         "output format: table, json or csv (default table)")
        ("tz", po::value<std::string>()->value_name("ZONE"),
         "zone for time inputs that carry none")
        ("print-config", "print the merged configuration and exit")
        ("version,V", "print the version and exit");
    return desc;
}

void print_help(std::ostream& out) {
    out << usage_line() << "\n"
        << "Query and administer a DARPA Spill Information Server over its HTTP API.\n\n"
        << global_options() << "\n"
        << "Commands:\n"
           "  health                      liveness check\n"
           "  status                      server and ingest status\n"
           "  sources                     the TDUs the server records from\n"
           "  signals                     accelerator signals the server knows\n"
           "  types                       decoded spill types\n"
           "  convert TIME                convert a time between timescales\n"
           "  time-help                   accepted time expressions\n"
           "  latest                      the most recent stored event\n"
           "  events                      one page of matching events\n"
           "  export                      stream a whole range\n"
           "  admin-check                 check the admin credentials\n"
           "  source ACTION NAME [URL]    enable, disable, set-url or reset a source\n\n"
           "Run '" << kProgram << " COMMAND --help' for a command's options.\n"
           "Settings: command line > environment > .env > config file > defaults.\n"
           "See darpa-spill-client-cpp(1).\n";
}

/// Split argv into the global options and the command with its arguments.
void split_arguments(int argc, char** argv, std::vector<std::string>& global,
                     std::vector<std::string>& command) {
    static const std::set<std::string> with_value = {
        "-c", "--config", "--env-file", "-u", "--url", "-t", "--timeout",
        "--admin-token", "--admin-token-file", "--ca-file", "-f", "--format", "--tz"};
    int i = 1;
    for (; i < argc; ++i) {
        std::string token = argv[i];
        if (token.size() < 2 || token[0] != '-') break;
        global.push_back(token);
        if (with_value.count(token) && i + 1 < argc) global.push_back(argv[++i]);
    }
    for (; i < argc; ++i) command.emplace_back(argv[i]);
}

/// Options shared by events and export.
void add_selection(po::options_description& desc) {
    desc.add_options()
        ("start", po::value<std::string>()->value_name("TIME"), "start of the range, inclusive")
        ("end", po::value<std::string>()->value_name("TIME"), "end of the range, exclusive")
        ("signal", po::value<std::vector<std::string>>()->value_name("SIG"),
         "accelerator signal such as $74; repeatable")
        ("type", po::value<std::vector<std::string>>()->value_name("TYPE"),
         "decoded spill type; repeatable")
        ("source", po::value<std::vector<std::string>>()->value_name("NAME"),
         "source (TDU) name; repeatable");
}

template <class T>
std::vector<T> many(const po::variables_map& vm, const char* name) {
    return vm.count(name) ? vm[name].as<std::vector<T>>() : std::vector<T>();
}

std::optional<std::string> one(const po::variables_map& vm, const char* name) {
    if (!vm.count(name)) return std::nullopt;
    return vm[name].as<std::string>();
}

/// A parsed command.
struct Command {
    std::string name;
    po::variables_map vm;
    std::vector<std::string> positional;
};

Command parse_command(const std::vector<std::string>& tokens) {
    Command command;
    command.name = tokens.front();
    bool known = false;
    for (const auto& c : kCommands) known = known || c == command.name;
    if (!known) {
        std::string choices;
        for (const auto& c : kCommands) choices += (choices.empty() ? "" : ", ") + c;
        throw UsageError("invalid command '" + command.name + "' (choose from " + choices + ")");
    }

    po::options_description desc(command.name + " options");
    desc.add_options()("help,h", "show this help and exit");
    std::string positional_names;
    int positional_max = 0;
    if (command.name == "convert") {
        positional_names = "TIME";
        positional_max = 1;
    } else if (command.name == "latest") {
        desc.add_options()
            ("signal", po::value<std::string>()->value_name("SIG"), "restrict to one signal")
            ("source", po::value<std::vector<std::string>>()->value_name("NAME"),
             "restrict to this source; repeatable");
    } else if (command.name == "events") {
        add_selection(desc);
        desc.add_options()
            ("limit", po::value<long long>()->value_name("N"), "maximum rows")
            ("offset", po::value<long long>()->value_name("N"), "rows to skip")
            ("desc", "newest first")
            ("columns", po::value<std::string>()->value_name("LIST"), "comma-separated columns");
    } else if (command.name == "export") {
        add_selection(desc);
        desc.add_options()
            ("columns", po::value<std::string>()->value_name("LIST"),
             "comma-separated columns (CSV)")
            ("output,o", po::value<std::string>()->value_name("FILE"), "write to FILE");
    } else if (command.name == "source") {
        positional_names = "ACTION NAME [URL]";
        positional_max = 3;
    }

    po::options_description hidden;
    hidden.add_options()("args", po::value<std::vector<std::string>>(), "");
    po::options_description all;
    all.add(desc).add(hidden);
    po::positional_options_description positional;
    if (positional_max > 0) positional.add("args", -1);

    std::vector<std::string> rest(tokens.begin() + 1, tokens.end());
    try {
        po::store(po::command_line_parser(rest).options(all).positional(positional).run(),
                  command.vm);
        po::notify(command.vm);
    } catch (const po::error& exc) {
        throw UsageError(exc.what());
    }
    if (command.vm.count("help")) {
        std::cout << "usage: " << kProgram << " " << command.name
                  << (positional_names.empty() ? "" : " " + positional_names)
                  << " [OPTIONS]\n\n" << desc;
        std::exit(kExitOk);
    }
    command.positional = many<std::string>(command.vm, "args");
    if (static_cast<int>(command.positional.size()) > positional_max)
        throw UsageError("unrecognized arguments");

    if (command.name == "convert" && command.positional.size() != 1)
        throw UsageError("convert: the TIME argument is required");
    if (command.name == "source") {
        static const std::set<std::string> actions = {"enable", "disable", "reset", "set-url"};
        const auto& p = command.positional;
        if (p.empty()) throw UsageError("source: an ACTION is required");
        if (!actions.count(p[0]))
            throw UsageError("source: invalid action '" + p[0] +
                             "' (choose from enable, disable, reset, set-url)");
        const std::size_t want = p[0] == "set-url" ? 3 : 2;
        if (p.size() < want)
            throw UsageError("source " + p[0] + ": the NAME" +
                             std::string(p[0] == "set-url" ? " and URL arguments are"
                                                           : " argument is") + " required");
        if (p.size() > want) throw UsageError("unrecognized arguments");
    }
    return command;
}

// ------------------------------------------------------------------- output

void write(const std::string& text) { std::cout.write(text.data(), static_cast<std::streamsize>(text.size())); }

void write_raw(const std::string& body) {
    write(body);
    if (body.empty() || body.back() != '\n') write("\n");
}

ds::Selection selection_from(const Command& command, const ds::ClientConfig& config) {
    ds::Selection selection;
    selection.start = one(command.vm, "start");
    selection.end = one(command.vm, "end");
    selection.signals = many<std::string>(command.vm, "signal");
    selection.types = many<std::string>(command.vm, "type");
    selection.sources = many<std::string>(command.vm, "source");
    if (!config.timezone.empty()) selection.tz = config.timezone;
    selection.columns = one(command.vm, "columns");
    return selection;
}

json::value parse_json(const std::string& body) {
    boost::system::error_code ec;
    auto value = json::parse(body, ec);
    if (ec) throw ds::Error("the server's response is not JSON: " + ec.message());
    return value;
}

const json::value& member(const json::value& value, const char* key) {
    const auto* object = value.if_object();
    const json::value* found = object ? object->if_contains(key) : nullptr;
    if (!found) throw ds::Error(std::string("the server's response has no '") + key + "'");
    return *found;
}

void run(const Command& command, const ds::Client& client, const ds::ClientConfig& config) {
    const std::string& name = command.name;
    const std::string& fmt = config.format;
    const auto wire = fmt == "json" ? ds::Format::json : ds::Format::csv;

    if (name == "export") {
        auto selection = selection_from(command, config);
        if (auto output = one(command.vm, "output")) {
            std::ofstream file(*output, std::ios::binary | std::ios::trunc);
            if (!file) throw UsageError("cannot open " + *output + " for writing");
            client.export_to(selection, file, wire);
        } else {
            client.export_to(selection, std::cout, wire);
        }
        return;
    }

    if (name == "events") {
        auto selection = selection_from(command, config);
        if (fmt == "table" && !selection.columns) selection.columns = ds::default_event_table_columns();
        ds::Paging paging;
        if (command.vm.count("limit")) paging.limit = command.vm["limit"].as<long long>();
        if (command.vm.count("offset")) paging.offset = command.vm["offset"].as<long long>();
        paging.descending = command.vm.count("desc") > 0;
        auto body = client.checked("GET", "/api/events",
                                   ds::selection_params(selection, wire, &paging)).body;
        if (fmt != "table") {
            write_raw(body);
            return;
        }
        std::string data;
        std::vector<std::string> warnings;
        ds::split_csv_comments(body, data, warnings);
        for (const auto& w : warnings) std::cerr << "warning: " << w << "\n";
        auto rows = ds::parse_csv(data);
        if (rows.empty()) return;
        ds::Row header = rows.front();
        rows.erase(rows.begin());
        write(ds::render_table(header, rows));
        return;
    }

    if (name == "source") {
        const auto& p = command.positional;
        const std::string path = "/api/sources/" + ds::percent_encode(p[1]);
        ds::Response response;
        if (p[0] == "reset") {
            response = client.checked("POST", path + "/reset");
        } else {
            json::object body;
            if (p[0] == "enable") body["enabled"] = true;
            else if (p[0] == "disable") body["enabled"] = false;
            else body["base_url"] = p[2];
            response = client.checked("PATCH", path, {}, json::serialize(body));
        }
        if (fmt == "json") write_raw(response.body);
        else write(ds::render_kv(member(parse_json(response.body), "source")));
        return;
    }

    static const std::map<std::string, std::string> simple = {
        {"health", "/api/health"},       {"status", "/api/status"},
        {"sources", "/api/sources"},     {"signals", "/api/signals"},
        {"types", "/api/types"},         {"time-help", "/api/time/help"},
        {"admin-check", "/api/admin/check"}};
    std::string path;
    ds::Params params;
    if (auto it = simple.find(name); it != simple.end()) {
        path = it->second;
    } else if (name == "convert") {
        path = "/api/time/convert";
        params.emplace_back("t", command.positional.front());
        if (!config.timezone.empty()) params.emplace_back("tz", config.timezone);
    } else if (name == "latest") {
        path = "/api/latest";
        auto signal = one(command.vm, "signal");
        if (signal && !signal->empty()) params.emplace_back("signal", *signal);
        for (const auto& s : many<std::string>(command.vm, "source")) params.emplace_back("source", s);
    }

    auto response = client.checked("GET", path, params);
    if (fmt == "json") {
        write_raw(response.body);
        return;
    }
    auto data = parse_json(response.body);
    auto columns = ds::list_columns(name);
    if (!columns.empty()) {
        auto rows = ds::rows_from_objects(member(data, name.c_str()), columns);
        write(fmt == "csv" ? ds::render_csv(columns, rows) : ds::render_table(columns, rows));
        return;
    }
    write(ds::render_kv(name == "latest" ? member(data, "event") : data));
}

}  // namespace

int main(int argc, char** argv) {
#ifdef _WIN32
    _setmode(_fileno(stdout), _O_BINARY);
#endif
    std::vector<std::string> global_tokens, command_tokens;
    split_arguments(argc, argv, global_tokens, command_tokens);

    po::variables_map vm;
    Command command;
    try {
        auto desc = global_options();
        po::store(po::command_line_parser(global_tokens).options(desc).run(), vm);
        po::notify(vm);
        if (vm.count("help")) {
            print_help(std::cout);
            return kExitOk;
        }
        if (vm.count("version")) {
            std::cout << kProgram << " " << DARPA_SPILL_VERSION << "\n";
            return kExitOk;
        }
        if (vm.count("format")) {
            auto f = vm["format"].as<std::string>();
            if (f != "table" && f != "json" && f != "csv")
                throw UsageError("argument -f/--format: invalid choice: '" + f +
                                 "' (choose from 'table', 'json', 'csv')");
        }
        if (!command_tokens.empty()) command = parse_command(command_tokens);
    } catch (const po::error& exc) {
        std::cerr << usage_line() << kProgram << ": error: " << exc.what() << "\n";
        return kExitUsage;
    } catch (const UsageError& exc) {
        std::cerr << usage_line() << kProgram << ": error: " << exc.what() << "\n";
        return kExitUsage;
    }

    ds::ClientConfig config;
    std::optional<ds::Client> client;
    try {
        ds::LoadRequest request;
        request.config_path = one(vm, "config");
        request.env_file = one(vm, "env-file");
        auto& o = request.overrides;
        o.url = one(vm, "url");
        if (vm.count("timeout")) o.timeout = vm["timeout"].as<double>();
        o.admin_token = one(vm, "admin-token");
        o.admin_token_file = one(vm, "admin-token-file");
        o.ca_file = one(vm, "ca-file");
        if (vm.count("insecure")) o.verify_tls = false;
        o.format = one(vm, "format");
        o.timezone = one(vm, "tz");
        config = ds::load_config(request);

        if (vm.count("print-config")) {
            write(ds::to_yaml(config));
            return kExitOk;
        }
        if (command.name.empty()) {
            std::cerr << usage_line() << "error: a COMMAND is required\n";
            return kExitUsage;
        }
        client.emplace(config.to_options());
    } catch (const ds::ConfigError& exc) {
        std::cerr << "error: " << exc.what() << "\n";
        return kExitUsage;
    } catch (const ds::Error& exc) {
        std::cerr << "error: " << exc.what() << "\n";
        return kExitUsage;
    }

    int status = kExitOk;
    try {
        run(command, *client, config);
    } catch (const ds::HttpError& exc) {
        std::cerr << "error: HTTP " << exc.status() << ": " << exc.message() << "\n";
        if (!exc.hint().empty()) std::cerr << "hint: " << exc.hint() << "\n";
        status = kExitHttp;
    } catch (const ds::ConnectionError& exc) {
        std::cerr << "error: " << exc.what() << "\n";
        status = kExitConnect;
    } catch (const UsageError& exc) {
        std::cerr << "error: " << exc.what() << "\n";
        status = kExitUsage;
    } catch (const std::exception& exc) {
        std::cerr << "error: " << exc.what() << "\n";
        status = kExitUsage;
    }
    std::cout.flush();
    return status;
}
