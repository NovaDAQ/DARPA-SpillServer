// client.cpp -- darpa::spill::Client and the query-building helpers.
#include <darpa_spill/client.hpp>
#include <darpa_spill/version.hpp>

#include "http.hpp"

#include <boost/json.hpp>

#include <sstream>

namespace darpa {
namespace spill {

namespace json = boost::json;

const char* version() { return DARPA_SPILL_VERSION; }

std::string percent_encode(const std::string& text) {
    static const char hex[] = "0123456789ABCDEF";
    std::string out;
    out.reserve(text.size() * 3);
    for (unsigned char c : text) {
        bool unreserved = (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
                          (c >= '0' && c <= '9') || c == '-' || c == '.' || c == '_' ||
                          c == '~';
        if (unreserved) {
            out.push_back(static_cast<char>(c));
        } else {
            out.push_back('%');
            out.push_back(hex[c >> 4]);
            out.push_back(hex[c & 0x0F]);
        }
    }
    return out;
}

std::string build_query(const Params& params) {
    std::string out;
    for (const auto& kv : params) {
        if (!out.empty()) out.push_back('&');
        out += percent_encode(kv.first);
        out.push_back('=');
        out += percent_encode(kv.second);
    }
    return out;
}

Params selection_params(const Selection& selection, std::optional<Format> format,
                        const Paging* paging) {
    Params params;
    if (selection.start) params.emplace_back("start", *selection.start);
    if (selection.end) params.emplace_back("end", *selection.end);
    for (const auto& s : selection.signals) params.emplace_back("signal", s);
    for (const auto& t : selection.types) params.emplace_back("type", t);
    for (const auto& s : selection.sources) params.emplace_back("source", s);
    if (format) params.emplace_back("format", *format == Format::csv ? "csv" : "json");
    if (paging) {
        if (paging->limit) params.emplace_back("limit", std::to_string(*paging->limit));
        if (paging->offset) params.emplace_back("offset", std::to_string(*paging->offset));
        if (paging->descending) params.emplace_back("order", "desc");
    }
    if (selection.tz) params.emplace_back("tz", *selection.tz);
    if (selection.columns) params.emplace_back("columns", *selection.columns);
    return params;
}

// ------------------------------------------------------------------ errors

HttpError::HttpError(int status, std::string message, std::string hint, std::string body)
    : Error("HTTP " + std::to_string(status) + ": " + message),
      status_(status),
      message_(std::move(message)),
      hint_(std::move(hint)),
      body_(std::move(body)) {}

namespace {

std::string as_text(const json::value& value) {
    if (value.is_string()) return std::string(value.get_string());
    return json::serialize(value);
}

}  // namespace

HttpError HttpError::from_response(int status, const std::string& body,
                                   const std::string& reason) {
    std::string message = reason.empty() ? std::string("error") : reason;
    std::string hint;
    boost::system::error_code ec;
    json::value parsed = json::parse(body, ec);
    if (!ec && parsed.is_object()) {
        // Mirrors the Python client: "detail" if present, else the body.
        const auto& root = parsed.get_object();
        const json::value* detail = root.if_contains("detail");
        if (!detail) detail = &parsed;
        if (detail->is_object()) {
            const auto& holder = detail->get_object();
            if (const auto* e = holder.if_contains("error")) message = as_text(*e);
            if (const auto* h = holder.if_contains("hint"); h && !h->is_null()) hint = as_text(*h);
        } else if (detail->is_string()) {
            message = std::string(detail->get_string());
        } else if (detail->is_array() && !detail->get_array().empty() &&
                   detail->get_array().front().is_object()) {
            // FastAPI's validation errors: [{"loc": [...], "msg": ...}, ...]
            const auto& first = detail->get_array().front().get_object();
            std::string where;
            if (const auto* l = first.if_contains("loc"); l && l->is_array()) {
                for (const auto& part : l->get_array()) {
                    if (!where.empty()) where += ".";
                    where += as_text(part);
                }
            }
            const auto* m = first.if_contains("msg");
            message = where + ": " + (m ? as_text(*m) : std::string("invalid"));
        }
    }
    return HttpError(status, message, hint, body);
}

// ------------------------------------------------------------------ client

Client::Client(ClientOptions options) : options_(std::move(options)) {
    detail::parse_url(options_.url);  // reject a malformed URL up front
}

Response Client::request(const std::string& method, const std::string& path,
                         const Params& params, const std::string& json_body) const {
    auto url = detail::parse_url(options_.url);
    detail::Request req;
    req.method = method;
    req.target = url.prefix + path;
    auto query = build_query(params);
    if (!query.empty()) req.target += "?" + query;
    req.body = json_body;
    req.admin_token = options_.admin_token;
    return perform(url, req, nullptr);
}

Response Client::checked(const std::string& method, const std::string& path,
                         const Params& params, const std::string& json_body) const {
    auto response = request(method, path, params, json_body);
    if (response.status >= 400)
        throw HttpError::from_response(response.status, response.body, response.reason);
    return response;
}

void Client::stream(const std::string& path, const Params& params, std::ostream& out) const {
    auto url = detail::parse_url(options_.url);
    detail::Request req;
    req.method = "GET";
    req.target = url.prefix + path;
    auto query = build_query(params);
    if (!query.empty()) req.target += "?" + query;
    req.admin_token = options_.admin_token;
    auto response = perform(url, req, &out);
    if (response.status >= 400)
        throw HttpError::from_response(response.status, response.body, response.reason);
}

Response Client::perform(const detail::Url& url, const detail::Request& request,
                         std::ostream* sink) const {
    try {
        return detail::perform(options_, url, request, sink);
    } catch (const ConnectionError& exc) {
        std::string base = options_.url;
        while (!base.empty() && base.back() == '/') base.pop_back();
        throw ConnectionError("cannot reach " + base + ": " + exc.what());
    }
}

json::value Client::get_json(const std::string& path, const Params& params) const {
    auto response = checked("GET", path, params);
    boost::system::error_code ec;
    auto value = json::parse(response.body, ec);
    if (ec) throw Error("the server's response to " + path + " is not JSON: " + ec.message());
    return value;
}

json::value Client::health() const { return get_json("/api/health"); }
json::value Client::status() const { return get_json("/api/status"); }
json::value Client::sources() const { return get_json("/api/sources"); }
json::value Client::signals() const { return get_json("/api/signals"); }
json::value Client::types() const { return get_json("/api/types"); }
json::value Client::time_help() const { return get_json("/api/time/help"); }

json::value Client::time_convert(const std::string& t, const std::optional<std::string>& tz) const {
    Params params{{"t", t}};
    if (tz) params.emplace_back("tz", *tz);
    return get_json("/api/time/convert", params);
}

json::value Client::latest(const std::optional<std::string>& signal,
                           const std::vector<std::string>& sources) const {
    Params params;
    if (signal) params.emplace_back("signal", *signal);
    for (const auto& s : sources) params.emplace_back("source", s);
    return get_json("/api/latest", params);
}

json::value Client::events(const Selection& selection, const Paging& paging) const {
    return get_json("/api/events", selection_params(selection, Format::json, &paging));
}

std::string Client::events_csv(const Selection& selection, const Paging& paging) const {
    return checked("GET", "/api/events", selection_params(selection, Format::csv, &paging)).body;
}

void Client::export_to(const Selection& selection, std::ostream& out, Format format) const {
    stream("/api/export", selection_params(selection, format), out);
}

json::value Client::admin_check() const { return get_json("/api/admin/check"); }

json::value Client::update_source(const std::string& name, std::optional<bool> enabled,
                                  std::optional<std::string> base_url) const {
    json::object body;
    if (enabled) body["enabled"] = *enabled;
    if (base_url) body["base_url"] = *base_url;
    auto response = checked("PATCH", "/api/sources/" + percent_encode(name), {},
                            json::serialize(body));
    return json::parse(response.body);
}

json::value Client::reset_source(const std::string& name) const {
    auto response = checked("POST", "/api/sources/" + percent_encode(name) + "/reset");
    return json::parse(response.body);
}

}  // namespace spill
}  // namespace darpa
