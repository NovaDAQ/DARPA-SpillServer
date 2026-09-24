// darpa_spill/client.hpp -- C++ client for the DARPA Spill Information Server.
//
// A thin, synchronous HTTP/1.1 client over Boost.Beast for the server's
// /api routes (docs/API.md). Every call opens one connection, sends one
// request and reads the whole response, or streams it to an std::ostream for
// the export route, whose body can be arbitrarily large.
//
// The contract shared with the Python client -- parameter names and order,
// percent-encoding, error decoding -- is docs/CLIENT.md.
#ifndef DARPA_SPILL_CLIENT_HPP
#define DARPA_SPILL_CLIENT_HPP

#include <darpa_spill/darpa_spill_export.h>

#include <boost/json/value.hpp>

#include <cstdint>
#include <iosfwd>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace darpa {
namespace spill {

namespace detail {
struct Url;
struct Request;
}  // namespace detail

/// The library version, e.g. "1.3.0".
DARPA_SPILL_EXPORT const char* version();

/// Query parameters in the order they are sent. Repeated keys are allowed.
using Params = std::vector<std::pair<std::string, std::string>>;

/// Percent-encode per RFC 3986: only A-Z a-z 0-9 - . _ ~ pass through.
DARPA_SPILL_EXPORT std::string percent_encode(const std::string& text);

/// Render params as "k=v&k=v", both sides percent-encoded. Empty for none.
DARPA_SPILL_EXPORT std::string build_query(const Params& params);

/// Response body format for events and export.
enum class Format { json, csv };

/// Connection settings.
struct ClientOptions {
    std::string url = "http://localhost:8080";  ///< base URL, may carry a path prefix
    double timeout = 30.0;                      ///< seconds, per network operation
    std::string admin_token;                    ///< sent as X-Admin-Token when non-empty
    std::string ca_file;                        ///< PEM bundle for https; empty = system store
    bool verify_tls = true;                     ///< false accepts any certificate
};

/// The event selection shared by /api/events, /api/export and /api/latest.
struct Selection {
    std::optional<std::string> start;  ///< any time expression; see /api/time/help
    std::optional<std::string> end;
    std::vector<std::string> signals;  ///< e.g. "$74", "one-hertz"
    std::vector<std::string> types;    ///< spill type name or number
    std::vector<std::string> sources;  ///< TDU source names
    std::optional<std::string> tz;     ///< zone for inputs that carry none
    std::optional<std::string> columns;  ///< comma-separated column subset
};

/// Paging for /api/events.
struct Paging {
    std::optional<std::int64_t> limit;
    std::optional<std::int64_t> offset;
    bool descending = false;
};

/// Build the /api/events (or, with no paging, /api/export) parameters in the
/// order docs/CLIENT.md fixes: start, end, signal..., type..., source...,
/// format, limit, offset, order, tz, columns.
DARPA_SPILL_EXPORT Params selection_params(const Selection& selection,
                                           std::optional<Format> format,
                                           const Paging* paging = nullptr);

/// A complete HTTP response.
struct Response {
    int status = 0;
    std::string reason;  ///< the status line's reason phrase, e.g. "Bad Request"
    std::string content_type;
    std::string body;
};

/// Base of every error the client throws.
class DARPA_SPILL_EXPORT Error : public std::runtime_error {
public:
    explicit Error(const std::string& what) : std::runtime_error(what) {}
};

/// The server could not be reached: DNS, connect, TLS or timeout. what() is
/// "cannot reach <url>: <reason>".
class DARPA_SPILL_EXPORT ConnectionError : public Error {
public:
    explicit ConnectionError(const std::string& what) : Error(what) {}
};

/// The server answered with a status of 400 or above.
class DARPA_SPILL_EXPORT HttpError : public Error {
public:
    HttpError(int status, std::string message, std::string hint, std::string body);

    /// Decode {"detail": {"error", "hint"}}, {"error"}, a string detail, or
    /// FastAPI's validation list (first item as "<loc.joined>: <msg>");
    /// otherwise the message is @p reason.
    static HttpError from_response(int status, const std::string& body,
                                   const std::string& reason = {});

    int status() const noexcept { return status_; }
    const std::string& message() const noexcept { return message_; }
    const std::string& hint() const noexcept { return hint_; }
    const std::string& body() const noexcept { return body_; }

private:
    int status_;
    std::string message_;
    std::string hint_;
    std::string body_;
};

/// The client. Cheap to construct; holds no connection between calls.
class DARPA_SPILL_EXPORT Client {
public:
    explicit Client(ClientOptions options);

    const ClientOptions& options() const noexcept { return options_; }

    // -- raw access ------------------------------------------------------

    /// Send one request and return the response, whatever its status.
    /// @param path  route below the base URL, e.g. "/api/status"
    /// @param json_body  sent as application/json when non-empty
    /// @throws ConnectionError
    Response request(const std::string& method, const std::string& path,
                     const Params& params = {},
                     const std::string& json_body = {}) const;

    /// request(), throwing HttpError for a status of 400 or above.
    Response checked(const std::string& method, const std::string& path,
                     const Params& params = {},
                     const std::string& json_body = {}) const;

    /// GET and stream the body to @p out as it arrives.
    /// @throws HttpError before writing anything if the status is an error.
    void stream(const std::string& path, const Params& params, std::ostream& out) const;

    // -- typed routes ----------------------------------------------------

    boost::json::value health() const;
    boost::json::value status() const;
    boost::json::value sources() const;
    boost::json::value signals() const;
    boost::json::value types() const;
    boost::json::value time_convert(const std::string& t,
                                    const std::optional<std::string>& tz = std::nullopt) const;
    boost::json::value time_help() const;
    boost::json::value latest(const std::optional<std::string>& signal = std::nullopt,
                              const std::vector<std::string>& sources = {}) const;

    /// /api/events as parsed JSON: {"meta": ..., "events": [...]}.
    boost::json::value events(const Selection& selection, const Paging& paging = {}) const;
    /// /api/events as CSV text, including the # comment header.
    std::string events_csv(const Selection& selection, const Paging& paging = {}) const;
    /// /api/export streamed to @p out.
    void export_to(const Selection& selection, std::ostream& out,
                   Format format = Format::csv) const;

    boost::json::value admin_check() const;
    boost::json::value update_source(const std::string& name,
                                     std::optional<bool> enabled,
                                     std::optional<std::string> base_url) const;
    boost::json::value reset_source(const std::string& name) const;

private:
    Response perform(const detail::Url& url, const detail::Request& request,
                     std::ostream* sink) const;
    boost::json::value get_json(const std::string& path, const Params& params = {}) const;

    ClientOptions options_;
};

}  // namespace spill
}  // namespace darpa

#endif
