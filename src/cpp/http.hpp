// http.hpp -- private: one HTTP/1.1 exchange over Boost.Beast.
#ifndef DARPA_SPILL_HTTP_HPP
#define DARPA_SPILL_HTTP_HPP

#include <darpa_spill/client.hpp>

#include <iosfwd>
#include <string>

namespace darpa {
namespace spill {
namespace detail {

/// A parsed base URL.
struct Url {
    bool https = false;
    std::string host;
    std::string port;    ///< always set; 80 or 443 by default
    std::string prefix;  ///< path prefix with no trailing '/', e.g. "/spills"
};

/// Parse an http:// or https:// base URL. @throws Error
Url parse_url(const std::string& text);

/// One request to send.
struct Request {
    std::string method;
    std::string target;  ///< path and query, prefix already applied
    std::string body;    ///< sent as application/json when not empty
    std::string admin_token;
};

/// Send @p request and read the response. When @p sink is not null and the
/// status is below 400, the body is written to it as it arrives and the
/// returned Response::body is empty; otherwise the body is collected.
/// @throws ConnectionError
Response perform(const ClientOptions& options, const Url& url, const Request& request,
                 std::ostream* sink);

}  // namespace detail
}  // namespace spill
}  // namespace darpa

#endif
