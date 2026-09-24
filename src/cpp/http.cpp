// http.cpp -- the Boost.Beast transport.
//
// Every operation is started asynchronously and driven by running the
// io_context to completion. That is not for concurrency: beast::tcp_stream
// applies its expiry only to asynchronous operations, so this is how a
// per-operation timeout is had without a second thread. The server streams
// query results with chunked transfer encoding; response_parser decodes it,
// and a buffer_body lets the export route be copied to its sink in constant
// memory.
#include "http.hpp"

#include <darpa_spill/version.hpp>

#include <boost/asio/connect.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>

#ifdef DARPA_SPILL_WITH_TLS
#include <boost/asio/ssl.hpp>
#include <boost/beast/ssl.hpp>
#endif

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdint>
#include <limits>
#include <ostream>
#include <sstream>

namespace darpa {
namespace spill {
namespace detail {

namespace net = boost::asio;
namespace beast = boost::beast;
namespace http = boost::beast::http;
using tcp = net::ip::tcp;
using error_code = boost::system::error_code;

Url parse_url(const std::string& text) {
    Url url;
    std::string rest;
    auto lower = text;
    std::transform(lower.begin(), lower.end(), lower.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    if (lower.rfind("http://", 0) == 0) {
        rest = text.substr(7);
    } else if (lower.rfind("https://", 0) == 0) {
        url.https = true;
        rest = text.substr(8);
    } else {
        throw Error("server URL must start with http:// or https://: " + text);
    }

    auto slash = rest.find('/');
    std::string authority = rest.substr(0, slash);
    if (slash != std::string::npos) url.prefix = rest.substr(slash);
    while (!url.prefix.empty() && url.prefix.back() == '/') url.prefix.pop_back();
    if (url.prefix.find_first_of("?#") != std::string::npos)
        throw Error("server URL must not carry a query or fragment: " + text);

    auto at = authority.rfind('@');
    if (at != std::string::npos) authority = authority.substr(at + 1);

    if (!authority.empty() && authority.front() == '[') {  // [IPv6]:port
        auto close = authority.find(']');
        if (close == std::string::npos) throw Error("malformed IPv6 host in URL: " + text);
        url.host = authority.substr(1, close - 1);
        if (close + 1 < authority.size() && authority[close + 1] == ':')
            url.port = authority.substr(close + 2);
    } else {
        auto colon = authority.rfind(':');
        url.host = authority.substr(0, colon);
        if (colon != std::string::npos) url.port = authority.substr(colon + 1);
    }
    if (url.host.empty()) throw Error("server URL has no host: " + text);
    if (url.port.empty()) url.port = url.https ? "443" : "80";
    if (!std::all_of(url.port.begin(), url.port.end(),
                     [](unsigned char c) { return std::isdigit(c) != 0; }))
        throw Error("server URL has a malformed port: " + text);
    return url;
}

namespace {

/// Runs one asynchronous operation at a time to completion.
class Driver {
public:
    net::io_context& context() { return ioc_; }

    /// Start @p initiate(handler), run until it completes, return its error.
    template <class Initiate>
    error_code run(Initiate&& initiate) {
        error_code result = net::error::would_block;
        initiate([&result](error_code ec, auto&&...) { result = ec; });
        ioc_.restart();
        ioc_.run();
        return result;
    }

private:
    net::io_context ioc_;
};

std::string authority(const Url& url) {
    std::string host = url.host.find(':') != std::string::npos ? "[" + url.host + "]" : url.host;
    bool default_port = (url.https && url.port == "443") || (!url.https && url.port == "80");
    return default_port ? host : host + ":" + url.port;
}

std::string describe(const Url& url) { return url.host + ":" + url.port; }

[[noreturn]] void fail(const Url& url, const std::string& stage, const error_code& ec,
                       double timeout) {
    std::ostringstream what;
    if (ec == beast::error::timeout || ec == net::error::timed_out ||
        ec == net::error::operation_aborted) {
        what << stage << " " << describe(url) << " timed out after " << timeout << " s";
    } else {
        what << stage << " " << describe(url) << " failed: " << ec.message();
    }
    throw ConnectionError(what.str());
}

std::chrono::steady_clock::duration as_duration(double seconds) {
    if (!(seconds > 0)) seconds = 30.0;
    return std::chrono::duration_cast<std::chrono::steady_clock::duration>(
        std::chrono::duration<double>(seconds));
}

tcp::resolver::results_type resolve(Driver& driver, const Url& url, double timeout) {
    tcp::resolver resolver(driver.context());
    net::steady_timer timer(driver.context(), as_duration(timeout));
    bool expired = false;
    timer.async_wait([&](error_code ec) {
        if (!ec) {
            expired = true;
            resolver.cancel();
        }
    });
    tcp::resolver::results_type results;
    error_code result = net::error::would_block;
    resolver.async_resolve(url.host, url.port,
                           [&](error_code ec, tcp::resolver::results_type r) {
                               result = ec;
                               results = std::move(r);
                               timer.cancel();
                           });
    driver.context().restart();
    driver.context().run();
    if (expired) fail(url, "resolving", beast::error::timeout, timeout);
    if (result) fail(url, "resolving", result, timeout);
    return results;
}

http::request<http::string_body> build(const Url& url, const Request& request) {
    http::request<http::string_body> req;
    req.method_string(request.method);
    req.target(request.target);
    req.version(11);
    req.set(http::field::host, authority(url));
    req.set(http::field::user_agent, std::string("darpa-spill-client-cpp/") + DARPA_SPILL_VERSION);
    req.set(http::field::accept, "*/*");
    req.keep_alive(false);
    if (!request.admin_token.empty()) req.set("X-Admin-Token", request.admin_token);
    if (!request.body.empty()) {
        req.set(http::field::content_type, "application/json");
        req.body() = request.body;
    }
    req.prepare_payload();
    return req;
}

/// Write the request and read the response over an established stream.
/// @p lowest is the tcp_stream carrying the expiry.
template <class Stream>
Response exchange(Driver& driver, Stream& stream, beast::tcp_stream& lowest, const Url& url,
                  const http::request<http::string_body>& req, double timeout,
                  std::ostream* sink) {
    const auto limit = as_duration(timeout);

    lowest.expires_after(limit);
    if (auto ec = driver.run([&](auto handler) { http::async_write(stream, req, handler); }))
        fail(url, "sending to", ec, timeout);

    beast::flat_buffer buffer;
    http::response_parser<http::buffer_body> parser;
    parser.body_limit((std::numeric_limits<std::uint64_t>::max)());

    lowest.expires_after(limit);
    if (auto ec = driver.run(
            [&](auto handler) { http::async_read_header(stream, buffer, parser, handler); }))
        fail(url, "reading from", ec, timeout);

    Response response;
    response.status = static_cast<int>(parser.get().result_int());
    response.reason = std::string(parser.get().reason());
    response.content_type = std::string(parser.get()[http::field::content_type]);
    const bool streaming = sink != nullptr && response.status < 400;

    char chunk[16384];
    while (!parser.is_done()) {
        parser.get().body().data = chunk;
        parser.get().body().size = sizeof chunk;
        lowest.expires_after(limit);
        auto ec = driver.run(
            [&](auto handler) { http::async_read(stream, buffer, parser, handler); });
        if (ec == http::error::need_buffer) ec = {};
        if (ec) fail(url, "reading from", ec, timeout);
        const auto used = sizeof chunk - parser.get().body().size;
        if (used == 0) continue;
        if (streaming) {
            sink->write(chunk, static_cast<std::streamsize>(used));
            if (!*sink) throw Error("cannot write the response body to its destination");
        } else {
            response.body.append(chunk, used);
        }
    }
    if (streaming) sink->flush();
    return response;
}

}  // namespace

Response perform(const ClientOptions& options, const Url& url, const Request& request,
                 std::ostream* sink) {
    Driver driver;
    const double timeout = options.timeout;
    auto results = resolve(driver, url, timeout);
    auto req = build(url, request);

    if (!url.https) {
        beast::tcp_stream stream(driver.context());
        stream.expires_after(as_duration(timeout));
        if (auto ec = driver.run([&](auto handler) { stream.async_connect(results, handler); }))
            fail(url, "connecting to", ec, timeout);
        auto response = exchange(driver, stream, stream, url, req, timeout, sink);
        error_code ignored;
        stream.socket().shutdown(tcp::socket::shutdown_both, ignored);
        return response;
    }

#ifdef DARPA_SPILL_WITH_TLS
    namespace ssl = net::ssl;
    ssl::context ctx(ssl::context::tls_client);
    try {
        // With verification off no CA is consulted, so a missing or bad
        // bundle must not stop --insecure from connecting.
        if (options.verify_tls && !options.ca_file.empty())
            ctx.load_verify_file(options.ca_file);
        else if (options.verify_tls)
            ctx.set_default_verify_paths();
    } catch (const boost::system::system_error& exc) {
        throw ConnectionError("cannot load CA certificates " +
                              (options.ca_file.empty() ? std::string("(system store)")
                                                       : options.ca_file) +
                              ": " + exc.code().message());
    }

    beast::ssl_stream<beast::tcp_stream> stream(driver.context(), ctx);
    if (!SSL_set_tlsext_host_name(stream.native_handle(), url.host.c_str()))
        throw ConnectionError("cannot set the TLS server name for " + url.host);
    if (options.verify_tls) {
        stream.set_verify_mode(ssl::verify_peer);
        stream.set_verify_callback(ssl::host_name_verification(url.host));
    } else {
        stream.set_verify_mode(ssl::verify_none);
    }

    auto& lowest = beast::get_lowest_layer(stream);
    lowest.expires_after(as_duration(timeout));
    if (auto ec = driver.run([&](auto handler) { lowest.async_connect(results, handler); }))
        fail(url, "connecting to", ec, timeout);
    lowest.expires_after(as_duration(timeout));
    if (auto ec = driver.run(
            [&](auto handler) { stream.async_handshake(ssl::stream_base::client, handler); }))
        fail(url, "TLS handshake with", ec, timeout);

    auto response = exchange(driver, stream, lowest, url, req, timeout, sink);
    lowest.expires_after(std::chrono::seconds(2));
    driver.run([&](auto handler) { stream.async_shutdown(handler); });  // best effort
    return response;
#else
    (void)sink;
    throw ConnectionError("this build of the client has no TLS support; rebuild with "
                          "-DDARPA_SPILL_WITH_TLS=ON to reach " + describe(url));
#endif
}

}  // namespace detail
}  // namespace spill
}  // namespace darpa
