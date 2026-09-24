// c_api.cpp -- the C ABI of darpa_spill/client.h over darpa::spill::Client.
#include <darpa_spill/client.h>
#include <darpa_spill/client.hpp>

#include <cstdlib>
#include <cstring>
#include <exception>
#include <memory>
#include <new>
#include <string>

struct dsc_client {
    darpa::spill::ClientOptions options;
    std::string last_error;
};

namespace {

char* duplicate(const std::string& text) {
    char* copy = static_cast<char*>(std::malloc(text.size() + 1));
    if (copy) std::memcpy(copy, text.c_str(), text.size() + 1);
    return copy;
}

/// Split "/path?query" into the path and the already-encoded query.
void split_target(const std::string& target, std::string& path, std::string& query) {
    auto mark = target.find('?');
    path = target.substr(0, mark);
    query = mark == std::string::npos ? std::string() : target.substr(mark + 1);
}

}  // namespace

extern "C" {

const char* dsc_version(void) { return darpa::spill::version(); }

dsc_client* dsc_client_new(const char* url, double timeout) {
    if (!url) return nullptr;
    try {
        darpa::spill::ClientOptions options;
        options.url = url;
        if (timeout > 0) options.timeout = timeout;
        darpa::spill::Client probe(options);  // validates the URL
        auto* client = new dsc_client;
        client->options = probe.options();
        return client;
    } catch (...) {
        return nullptr;
    }
}

void dsc_client_free(dsc_client* client) { delete client; }

int dsc_client_set_admin_token(dsc_client* client, const char* token) {
    if (!client) return DSC_ERR_USAGE;
    client->options.admin_token = token ? token : "";
    return DSC_OK;
}

int dsc_client_set_tls(dsc_client* client, const char* ca_file, int verify) {
    if (!client) return DSC_ERR_USAGE;
    client->options.ca_file = ca_file ? ca_file : "";
    client->options.verify_tls = verify != 0;
    return DSC_OK;
}

int dsc_request(dsc_client* client, const char* method, const char* path_and_query,
                const char* json_body, char** body, long* status) {
    if (body) *body = nullptr;
    if (status) *status = 0;
    if (!client) return DSC_ERR_USAGE;
    if (!method || !path_and_query || path_and_query[0] != '/') {
        client->last_error = "method and a path starting with '/' are required";
        return DSC_ERR_USAGE;
    }
    client->last_error.clear();
    try {
        std::string path, query;
        split_target(path_and_query, path, query);
        // The query arrives encoded; pass it through by appending it to the
        // path rather than re-encoding it as parameters.
        darpa::spill::Client c(client->options);
        auto response = c.request(method, query.empty() ? path : path + "?" + query, {},
                                  json_body ? json_body : "");
        if (status) *status = response.status;
        if (body) {
            *body = duplicate(response.body);
            if (!*body) throw std::bad_alloc();
        }
        if (response.status >= 400) {
            client->last_error = darpa::spill::HttpError::from_response(
                                     response.status, response.body, response.reason)
                                     .what();
            return DSC_ERR_HTTP;
        }
        return DSC_OK;
    } catch (const darpa::spill::ConnectionError& exc) {
        client->last_error = exc.what();
        return DSC_ERR_CONNECTION;
    } catch (const std::exception& exc) {
        client->last_error = exc.what();
        return DSC_ERR_INTERNAL;
    } catch (...) {
        client->last_error = "unknown error";
        return DSC_ERR_INTERNAL;
    }
}

int dsc_get(dsc_client* client, const char* path_and_query, char** body, long* status) {
    return dsc_request(client, "GET", path_and_query, nullptr, body, status);
}

const char* dsc_last_error(const dsc_client* client) {
    return client ? client->last_error.c_str() : "no client";
}

char* dsc_percent_encode(const char* text) {
    if (!text) return nullptr;
    try {
        return duplicate(darpa::spill::percent_encode(text));
    } catch (...) {
        return nullptr;
    }
}

void dsc_string_free(char* text) { std::free(text); }

}  // extern "C"
