/* darpa_spill/client.h -- C interface to the DARPA Spill Information Server
 * client library.
 *
 * A C ABI over darpa::spill::Client for programs that are not C++. Requests
 * take a path with its query string already encoded (dsc_percent_encode does
 * the encoding) and return the response body as a NUL-terminated string the
 * caller releases with dsc_string_free. No function throws.
 *
 *     dsc_client *c = dsc_client_new("http://localhost:8080", 30.0);
 *     char *body = NULL;
 *     long status = 0;
 *     if (dsc_get(c, "/api/latest", &body, &status) == DSC_OK)
 *         puts(body);
 *     dsc_string_free(body);
 *     dsc_client_free(c);
 */
#ifndef DARPA_SPILL_CLIENT_H
#define DARPA_SPILL_CLIENT_H

#include <darpa_spill/darpa_spill_export.h>

#ifdef __cplusplus
extern "C" {
#endif

/** Return codes. They match the exit status of the command-line clients. */
enum dsc_result {
    DSC_OK = 0,             /**< success, HTTP status below 400 */
    DSC_ERR_HTTP = 1,       /**< the server answered with a status of 400 or above */
    DSC_ERR_USAGE = 2,      /**< a NULL or malformed argument */
    DSC_ERR_CONNECTION = 3, /**< connect, TLS or timeout failure */
    DSC_ERR_INTERNAL = 4    /**< anything else, e.g. out of memory */
};

/** Opaque client handle. */
typedef struct dsc_client dsc_client;

/** Library version string, e.g. "1.3.0". */
DARPA_SPILL_EXPORT const char *dsc_version(void);

/** Create a client for @p url with a per-operation timeout in seconds.
 *  Returns NULL if @p url is NULL or not an http:// or https:// URL. */
DARPA_SPILL_EXPORT dsc_client *dsc_client_new(const char *url, double timeout);

/** Release a client. NULL is ignored. */
DARPA_SPILL_EXPORT void dsc_client_free(dsc_client *client);

/** Send @p token as X-Admin-Token on later requests; NULL or "" clears it. */
DARPA_SPILL_EXPORT int dsc_client_set_admin_token(dsc_client *client, const char *token);

/** Verify https servers against @p ca_file (NULL for the system store), or
 *  not at all when @p verify is 0. */
DARPA_SPILL_EXPORT int dsc_client_set_tls(dsc_client *client, const char *ca_file, int verify);

/** GET @p path_and_query, e.g. "/api/latest?signal=%2474".
 *  On DSC_OK and DSC_ERR_HTTP, *body receives the response body (free it with
 *  dsc_string_free) and *status the HTTP status; either pointer may be NULL.
 *  On failure dsc_last_error describes what went wrong. */
DARPA_SPILL_EXPORT int dsc_get(dsc_client *client, const char *path_and_query,
                               char **body, long *status);

/** Send any request. @p json_body, when not NULL, is sent as application/json. */
DARPA_SPILL_EXPORT int dsc_request(dsc_client *client, const char *method,
                                   const char *path_and_query, const char *json_body,
                                   char **body, long *status);

/** Description of the last failure on @p client; "" if none. Valid until the
 *  next call on the same client. */
DARPA_SPILL_EXPORT const char *dsc_last_error(const dsc_client *client);

/** Percent-encode @p text per RFC 3986 for use in a query string. Free the
 *  result with dsc_string_free. Returns NULL for NULL input. */
DARPA_SPILL_EXPORT char *dsc_percent_encode(const char *text);

/** Release a string returned by this library. NULL is ignored. */
DARPA_SPILL_EXPORT void dsc_string_free(char *text);

#ifdef __cplusplus
}
#endif

#endif
