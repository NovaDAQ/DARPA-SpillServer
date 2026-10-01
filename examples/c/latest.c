/* latest.c -- print the newest event a DARPA Spill Information Server holds,
 * using the library's C interface.
 *
 *     example_c_latest [URL] [SIGNAL]
 *     example_c_latest http://localhost:8080 '$74'
 *
 * Exit status is the dsc_result code, which matches the command-line clients.
 */
#include <darpa_spill/client.h>

#include <stdio.h>
#include <string.h>

int main(int argc, char **argv) {
    const char *url = argc > 1 ? argv[1] : "http://localhost:8080";
    const char *signal = argc > 2 ? argv[2] : NULL;
    char path[512] = "/api/latest";
    char *body = NULL;
    long status = 0;
    int rc;

    dsc_client *client = dsc_client_new(url, 30.0);
    if (client == NULL) {
        fprintf(stderr, "error: not an http:// or https:// URL: %s\n", url);
        return DSC_ERR_USAGE;
    }

    if (signal != NULL) {
        char *encoded = dsc_percent_encode(signal);
        if (encoded == NULL || strlen(encoded) > sizeof path - 32) {
            dsc_string_free(encoded);
            dsc_client_free(client);
            return DSC_ERR_USAGE;
        }
        snprintf(path, sizeof path, "/api/latest?signal=%s", encoded);
        dsc_string_free(encoded);
    }

    rc = dsc_get(client, path, &body, &status);
    if (rc == DSC_OK) {
        printf("%s\n", body);
    } else {
        fprintf(stderr, "error: %s\n", dsc_last_error(client));
    }
    dsc_string_free(body);
    dsc_client_free(client);
    return rc;
}
