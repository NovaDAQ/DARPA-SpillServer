// Percent-encoding and query parameter order (docs/CLIENT.md, "Commands").
#include <darpa_spill/client.hpp>
#include <darpa_spill/client.h>

#include <cppunit/extensions/HelperMacros.h>

#include <cstring>

using namespace darpa::spill;

class QueryTest : public CppUnit::TestFixture {
    CPPUNIT_TEST_SUITE(QueryTest);
    CPPUNIT_TEST(unreserved_pass_through);
    CPPUNIT_TEST(dollar_and_space_are_encoded);
    CPPUNIT_TEST(utf8_is_encoded_bytewise);
    CPPUNIT_TEST(query_joins_in_order);
    CPPUNIT_TEST(empty_query);
    CPPUNIT_TEST(selection_order_is_fixed);
    CPPUNIT_TEST(selection_omits_unset);
    CPPUNIT_TEST(c_api_encodes);
    CPPUNIT_TEST(c_api_rejects_bad_url);
    CPPUNIT_TEST(client_rejects_bad_url);
    CPPUNIT_TEST_SUITE_END();

public:
    void unreserved_pass_through() {
        CPPUNIT_ASSERT_EQUAL(std::string("AZaz09-._~"), percent_encode("AZaz09-._~"));
    }

    void dollar_and_space_are_encoded() {
        CPPUNIT_ASSERT_EQUAL(std::string("%2474"), percent_encode("$74"));
        CPPUNIT_ASSERT_EQUAL(std::string("a%20b%2Cc%2Fd%3F%26%3D%2B"), percent_encode("a b,c/d?&=+"));
    }

    void utf8_is_encoded_bytewise() {
        CPPUNIT_ASSERT_EQUAL(std::string("%C2%B5s"), percent_encode("\xC2\xB5s"));
    }

    void query_joins_in_order() {
        Params p{{"signal", "$74"}, {"signal", "$8F"}, {"t", "-2h"}};
        CPPUNIT_ASSERT_EQUAL(std::string("signal=%2474&signal=%248F&t=-2h"), build_query(p));
    }

    void empty_query() { CPPUNIT_ASSERT_EQUAL(std::string(), build_query({})); }

    void selection_order_is_fixed() {
        Selection s;
        s.columns = "utc,signal";
        s.tz = "America/Chicago";
        s.sources = {"a", "b"};
        s.types = {"NUMI"};
        s.signals = {"$74", "$8f"};
        s.end = "now";
        s.start = "today";
        Paging paging;
        paging.limit = 10;
        paging.offset = 5;
        paging.descending = true;
        auto params = selection_params(s, Format::csv, &paging);
        CPPUNIT_ASSERT_EQUAL(std::string(
            "start=today&end=now&signal=%2474&signal=%248f&type=NUMI&source=a&source=b"
            "&format=csv&limit=10&offset=5&order=desc&tz=America%2FChicago&columns=utc%2Csignal"),
            build_query(params));
    }

    void selection_omits_unset() {
        Selection s;
        CPPUNIT_ASSERT_EQUAL(std::string("format=json"), build_query(selection_params(s, Format::json)));
        CPPUNIT_ASSERT_EQUAL(std::string(), build_query(selection_params(s, std::nullopt)));
    }

    void c_api_encodes() {
        char* text = dsc_percent_encode("$8F");
        CPPUNIT_ASSERT(text != nullptr);
        CPPUNIT_ASSERT_EQUAL(std::string("%248F"), std::string(text));
        dsc_string_free(text);
        CPPUNIT_ASSERT(dsc_percent_encode(nullptr) == nullptr);
        CPPUNIT_ASSERT_EQUAL(std::string(version()), std::string(dsc_version()));
    }

    void c_api_rejects_bad_url() {
        CPPUNIT_ASSERT(dsc_client_new("ftp://x", 1.0) == nullptr);
        CPPUNIT_ASSERT(dsc_client_new(nullptr, 1.0) == nullptr);
        dsc_client* c = dsc_client_new("http://localhost:1/prefix/", 1.0);
        CPPUNIT_ASSERT(c != nullptr);
        CPPUNIT_ASSERT_EQUAL(static_cast<int>(DSC_ERR_USAGE), dsc_get(c, "api/health", nullptr, nullptr));
        dsc_client_free(c);
    }

    void client_rejects_bad_url() {
        ClientOptions o;
        o.url = "localhost:8080";
        CPPUNIT_ASSERT_THROW(Client{o}, Error);
        o.url = "http://:80";
        CPPUNIT_ASSERT_THROW(Client{o}, Error);
        o.url = "http://host:8x";
        CPPUNIT_ASSERT_THROW(Client{o}, Error);
        o.url = "https://[::1]:8443/spills";
        CPPUNIT_ASSERT_NO_THROW(Client{o});
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(QueryTest);
