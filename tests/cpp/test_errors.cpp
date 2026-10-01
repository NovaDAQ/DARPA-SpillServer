// Decoding the server's error bodies (docs/CLIENT.md, "Errors").
#include <darpa_spill/client.hpp>

#include <cppunit/extensions/HelperMacros.h>

using namespace darpa::spill;

class ErrorTest : public CppUnit::TestFixture {
    CPPUNIT_TEST_SUITE(ErrorTest);
    CPPUNIT_TEST(detail_object);
    CPPUNIT_TEST(top_level_error);
    CPPUNIT_TEST(string_detail);
    CPPUNIT_TEST(validation_list);
    CPPUNIT_TEST(unparseable_body_uses_reason);
    CPPUNIT_TEST_SUITE_END();

public:
    void detail_object() {
        auto e = HttpError::from_response(
            400, R"({"detail":{"error":"unknown signal 'zz'","hint":"See /api/signals."}})",
            "Bad Request");
        CPPUNIT_ASSERT_EQUAL(400, e.status());
        CPPUNIT_ASSERT_EQUAL(std::string("unknown signal 'zz'"), e.message());
        CPPUNIT_ASSERT_EQUAL(std::string("See /api/signals."), e.hint());
        CPPUNIT_ASSERT_EQUAL(std::string("HTTP 400: unknown signal 'zz'"), std::string(e.what()));
    }

    void top_level_error() {
        auto e = HttpError::from_response(404, R"({"error":"no such endpoint: /api/x","hint":null})",
                                          "Not Found");
        CPPUNIT_ASSERT_EQUAL(std::string("no such endpoint: /api/x"), e.message());
        CPPUNIT_ASSERT_EQUAL(std::string(), e.hint());
    }

    void string_detail() {
        auto e = HttpError::from_response(405, R"({"detail":"Method Not Allowed"})", "x");
        CPPUNIT_ASSERT_EQUAL(std::string("Method Not Allowed"), e.message());
    }

    void validation_list() {
        auto e = HttpError::from_response(
            422, R"({"detail":[{"loc":["query","limit"],"msg":"Input should be greater than or equal to 1"},{"loc":["x"],"msg":"y"}]})",
            "Unprocessable Entity");
        CPPUNIT_ASSERT_EQUAL(std::string("query.limit: Input should be greater than or equal to 1"),
                             e.message());
    }

    void unparseable_body_uses_reason() {
        CPPUNIT_ASSERT_EQUAL(std::string("Bad Gateway"),
                             HttpError::from_response(502, "<html>", "Bad Gateway").message());
        CPPUNIT_ASSERT_EQUAL(std::string("error"), HttpError::from_response(500, "", "").message());
        CPPUNIT_ASSERT_EQUAL(std::string("Teapot"),
                             HttpError::from_response(418, R"({"detail":{}})", "Teapot").message());
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(ErrorTest);
