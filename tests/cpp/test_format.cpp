// Output rules (docs/CLIENT.md, "Output").
#include <darpa_spill/format.hpp>

#include <boost/json.hpp>

#include <cppunit/extensions/HelperMacros.h>

using namespace darpa::spill;
namespace json = boost::json;

class FormatTest : public CppUnit::TestFixture {
    CPPUNIT_TEST_SUITE(FormatTest);
    CPPUNIT_TEST(fixed6_trims);
    CPPUNIT_TEST(cells);
    CPPUNIT_TEST(table_alignment);
    CPPUNIT_TEST(table_width_counts_code_points);
    CPPUNIT_TEST(csv_quoting);
    CPPUNIT_TEST(csv_parsing);
    CPPUNIT_TEST(flatten_rules);
    CPPUNIT_TEST(rows_pick_columns);
    CPPUNIT_TEST(comments_split);
    CPPUNIT_TEST_SUITE_END();

public:
    void fixed6_trims() {
        CPPUNIT_ASSERT_EQUAL(std::string("1025136016.123457"), format_fixed6(1025136016.1234567));
        CPPUNIT_ASSERT_EQUAL(std::string("2.5"), format_fixed6(2.5));
        CPPUNIT_ASSERT_EQUAL(std::string("3"), format_fixed6(3.0));
        CPPUNIT_ASSERT_EQUAL(std::string("0"), format_fixed6(1e-9));
        CPPUNIT_ASSERT_EQUAL(std::string("-0"), format_fixed6(-1e-9));
        CPPUNIT_ASSERT_EQUAL(std::string("100"), format_fixed6(100.0));
    }

    void cells() {
        CPPUNIT_ASSERT_EQUAL(std::string("text"), format_cell(json::value("text")));
        CPPUNIT_ASSERT_EQUAL(std::string("33778458638680249"),
                             format_cell(json::parse("33778458638680249")));
        CPPUNIT_ASSERT_EQUAL(std::string("-7"), format_cell(json::value(-7)));
        CPPUNIT_ASSERT_EQUAL(std::string("1.5"), format_cell(json::parse("1.5")));
        CPPUNIT_ASSERT_EQUAL(std::string("1"), format_cell(json::parse("1.0")));
        CPPUNIT_ASSERT_EQUAL(std::string("yes"), format_cell(json::value(true)));
        CPPUNIT_ASSERT_EQUAL(std::string("no"), format_cell(json::value(false)));
        CPPUNIT_ASSERT_EQUAL(std::string("-"), format_cell(json::value(nullptr)));
        CPPUNIT_ASSERT_EQUAL(std::string("$74,$8F"), format_cell(json::parse("[\"$74\",\"$8F\"]")));
        CPPUNIT_ASSERT_EQUAL(std::string(""), format_cell(json::parse("[]")));
        CPPUNIT_ASSERT_EQUAL(std::string("{}"), format_cell(json::parse("{}")));
        CPPUNIT_ASSERT_EQUAL(std::string("a=1,b=yes"), format_cell(json::parse("{\"a\":1,\"b\":true}")));
    }

    void table_alignment() {
        Row header{"name", "enabled"};
        std::vector<Row> rows{{"tdu-01", "yes"}, {"x", ""}};
        CPPUNIT_ASSERT_EQUAL(std::string("name    enabled\n"
                                         "tdu-01  yes\n"
                                         "x\n"),
                             render_table(header, rows));
        CPPUNIT_ASSERT_EQUAL(std::string("a\n"), render_table({"a"}, {}));
    }

    void table_width_counts_code_points() {
        CPPUNIT_ASSERT_EQUAL(std::size_t(2), display_width("\xC2\xB5s"));
        Row header{"u", "v"};
        std::vector<Row> rows{{"\xC2\xB5s", "1"}};
        CPPUNIT_ASSERT_EQUAL(std::string("u   v\n\xC2\xB5s  1\n"), render_table(header, rows));
    }

    void csv_quoting() {
        CPPUNIT_ASSERT_EQUAL(std::string("plain"), csv_field("plain"));
        CPPUNIT_ASSERT_EQUAL(std::string("\"a,b\""), csv_field("a,b"));
        CPPUNIT_ASSERT_EQUAL(std::string("\"say \"\"hi\"\"\""), csv_field("say \"hi\""));
        CPPUNIT_ASSERT_EQUAL(std::string("\"two\nlines\""), csv_field("two\nlines"));
        CPPUNIT_ASSERT_EQUAL(std::string("value,signals\n1,\"$74,$8F\"\n"),
                             render_csv({"value", "signals"}, {{"1", "$74,$8F"}}));
    }

    void csv_parsing() {
        auto rows = parse_csv("a,b,c\r\n1,\"x, \"\"y\"\"\",\n\n2,,z\n");
        CPPUNIT_ASSERT_EQUAL(std::size_t(3), rows.size());
        CPPUNIT_ASSERT_EQUAL(std::string("x, \"y\""), rows[1][1]);
        CPPUNIT_ASSERT_EQUAL(std::string(""), rows[1][2]);
        CPPUNIT_ASSERT_EQUAL(std::size_t(3), rows[2].size());
        CPPUNIT_ASSERT(parse_csv("").empty());
        CPPUNIT_ASSERT_EQUAL(std::size_t(1), parse_csv("h").size());
    }

    void flatten_rules() {
        auto value = json::parse(
            R"({"status":"ok","archive":{"events":65,"by_type":{}},"sources":[{"name":"a"}],)"
            R"("warnings":[],"gps":1.25,"none":null})");
        CPPUNIT_ASSERT_EQUAL(std::string("status: ok\n"
                                         "archive.events: 65\n"
                                         "archive.by_type: {}\n"
                                         "sources.0.name: a\n"
                                         "warnings: []\n"
                                         "gps: 1.25\n"
                                         "none: -\n"),
                             render_kv(value));
        CPPUNIT_ASSERT_EQUAL(std::string(), render_kv(json::parse("{}")));
        CPPUNIT_ASSERT_EQUAL(std::string(": []\n"), render_kv(json::parse("[]")));
    }

    void rows_pick_columns() {
        auto list = json::parse(R"([{"value":1,"name":"NUMI","ambiguous":false,"signals":["$74"]},)"
                                R"({"name":"X"}])");
        auto rows = rows_from_objects(list, list_columns("types"));
        CPPUNIT_ASSERT_EQUAL(std::size_t(2), rows.size());
        CPPUNIT_ASSERT_EQUAL(std::string("no"), rows[0][2]);
        CPPUNIT_ASSERT_EQUAL(std::string("-"), rows[1][0]);
        CPPUNIT_ASSERT(list_columns("health").empty());
        CPPUNIT_ASSERT_EQUAL(std::size_t(5), list_columns("sources").size());
    }

    void comments_split() {
        std::string data;
        std::vector<std::string> warnings;
        split_csv_comments("# DARPA\n# WARNING: sampled only \n#WARNING:tight\nh1,h2\n1,2\n", data,
                           warnings);
        CPPUNIT_ASSERT_EQUAL(std::string("h1,h2\n1,2\n"), data);
        CPPUNIT_ASSERT_EQUAL(std::size_t(2), warnings.size());
        CPPUNIT_ASSERT_EQUAL(std::string("sampled only"), warnings[0]);
        CPPUNIT_ASSERT_EQUAL(std::string("tight"), warnings[1]);
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(FormatTest);
