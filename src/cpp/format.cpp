// format.cpp -- the output rules of docs/CLIENT.md, "Output".
#include <darpa_spill/format.hpp>

#include <boost/json.hpp>

#include <algorithm>
#include <cstdio>

namespace darpa {
namespace spill {

namespace json = boost::json;

std::string format_fixed6(double value) {
    char buffer[512];
    std::snprintf(buffer, sizeof buffer, "%.6f", value);
    std::string text(buffer);
    if (text.find('.') != std::string::npos) {
        while (!text.empty() && text.back() == '0') text.pop_back();
        if (!text.empty() && text.back() == '.') text.pop_back();
    }
    return text;
}

std::string format_cell(const json::value& value) {
    switch (value.kind()) {
    case json::kind::string: return std::string(value.get_string());
    case json::kind::int64: return std::to_string(value.get_int64());
    case json::kind::uint64: return std::to_string(value.get_uint64());
    case json::kind::double_: return format_fixed6(value.get_double());
    case json::kind::bool_: return value.get_bool() ? "yes" : "no";
    case json::kind::null: return "-";
    case json::kind::array: {
        std::string joined;
        const auto& items = value.get_array();
        for (std::size_t i = 0; i < items.size(); ++i) {
            if (i) joined.push_back(',');
            joined += format_cell(items[i]);
        }
        return joined;
    }
    case json::kind::object: {
        const auto& object = value.get_object();
        if (object.empty()) return "{}";
        std::string joined;
        for (const auto& member : object) {
            if (!joined.empty()) joined.push_back(',');
            joined += std::string(member.key()) + "=" + format_cell(member.value());
        }
        return joined;
    }
    }
    return {};
}

std::size_t display_width(const std::string& text) {
    std::size_t width = 0;
    for (unsigned char c : text)
        if ((c & 0xC0) != 0x80) ++width;  // count every byte that starts a code point
    return width;
}

std::string render_table(const Row& header, const std::vector<Row>& rows) {
    std::size_t columns = header.size();
    for (const auto& row : rows) columns = std::max(columns, row.size());
    std::vector<std::size_t> widths(columns, 0);
    auto measure = [&](const Row& row) {
        for (std::size_t i = 0; i < row.size(); ++i)
            widths[i] = std::max(widths[i], display_width(row[i]));
    };
    measure(header);
    for (const auto& row : rows) measure(row);

    std::string out;
    auto emit = [&](const Row& row) {
        std::string line;
        for (std::size_t i = 0; i < columns; ++i) {
            const std::string cell = i < row.size() ? row[i] : std::string();
            if (i) line += "  ";
            line += cell;
            if (i + 1 < columns) line.append(widths[i] - display_width(cell), ' ');
        }
        while (!line.empty() && line.back() == ' ') line.pop_back();
        out += line;
        out.push_back('\n');
    };
    emit(header);
    for (const auto& row : rows) emit(row);
    return out;
}

std::string csv_field(const std::string& field) {
    if (field.find_first_of(",\"\n") == std::string::npos) return field;
    std::string out = "\"";
    for (char c : field) {
        if (c == '"') out.push_back('"');
        out.push_back(c);
    }
    out.push_back('"');
    return out;
}

std::string render_csv(const Row& header, const std::vector<Row>& rows) {
    std::string out;
    auto emit = [&](const Row& row) {
        for (std::size_t i = 0; i < row.size(); ++i) {
            if (i) out.push_back(',');
            out += csv_field(row[i]);
        }
        out.push_back('\n');
    };
    emit(header);
    for (const auto& row : rows) emit(row);
    return out;
}

std::vector<Row> parse_csv(const std::string& text) {
    std::vector<Row> rows;
    Row row;
    std::string field;
    bool quoted = false;
    bool any = false;  // the current row has content
    for (std::size_t i = 0; i < text.size(); ++i) {
        char c = text[i];
        if (quoted) {
            if (c == '"') {
                if (i + 1 < text.size() && text[i + 1] == '"') {
                    field.push_back('"');
                    ++i;
                } else {
                    quoted = false;
                }
            } else {
                field.push_back(c);
            }
            continue;
        }
        if (c == '"') {
            quoted = true;
            any = true;
        } else if (c == ',') {
            row.push_back(std::move(field));
            field.clear();
            any = true;
        } else if (c == '\r' || c == '\n') {
            if (c == '\r' && i + 1 < text.size() && text[i + 1] == '\n') ++i;
            if (any || !field.empty()) {
                row.push_back(std::move(field));
                rows.push_back(std::move(row));
            }
            row.clear();
            field.clear();
            any = false;
        } else {
            field.push_back(c);
            any = true;
        }
    }
    if (any || !field.empty()) {
        row.push_back(std::move(field));
        rows.push_back(std::move(row));
    }
    return rows;
}

std::vector<Row> rows_from_objects(const json::value& list, const Row& columns) {
    std::vector<Row> rows;
    if (!list.is_array()) return rows;
    for (const auto& item : list.get_array()) {
        Row row;
        const json::object* object = item.if_object();
        for (const auto& column : columns) {
            const json::value* cell = object ? object->if_contains(column) : nullptr;
            row.push_back(cell ? format_cell(*cell) : "-");
        }
        rows.push_back(std::move(row));
    }
    return rows;
}

namespace {

void flatten_into(const json::value& value, const std::string& key,
                  std::vector<std::pair<std::string, std::string>>& out) {
    auto join = [&](const std::string& part) { return key.empty() ? part : key + "." + part; };
    if (value.is_object()) {
        const auto& object = value.get_object();
        if (object.empty()) {
            if (!key.empty()) out.emplace_back(key, "{}");
            return;
        }
        for (const auto& member : object)
            flatten_into(member.value(), join(std::string(member.key())), out);
    } else if (value.is_array()) {
        const auto& array = value.get_array();
        if (array.empty()) {
            out.emplace_back(key, "[]");
            return;
        }
        for (std::size_t i = 0; i < array.size(); ++i)
            flatten_into(array[i], join(std::to_string(i)), out);
    } else {
        out.emplace_back(key, format_cell(value));
    }
}

}  // namespace

std::vector<std::pair<std::string, std::string>> flatten(const json::value& value) {
    std::vector<std::pair<std::string, std::string>> out;
    flatten_into(value, "", out);
    return out;
}

std::string render_kv(const json::value& value) {
    std::string out;
    for (const auto& kv : flatten(value)) {
        out += kv.first;
        out += ": ";
        out += kv.second;
        out.push_back('\n');
    }
    return out;
}

Row list_columns(const std::string& command) {
    if (command == "sources") return {"name", "base_url", "enabled", "overridden", "events"};
    if (command == "signals") return {"hex", "name", "spill_type_name", "description"};
    if (command == "types") return {"value", "name", "ambiguous", "signals"};
    return {};
}

const char* default_event_table_columns() {
    return "utc_string,gps_week,gps_tow_exact,signal,spill_type_name,event_number,source";
}

void split_csv_comments(const std::string& body, std::string& data,
                        std::vector<std::string>& warnings) {
    static const char* const space = " \t\r\n\f\v";
    auto strip = [](std::string text) {
        auto first = text.find_first_not_of(space);
        if (first == std::string::npos) return std::string();
        return text.substr(first, text.find_last_not_of(space) - first + 1);
    };
    data.clear();
    warnings.clear();
    std::size_t pos = 0;
    while (pos < body.size()) {
        auto end = body.find('\n', pos);
        auto next = end == std::string::npos ? body.size() : end + 1;
        std::string line = body.substr(pos, next - pos);
        if (!line.empty() && line[0] == '#') {
            std::string comment = strip(line.substr(1));
            if (comment.compare(0, 8, "WARNING:") == 0) warnings.push_back(strip(comment.substr(8)));
        } else {
            data += line;
        }
        pos = next;
    }
}

}  // namespace spill
}  // namespace darpa
