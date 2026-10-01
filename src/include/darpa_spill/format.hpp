// darpa_spill/format.hpp -- the output rules the CLI shares with the Python
// client (docs/CLIENT.md, "Output"). Exposed so that other programs can render
// server responses the same way, and so that the rules are unit-tested.
#ifndef DARPA_SPILL_FORMAT_HPP
#define DARPA_SPILL_FORMAT_HPP

#include <darpa_spill/darpa_spill_export.h>

#include <boost/json/value.hpp>

#include <string>
#include <utility>
#include <vector>

namespace darpa {
namespace spill {

using Row = std::vector<std::string>;

/// printf("%.6f"), then trailing zeros and a trailing '.' removed.
DARPA_SPILL_EXPORT std::string format_fixed6(double value);

/// One JSON value as a cell: strings as is, integers in decimal, other
/// numbers via format_fixed6, yes/no, "-" for null, list items joined with
/// ",", and objects as "{}" when empty, else "key=cell,key=cell".
DARPA_SPILL_EXPORT std::string format_cell(const boost::json::value& value);

/// Number of Unicode code points in UTF-8 text (Python's len()).
DARPA_SPILL_EXPORT std::size_t display_width(const std::string& text);

/// Columns separated by two spaces, left-aligned to the widest cell,
/// trailing spaces stripped, every line ending in '\n'.
DARPA_SPILL_EXPORT std::string render_table(const Row& header, const std::vector<Row>& rows);

/// Quote a field only if it holds a comma, quote or LF; quotes doubled.
DARPA_SPILL_EXPORT std::string csv_field(const std::string& field);

/// Header and rows as CSV with '\n' line endings.
DARPA_SPILL_EXPORT std::string render_csv(const Row& header, const std::vector<Row>& rows);

/// Parse CSV text (RFC 4180 quoting; LF or CRLF line ends) into rows.
DARPA_SPILL_EXPORT std::vector<Row> parse_csv(const std::string& text);

/// Pick @p columns out of each object in a JSON array, via format_cell.
DARPA_SPILL_EXPORT std::vector<Row> rows_from_objects(const boost::json::value& list,
                                                      const Row& columns);

/// Flatten to (dotted key, cell) leaves in the server's key order; list
/// elements use their index, empty objects are "{}" and empty lists "[]".
DARPA_SPILL_EXPORT std::vector<std::pair<std::string, std::string>>
flatten(const boost::json::value& value);

/// flatten() as "key: value\n" lines.
DARPA_SPILL_EXPORT std::string render_kv(const boost::json::value& value);

/// The table and csv columns of the list commands; empty for any other name.
DARPA_SPILL_EXPORT Row list_columns(const std::string& command);

/// The column subset the events table asks for when --columns is not given.
DARPA_SPILL_EXPORT const char* default_event_table_columns();

/// Split a CSV body from the server into its data lines (every line that
/// starts with '#' removed) and the text of each "# WARNING: ..." comment,
/// both ends stripped of whitespace.
DARPA_SPILL_EXPORT void split_csv_comments(const std::string& body, std::string& data,
                                           std::vector<std::string>& warnings);

}  // namespace spill
}  // namespace darpa

#endif
