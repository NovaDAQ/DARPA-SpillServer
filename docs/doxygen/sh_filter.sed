# Doxygen input filter for shell scripts (see Doxyfile FILTER_PATTERNS).
# Doxygen has no shell parser, so *.sh is mapped to C and this filter emits
# only what C mode needs: "##" comment lines become "///" doc comments and
# "name() {" becomes "int name();" so @fn blocks have a declaration to bind
# to.  Every other line is blanked, which keeps line numbers aligned with
# the source.  Usage: sed -E -f docs/doxygen/sh_filter.sed script.sh
/^[[:space:]]*##/ {
  s/^[[:space:]]*##/\/\/\//
  b
}
/^[[:space:]]*(function[[:space:]]+)?[A-Za-z_][A-Za-z0-9_]*[[:space:]]*\(\)/ {
  s/^[[:space:]]*(function[[:space:]]+)?([A-Za-z_][A-Za-z0-9_]*).*/int \2();/
  b
}
s/.*//
