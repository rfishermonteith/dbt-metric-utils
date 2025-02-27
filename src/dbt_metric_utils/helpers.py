import regex

# Constant for the regex pattern to extract materialize calls.
MATERIALIZE_CALL_PATTERN = regex.compile(r"""( # Capture group for the whole match.
      dbt_metric_utils_materialize\s*\(        # Match the function name and opening parenthesis.
        (?:
            [^()'"\\]+                         # Any characters except parentheses, quotes, or escapes.
          | "(?:\\.|[^"\\])*"                  # A double-quoted string with escapes.
          | '(?:\\.|[^'\\])*'                  # A single-quoted string with escapes.
          | (?R)                               # Recursive call for nested parentheses.
        )*
      \)                                       # Closing parenthesis for the function call.
    )
""", regex.VERBOSE)


def _extract_materialize_calls(raw_code):
    matches = MATERIALIZE_CALL_PATTERN.findall(raw_code)
    return matches
