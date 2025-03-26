import hashlib
import regex
import json

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


def extract_materialize_calls(raw_code):
    matches = MATERIALIZE_CALL_PATTERN.findall(raw_code)
    return matches


def compute_file_hash(filenames):
    m = hashlib.md5()
    for filename in sorted(filenames):
        with open(filename, 'rb') as f:
            m.update(f.read())
    return m.hexdigest()


def _compute_dict_str_for_hash(d):
    """
    Create a caching hash for a dict by sorting the keys and dumping to string
    """
    return json.dumps(d, sort_keys=True, ensure_ascii=False)


def compute_list_hash(dependencies):
    """
    Create a hash for caching a list of dicts by hashing the concatenation of the string representation of the dicts
    """
    return hashlib.md5("".join([_compute_dict_str_for_hash(x) for x in dependencies]).encode('utf8')).hexdigest()
