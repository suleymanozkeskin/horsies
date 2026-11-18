//! The expected results are the results of Python's `loads_json`
//! (`python/horsies/core/codec/json_io.py`) on CPython 3.13.

use super::*;

/// Each text with the result of `loads_json`: the type of the value, or the
/// text of the refusal after `JSON parse failed: `.
const VECTORS: [(&str, Result<PythonType, &str>); 70] = [
    (
        "{",
        Err("Expecting property name enclosed in double quotes: line 1 column 2 (char 1)"),
    ),
    ("[1]", Ok(PythonType::List)),
    ("null", Ok(PythonType::NoneType)),
    ("\"x\"", Ok(PythonType::Str)),
    ("1", Ok(PythonType::Int)),
    ("1.5", Ok(PythonType::Float)),
    ("-0", Ok(PythonType::Int)),
    ("1e5", Ok(PythonType::Float)),
    ("true", Ok(PythonType::Bool)),
    ("false", Ok(PythonType::Bool)),
    ("[]", Ok(PythonType::List)),
    ("{}", Ok(PythonType::Dict)),
    ("  {\"a\" : [ ] }  ", Ok(PythonType::Dict)),
    ("{\"a\":1,\"a\":2}", Err("JSON object repeats a key")),
    ("{\"a\":1,\"\\u0061\":2}", Err("JSON object repeats a key")),
    (
        "[1,]",
        Err("Illegal trailing comma before end of array: line 1 column 3 (char 2)"),
    ),
    (
        "{\"a\":1,}",
        Err("Illegal trailing comma before end of object: line 1 column 7 (char 6)"),
    ),
    (
        "[1 ,  ]",
        Err("Illegal trailing comma before end of array: line 1 column 4 (char 3)"),
    ),
    ("", Err("the text is empty")),
    (" ", Err("Expecting value: line 1 column 2 (char 1)")),
    ("x", Err("Expecting value: line 1 column 1 (char 0)")),
    (
        "[1 2]",
        Err("Expecting ',' delimiter: line 1 column 4 (char 3)"),
    ),
    (
        "{\"a\" 1}",
        Err("Expecting ':' delimiter: line 1 column 6 (char 5)"),
    ),
    (
        "{1:2}",
        Err("Expecting property name enclosed in double quotes: line 1 column 2 (char 1)"),
    ),
    (
        "{\"a\"",
        Err("Expecting ':' delimiter: line 1 column 5 (char 4)"),
    ),
    ("{\"a\":", Err("Expecting value: line 1 column 6 (char 5)")),
    (
        "{\"a\":1",
        Err("Expecting ',' delimiter: line 1 column 7 (char 6)"),
    ),
    ("[", Err("Expecting value: line 1 column 2 (char 1)")),
    (
        "\"abc",
        Err("Unterminated string starting at: line 1 column 1 (char 0)"),
    ),
    (
        "\"a\\qb\"",
        Err("Invalid \\escape: line 1 column 3 (char 2)"),
    ),
    (
        "\"\\u12\"",
        Err("Invalid \\uXXXX escape: line 1 column 3 (char 2)"),
    ),
    (
        "\"\\u12g4\"",
        Err("Invalid \\uXXXX escape: line 1 column 3 (char 2)"),
    ),
    (
        "\"\\u1234",
        Err("Invalid \\uXXXX escape: line 1 column 3 (char 2)"),
    ),
    (
        "\"\\",
        Err("Unterminated string starting at: line 1 column 1 (char 0)"),
    ),
    (
        "[NaN]",
        Err("Non-standard JSON constant is not allowed: NaN"),
    ),
    (
        "[Infinity]",
        Err("Non-standard JSON constant is not allowed: Infinity"),
    ),
    (
        "[-Infinity]",
        Err("Non-standard JSON constant is not allowed: -Infinity"),
    ),
    ("-Infinit", Err("Expecting value: line 1 column 1 (char 0)")),
    ("nul", Err("Expecting value: line 1 column 1 (char 0)")),
    ("nulll", Err("Extra data: line 1 column 5 (char 4)")),
    (
        "[1e400]",
        Err("JSON number is outside the finite binary64 range"),
    ),
    (
        "[-1e400]",
        Err("JSON number is outside the finite binary64 range"),
    ),
    ("[1e-400]", Ok(PythonType::List)),
    ("\"\\ud800\"", Err("JSON string holds a lone surrogate")),
    ("\"\\udc00\"", Err("JSON string holds a lone surrogate")),
    ("\"\\ud83d\\ude00\"", Ok(PythonType::Str)),
    (
        "\"\\ud800\\u0041\"",
        Err("JSON string holds a lone surrogate"),
    ),
    (
        "\"\\ud83d\\uzzzz\"",
        Err("Invalid \\uXXXX escape: line 1 column 9 (char 8)"),
    ),
    (
        "\u{feff}{}",
        Err("Unexpected UTF-8 BOM (decode using utf-8-sig): line 1 column 1 (char 0)"),
    ),
    ("{} x", Err("Extra data: line 1 column 4 (char 3)")),
    ("-", Err("Expecting value: line 1 column 1 (char 0)")),
    ("[-]", Err("Expecting value: line 1 column 2 (char 1)")),
    ("01", Err("Extra data: line 1 column 2 (char 1)")),
    ("1e", Err("Extra data: line 1 column 2 (char 1)")),
    ("1.", Err("Extra data: line 1 column 2 (char 1)")),
    ("1e+", Err("Extra data: line 1 column 2 (char 1)")),
    ("1.e5", Err("Extra data: line 1 column 2 (char 1)")),
    (
        "[\"a\nb\"]",
        Err("Invalid control character at: line 1 column 4 (char 3)"),
    ),
    ("[\"\u{7f}\"]", Ok(PythonType::List)),
    (
        "{\"\u{e9}\":\u{e9}}",
        Err("Expecting value: line 1 column 6 (char 5)"),
    ),
    (
        "\n\n  {\n \"a\": x}",
        Err("Expecting value: line 4 column 7 (char 12)"),
    ),
    (
        "\r\n[1,\n2,\n]",
        Err("Illegal trailing comma before end of array: line 3 column 2 (char 7)"),
    ),
    (
        "{\"\u{1f600}\":1,\"\\ud83d\\ude00\":2}",
        Err("JSON object repeats a key"),
    ),
    (
        "{\"\\ud800\":1,\"\\ud800\":2}",
        Err("JSON object repeats a key"),
    ),
    (
        "{\"a\":{\"b\":1,\"b\":2},\"a\":1}",
        Err("JSON object repeats a key"),
    ),
    (
        "{\"a\":1,\"a\":2,\"b\":[x]}",
        Err("Expecting value: line 1 column 19 (char 18)"),
    ),
    ("{\"a\":1,\"a\":2} x", Err("JSON object repeats a key")),
    (
        "[NaN, 1e400]",
        Err("Non-standard JSON constant is not allowed: NaN"),
    ),
    (
        "[1e400, NaN]",
        Err("JSON number is outside the finite binary64 range"),
    ),
    (
        "[\"\\ud800\", x]",
        Err("Expecting value: line 1 column 12 (char 11)"),
    ),
];

#[test]
fn each_text_reads_or_is_refused_as_python_reads_it() {
    for (text, expected) in VECTORS {
        assert_eq!(
            check_python_loads(text).map_err(|refusal| refusal.to_string()),
            expected.map_err(str::to_owned),
            "{text:?}"
        );
    }
}

#[test]
fn an_int_of_more_than_4300_digits_is_refused_and_its_sign_does_not_count() {
    let digits = "1".repeat(MAX_INT_DIGITS);
    assert_eq!(check_python_loads(&digits), Ok(PythonType::Int));
    assert_eq!(
        check_python_loads(&format!("-{digits}")),
        Ok(PythonType::Int)
    );
    let too_long = "1".repeat(MAX_INT_DIGITS + 1);
    for text in [
        too_long.clone(),
        format!("-{too_long}"),
        format!("[{too_long}]"),
    ] {
        assert_eq!(
            check_python_loads(&text).map_err(|refusal| refusal.to_string()),
            Err(
                "Exceeds the limit (4300 digits) for integer string conversion: value has 4301 \
                 digits; use sys.set_int_max_str_digits() to increase the limit"
                    .to_owned()
            ),
        );
    }
    // A float literal has no digit limit.
    let long_float = format!("1.{}", "1".repeat(5000));
    assert_eq!(check_python_loads(&long_float), Ok(PythonType::Float));
}

#[test]
fn a_nesting_deeper_than_300_levels_is_refused_after_the_scan() {
    let nested = |depth: usize| format!("{}{}", "[".repeat(depth), "]".repeat(depth));
    assert_eq!(check_python_loads(&nested(300)), Ok(PythonType::List));
    assert_eq!(check_python_loads(&nested(301)), Err(LoadsRefusal::TooDeep));
    assert_eq!(
        LoadsRefusal::TooDeep.to_string(),
        "JSON text is nested deeper than 300 levels"
    );
    // The grammar comes first, as in Python.
    assert_eq!(
        check_python_loads(&format!("{}x", nested(301))).map_err(|refusal| refusal.to_string()),
        Err("Extra data: line 1 column 603 (char 602)".to_owned()),
    );
    // A lone surrogate comes before the depth.
    let deep_surrogate = format!("{}\"\\ud800\"{}", "[".repeat(400), "]".repeat(400));
    assert_eq!(
        check_python_loads(&deep_surrogate),
        Err(LoadsRefusal::LoneSurrogate)
    );
    let deep_objects = format!("{}1{}", "[{\"a\":".repeat(200), "}]".repeat(200));
    assert_eq!(
        check_python_loads(&deep_objects),
        Err(LoadsRefusal::TooDeep)
    );
    // The scan does not recurse, so a deep text does not exhaust the stack.
    assert_eq!(
        check_python_loads(&nested(20_000)),
        Err(LoadsRefusal::TooDeep)
    );
}
