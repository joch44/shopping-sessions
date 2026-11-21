//! The CSV dialect of rules.md "CSV dialect".
//!
//! Only `\n` ends a line (`\r` is an ordinary byte of its field). Every comma
//! separates fields. A field may be wrapped in double quotes, which are syntax;
//! a quote anywhere else makes the line malformed. An empty field is null.
//! Spaces are kept. An empty line is malformed. The first line of every file
//! (and only the first) is the header.

/// Splits `line` (without its `\n`) into exactly `N` fields. Returns false when
/// the line is malformed. Each field is `None` for null, else its unquoted bytes.
pub fn parse_line<'a, const N: usize>(line: &'a [u8], out: &mut [Option<&'a [u8]>; N]) -> bool {
    if line.is_empty() {
        return false;
    }
    let mut count = 0;
    for field in line.split(|&byte| byte == b',') {
        if count == N {
            return false; // too many fields
        }
        let value = if field.contains(&b'"') {
            let n = field.len();
            if n < 2 || field[0] != b'"' || field[n - 1] != b'"' || field[1..n - 1].contains(&b'"') {
                return false;
            }
            &field[1..n - 1]
        } else {
            field
        };
        out[count] = if value.is_empty() { None } else { Some(value) };
        count += 1;
    }
    count == N
}

/// Every data line of one file's bytes: the first line is skipped; the text
/// after the final `\n` (if any) is the last line.
pub fn data_lines(bytes: &[u8]) -> impl Iterator<Item = &[u8]> {
    let body = match bytes.iter().position(|&byte| byte == b'\n') {
        Some(end) => &bytes[end + 1..],
        None => &bytes[bytes.len()..], // only a header, or nothing
    };
    let body = body.strip_suffix(b"\n").unwrap_or(body);
    // An empty body after stripping means no lines; `split` would yield one empty line.
    let has_lines = !body.is_empty() || bytes.ends_with(b"\n\n");
    body.split(|&byte| byte == b'\n').take(if has_lines { usize::MAX } else { 0 })
}

/// ASCII digits only, at least one.
pub fn ascii_digits(value: &[u8]) -> bool {
    !value.is_empty() && value.iter().all(u8::is_ascii_digit)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lines(bytes: &[u8]) -> Vec<&[u8]> {
        data_lines(bytes).collect()
    }

    #[test]
    fn splits_lines_like_the_rules() {
        assert_eq!(lines(b""), Vec::<&[u8]>::new());
        assert_eq!(lines(b"h"), Vec::<&[u8]>::new());
        assert_eq!(lines(b"h\n"), Vec::<&[u8]>::new());
        assert_eq!(lines(b"h\na"), vec![b"a" as &[u8]]);
        assert_eq!(lines(b"h\na\n"), vec![b"a" as &[u8]]);
        assert_eq!(lines(b"h\n\n"), vec![b"" as &[u8]]);
        assert_eq!(lines(b"h\na\n\nb\n"), vec![b"a" as &[u8], b"", b"b"]);
        assert_eq!(lines(b"h\na\r\n"), vec![b"a\r" as &[u8]]);
    }

    #[test]
    fn parses_fields() {
        let mut out = [None; 3];
        assert!(parse_line(b"a,\"b\",", &mut out));
        assert_eq!(out, [Some(b"a" as &[u8]), Some(b"b"), None]);
        assert!(parse_line(b"\"\",x ,\r", &mut out));
        assert_eq!(out, [None, Some(b"x " as &[u8]), Some(b"\r")]);
        assert!(!parse_line(b"a,b", &mut out));
        assert!(!parse_line(b"a,b,c,d", &mut out));
        assert!(!parse_line(b"a,b\"x\",c", &mut out));
        assert!(!parse_line(b"a,\",c", &mut out));
        assert!(!parse_line(b"a,\"\"\",c", &mut out));
        assert!(!parse_line(b"", &mut out));
    }
}
