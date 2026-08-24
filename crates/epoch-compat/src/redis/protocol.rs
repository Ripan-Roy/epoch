use std::fmt::Write as _;

use thiserror::Error;

use crate::{MAX_FRAME_BYTES, MAX_REQUEST_ITEMS};

const MAX_ARGUMENT_BYTES: usize = 4 * 1024 * 1024;
const MAX_INLINE_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, PartialEq)]
pub enum RespValue {
    Simple(String),
    Error(String),
    Integer(i64),
    Bulk(Vec<u8>),
    Null,
    Array(Vec<RespValue>),
    Map(Vec<(RespValue, RespValue)>),
    Boolean(bool),
    Double(f64),
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum RespDecodeError {
    #[error("incomplete frame")]
    Incomplete,
    #[error("protocol error: {0}")]
    Protocol(&'static str),
}

pub fn decode_request(input: &[u8]) -> Result<(Vec<Vec<u8>>, usize), RespDecodeError> {
    if input.len() > MAX_FRAME_BYTES {
        return Err(RespDecodeError::Protocol("frame exceeds limit"));
    }
    if input.is_empty() {
        return Err(RespDecodeError::Incomplete);
    }
    if input[0] != b'*' {
        return decode_inline(input);
    }
    let (count, mut cursor) = decode_decimal_line(input, 1)?;
    let count = usize::try_from(count)
        .ok()
        .filter(|count| *count > 0 && *count <= MAX_REQUEST_ITEMS)
        .ok_or(RespDecodeError::Protocol("invalid argument count"))?;
    let mut result = Vec::with_capacity(count);
    for _ in 0..count {
        if cursor >= input.len() {
            return Err(RespDecodeError::Incomplete);
        }
        let prefix = input[cursor];
        cursor += 1;
        match prefix {
            b'$' => {
                let (length, next) = decode_decimal_line(input, cursor)?;
                let length = usize::try_from(length)
                    .ok()
                    .filter(|length| *length <= MAX_ARGUMENT_BYTES)
                    .ok_or(RespDecodeError::Protocol("invalid bulk length"))?;
                cursor = next;
                let end = cursor
                    .checked_add(length)
                    .and_then(|value| value.checked_add(2))
                    .ok_or(RespDecodeError::Protocol("bulk length overflow"))?;
                if end > input.len() {
                    return Err(RespDecodeError::Incomplete);
                }
                if &input[end - 2..end] != b"\r\n" {
                    return Err(RespDecodeError::Protocol("bulk string is not terminated"));
                }
                result.push(input[cursor..cursor + length].to_vec());
                cursor = end;
            }
            b'+' => {
                let (line, next) = decode_line(input, cursor)?;
                if line.len() > MAX_ARGUMENT_BYTES {
                    return Err(RespDecodeError::Protocol("argument exceeds limit"));
                }
                result.push(line.to_vec());
                cursor = next;
            }
            _ => {
                return Err(RespDecodeError::Protocol(
                    "commands require string arguments",
                ));
            }
        }
    }
    Ok((result, cursor))
}

fn decode_inline(input: &[u8]) -> Result<(Vec<Vec<u8>>, usize), RespDecodeError> {
    let (line, consumed) = decode_line(input, 0)?;
    if line.len() > MAX_INLINE_BYTES {
        return Err(RespDecodeError::Protocol("inline command exceeds limit"));
    }
    let parts = line
        .split(u8::is_ascii_whitespace)
        .filter(|part| !part.is_empty())
        .map(<[u8]>::to_vec)
        .collect::<Vec<_>>();
    if parts.is_empty() || parts.len() > MAX_REQUEST_ITEMS {
        return Err(RespDecodeError::Protocol("invalid inline command"));
    }
    Ok((parts, consumed))
}

fn decode_decimal_line(input: &[u8], start: usize) -> Result<(i64, usize), RespDecodeError> {
    let (line, consumed) = decode_line(input, start)?;
    let text =
        std::str::from_utf8(line).map_err(|_| RespDecodeError::Protocol("length is not ASCII"))?;
    let value = text
        .parse::<i64>()
        .map_err(|_| RespDecodeError::Protocol("length is not a decimal"))?;
    Ok((value, consumed))
}

fn decode_line(input: &[u8], start: usize) -> Result<(&[u8], usize), RespDecodeError> {
    let Some(relative) = input
        .get(start..)
        .ok_or(RespDecodeError::Incomplete)?
        .windows(2)
        .position(|window| window == b"\r\n")
    else {
        return Err(RespDecodeError::Incomplete);
    };
    let end = start + relative;
    Ok((&input[start..end], end + 2))
}

pub fn encode_response(value: &RespValue, resp3: bool) -> Vec<u8> {
    let mut output = Vec::new();
    encode_into(value, resp3, &mut output);
    output
}

fn encode_into(value: &RespValue, resp3: bool, output: &mut Vec<u8>) {
    match value {
        RespValue::Simple(value) => encode_line(b'+', sanitized(value).as_bytes(), output),
        RespValue::Error(value) => encode_line(b'-', sanitized(value).as_bytes(), output),
        RespValue::Integer(value) => encode_line(b':', value.to_string().as_bytes(), output),
        RespValue::Bulk(value) => {
            let mut length = String::new();
            let _ = write!(length, "{}", value.len());
            encode_line(b'$', length.as_bytes(), output);
            output.extend_from_slice(value);
            output.extend_from_slice(b"\r\n");
        }
        RespValue::Null if resp3 => output.extend_from_slice(b"_\r\n"),
        RespValue::Null => output.extend_from_slice(b"$-1\r\n"),
        RespValue::Array(values) => {
            encode_aggregate(b'*', values.len(), output);
            for value in values {
                encode_into(value, resp3, output);
            }
        }
        RespValue::Map(entries) if resp3 => {
            encode_aggregate(b'%', entries.len(), output);
            for (key, value) in entries {
                encode_into(key, resp3, output);
                encode_into(value, resp3, output);
            }
        }
        RespValue::Map(entries) => {
            encode_aggregate(b'*', entries.len() * 2, output);
            for (key, value) in entries {
                encode_into(key, resp3, output);
                encode_into(value, resp3, output);
            }
        }
        RespValue::Boolean(value) if resp3 => {
            output.extend_from_slice(if *value { b"#t\r\n" } else { b"#f\r\n" });
        }
        RespValue::Boolean(value) => {
            encode_into(&RespValue::Integer(i64::from(*value)), false, output);
        }
        RespValue::Double(value) if resp3 => {
            encode_line(b',', value.to_string().as_bytes(), output);
        }
        RespValue::Double(value) => {
            encode_into(
                &RespValue::Bulk(value.to_string().into_bytes()),
                false,
                output,
            );
        }
    }
}

fn encode_line(prefix: u8, value: &[u8], output: &mut Vec<u8>) {
    output.push(prefix);
    output.extend_from_slice(value);
    output.extend_from_slice(b"\r\n");
}

fn encode_aggregate(prefix: u8, length: usize, output: &mut Vec<u8>) {
    let mut value = String::new();
    let _ = write!(value, "{length}");
    encode_line(prefix, value.as_bytes(), output);
}

fn sanitized(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if matches!(character, '\r' | '\n') {
                ' '
            } else {
                character
            }
        })
        .take(1_024)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_pipelined_binary_safe_request_without_consuming_next_frame() {
        let input = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$6\r\na\0b\r\nc\r\n*1\r\n$4\r\nPING\r\n";
        let (request, consumed) = decode_request(input).unwrap();
        assert_eq!(
            request,
            vec![b"SET".to_vec(), b"key".to_vec(), b"a\0b\r\nc".to_vec()]
        );
        assert_eq!(&input[consumed..], b"*1\r\n$4\r\nPING\r\n");
    }

    #[test]
    fn distinguishes_incomplete_from_malformed_and_bounds_lengths() {
        assert_eq!(
            decode_request(b"*2\r\n$3\r\nGET\r\n$4\r\nke"),
            Err(RespDecodeError::Incomplete)
        );
        assert!(matches!(
            decode_request(b"*0\r\n"),
            Err(RespDecodeError::Protocol(_))
        ));
        assert!(matches!(
            decode_request(b"*1\r\n$999999999\r\n"),
            Err(RespDecodeError::Protocol(_))
        ));
    }

    #[test]
    fn emits_resp3_maps_and_resp2_flat_arrays() {
        let value = RespValue::Map(vec![(
            RespValue::Bulk(b"server".to_vec()),
            RespValue::Bulk(b"epoch".to_vec()),
        )]);
        assert_eq!(
            encode_response(&value, true),
            b"%1\r\n$6\r\nserver\r\n$5\r\nepoch\r\n"
        );
        assert_eq!(
            encode_response(&value, false),
            b"*2\r\n$6\r\nserver\r\n$5\r\nepoch\r\n"
        );
    }
}
