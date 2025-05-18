use std::{io::BufRead, io::Write};

#[derive(Debug, PartialEq)]
pub enum Value {
    SimpleString(String),
    BulkString(String),
    Array(Vec<Value>),
    Error(String),
    Null,
}

pub fn parse<R: BufRead>(reader: &mut R) -> std::io::Result<Value> {
    let mut prefix = [0];
    reader.read_exact(&mut prefix)?;
    if &prefix == b"*" {
        let len = parse_length(reader)?;
        let mut values = Vec::with_capacity(len);
        for _ in 0..len {
            values.push(parse(reader)?);
        }
        Ok(Value::Array(values))
    } else if &prefix == b"$" {
        let len = parse_length(reader)?;
        let string = parse_string(reader, len)?;
        Ok(Value::BulkString(string))
    } else {
        Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "invalid prefix",
        ))
    }
}

fn parse_length<R: BufRead>(reader: &mut R) -> std::io::Result<usize> {
    let mut len_str = String::new();
    reader.read_line(&mut len_str)?;
    len_str.truncate(len_str.len() - 2);
    len_str
        .parse()
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid length"))
}

fn parse_string<R: BufRead>(reader: &mut R, length: usize) -> std::io::Result<String> {
    let mut string = vec![0; length];
    reader.read_exact(&mut string)?;
    reader.read_exact(&mut [0; 2])?;
    String::from_utf8(string)
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid bulk string"))
}

pub fn serialise<W: Write>(writer: &mut W, value: &Value) -> std::io::Result<()> {
    match value {
        Value::SimpleString(s) => {
            writer.write_all(b"+")?;
            writer.write_all(s.as_bytes())?;
            writer.write_all(b"\r\n")?;
        }
        Value::Error(e) => {
            writer.write_all(b"-")?;
            writer.write_all(e.to_string().as_bytes())?;
            writer.write_all(b"\r\n")?;
        }
        Value::BulkString(s) => {
            writer.write_all(b"$")?;
            writer.write_all(s.len().to_string().as_bytes())?;
            writer.write_all(b"\r\n")?;
            writer.write_all(s.as_bytes())?;
            writer.write_all(b"\r\n")?;
        }
        Value::Array(a) => {
            writer.write_all(b"*")?;
            writer.write_all(a.len().to_string().as_bytes())?;
            writer.write_all(b"\r\n")?;
            for item in a {
                serialise(writer, item)?;
            }
        }
        Value::Null => {
            writer.write_all(b"_\r\n")?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_parse_bulk_string() {
        let mut reader = Cursor::new(b"$5\r\nHello\r\n");
        let value = parse(&mut reader).unwrap();
        assert_eq!(value, Value::BulkString("Hello".to_string()));
    }

    #[test]
    fn test_parse_array() {
        let mut reader = Cursor::new(b"*2\r\n$5\r\nHello\r\n$5\r\nWorld\r\n");
        let value = parse(&mut reader).unwrap();
        assert_eq!(
            value,
            Value::Array(vec![
                Value::BulkString("Hello".to_string()),
                Value::BulkString("World".to_string()),
            ])
        );
    }

    #[test]
    fn test_serialise_simple_string() {
        let mut writer = Vec::new();
        let value = Value::SimpleString("Hello".to_string());
        serialise(&mut writer, &value).unwrap();
        assert_eq!(writer, b"+Hello\r\n");
    }

    #[test]
    fn test_serialise_error() {
        let mut writer = Vec::new();
        let value = Value::Error("Hello".to_string());
        serialise(&mut writer, &value).unwrap();
        assert_eq!(writer, b"-Hello\r\n");
    }

    #[test]
    fn test_serialise_bulk_string() {
        let mut writer = Vec::new();
        let value = Value::BulkString("Hello".to_string());
        serialise(&mut writer, &value).unwrap();
        assert_eq!(writer, b"$5\r\nHello\r\n");
    }

    #[test]
    fn test_serialise_array() {
        let mut writer = Vec::new();
        let value = Value::Array(vec![
            Value::SimpleString("Hello".to_string()),
            Value::BulkString("World".to_string()),
        ]);
        serialise(&mut writer, &value).unwrap();
        assert_eq!(writer, b"*2\r\n+Hello\r\n$5\r\nWorld\r\n");
    }

    #[test]
    fn test_serialise_null() {
        let mut writer = Vec::new();
        let value = Value::Null;
        serialise(&mut writer, &value).unwrap();
        assert_eq!(writer, b"_\r\n");
    }
}
