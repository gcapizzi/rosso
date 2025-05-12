use std::{io::BufRead, io::Write};

#[derive(Debug)]
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
        let mut array_len_str = String::new();
        reader.read_line(&mut array_len_str)?;
        array_len_str.truncate(array_len_str.len() - 2);
        let array_len: usize = array_len_str.parse().unwrap();

        let mut values = Vec::with_capacity(array_len);
        for _ in 0..array_len {
            values.push(parse(reader)?);
        }
        Ok(Value::Array(values))
    } else if &prefix == b"$" {
        let mut string_len_str = String::new();
        reader.read_line(&mut string_len_str)?;
        string_len_str.truncate(string_len_str.len() - 2);
        let string_len: usize = string_len_str.parse().unwrap();
        let mut string = vec![0; string_len];
        reader.read_exact(&mut string)?;
        reader.read_exact(&mut [0; 2])?;
        Ok(Value::BulkString(String::from_utf8(string).map_err(
            |_| std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid bulk string"),
        )?))
    } else {
        Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "invalid prefix",
        ))
    }
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
