use futures::io::{AsyncBufRead, AsyncWrite};
use futures_lite::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};

#[derive(Debug, PartialEq)]
pub enum Value {
    SimpleString(String),
    BulkString(String),
    Array(Vec<Value>),
    Error(String),
    Null,
    Integer(i64),
}

pub async fn parse<R: AsyncBufRead + Unpin>(reader: &mut R) -> std::io::Result<Value> {
    let mut prefix = [0];
    reader.read_exact(&mut prefix).await?;
    if &prefix == b"*" {
        let len = parse_length(reader).await?;
        let mut values = Vec::with_capacity(len);
        for _ in 0..len {
            values.push(Box::pin(parse(reader)).await?);
        }
        Ok(Value::Array(values))
    } else if &prefix == b"$" {
        let len = parse_length(reader).await?;
        let string = parse_string(reader, len).await?;
        Ok(Value::BulkString(string))
    } else {
        Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "invalid prefix",
        ))
    }
}

async fn parse_length<R: AsyncBufRead + Unpin>(reader: &mut R) -> std::io::Result<usize> {
    let mut len_str = String::new();
    reader.read_line(&mut len_str).await?;
    len_str.truncate(len_str.len() - 2);
    len_str
        .parse()
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid length"))
}

async fn parse_string<R: AsyncBufRead + Unpin>(
    reader: &mut R,
    length: usize,
) -> std::io::Result<String> {
    let mut string = vec![0; length];
    reader.read_exact(&mut string).await?;
    reader.read_exact(&mut [0; 2]).await?;
    String::from_utf8(string)
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid bulk string"))
}

pub async fn serialise<W: AsyncWrite + Unpin>(
    writer: &mut W,
    value: &Value,
) -> std::io::Result<()> {
    match value {
        Value::SimpleString(s) => {
            writer.write_all(b"+").await?;
            writer.write_all(s.as_bytes()).await?;
            writer.write_all(b"\r\n").await?;
        }
        Value::Error(e) => {
            writer.write_all(b"-").await?;
            writer.write_all(e.as_bytes()).await?;
            writer.write_all(b"\r\n").await?;
        }
        Value::BulkString(s) => {
            writer.write_all(b"$").await?;
            writer.write_all(s.len().to_string().as_bytes()).await?;
            writer.write_all(b"\r\n").await?;
            writer.write_all(s.as_bytes()).await?;
            writer.write_all(b"\r\n").await?;
        }
        Value::Array(a) => {
            writer.write_all(b"*").await?;
            writer.write_all(a.len().to_string().as_bytes()).await?;
            writer.write_all(b"\r\n").await?;
            for item in a {
                Box::pin(serialise(writer, item)).await?;
            }
        }
        Value::Null => {
            writer.write_all(b"_\r\n").await?;
        }
        Value::Integer(i) => {
            writer.write_all(b":").await?;
            writer.write_all(i.to_string().as_bytes()).await?;
            writer.write_all(b"\r\n").await?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_lite::io::Cursor;
    use macro_rules_attribute::apply;
    use smol_macros::test;

    #[apply(test!)]
    async fn test_parse_bulk_string() {
        let mut bytes = b"$5\r\nHello\r\n".to_vec();
        let mut reader = Cursor::new(&mut bytes);
        let value = parse(&mut reader).await.unwrap();
        assert_eq!(value, Value::BulkString("Hello".to_string()));
    }

    #[apply(test!)]
    async fn test_parse_array() {
        let mut bytes = b"*2\r\n$5\r\nHello\r\n$5\r\nWorld\r\n".to_vec();
        let mut reader = Cursor::new(&mut bytes);
        let value = parse(&mut reader).await.unwrap();
        assert_eq!(
            value,
            Value::Array(vec![
                Value::BulkString("Hello".to_string()),
                Value::BulkString("World".to_string()),
            ])
        );
    }

    #[apply(test!)]
    async fn test_serialise_simple_string() {
        let mut writer = Vec::new();
        let value = Value::SimpleString("Hello".to_string());
        serialise(&mut writer, &value).await.unwrap();
        assert_eq!(writer, b"+Hello\r\n");
    }

    #[apply(test!)]
    async fn test_serialise_error() {
        let mut writer = Vec::new();
        let value = Value::Error("Hello".to_string());
        serialise(&mut writer, &value).await.unwrap();
        assert_eq!(writer, b"-Hello\r\n");
    }

    #[apply(test!)]
    async fn test_serialise_bulk_string() {
        let mut writer = Vec::new();
        let value = Value::BulkString("Hello".to_string());
        serialise(&mut writer, &value).await.unwrap();
        assert_eq!(writer, b"$5\r\nHello\r\n");
    }

    #[apply(test!)]
    async fn test_serialise_array() {
        let mut writer = Vec::new();
        let value = Value::Array(vec![
            Value::SimpleString("Hello".to_string()),
            Value::BulkString("World".to_string()),
        ]);
        serialise(&mut writer, &value).await.unwrap();
        assert_eq!(writer, b"*2\r\n+Hello\r\n$5\r\nWorld\r\n");
    }

    #[apply(test!)]
    async fn test_serialise_null() {
        let mut writer = Vec::new();
        let value = Value::Null;
        serialise(&mut writer, &value).await.unwrap();
        assert_eq!(writer, b"_\r\n");
    }
}
